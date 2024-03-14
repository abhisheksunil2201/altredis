mod connection;
mod parse;
mod rdb;
mod replica;
mod store;
mod utils;
use crate::{
    connection::handle_connection, parse::parse_command, replica::Replica, utils::get_array,
};
use bytes::BufMut;
use once_cell::sync::Lazy;
use parse::process_buff;
use std::{env::args, path::Path, result::Result::Ok, str, sync::Arc, time::SystemTime};
use store::Config;
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Mutex, RwLock},
};
#[derive(Debug, Clone)]
pub enum Command {
    Ping,
    Echo(String),
    ReplConf(String),
    ReplConfAck,
    Psync(Vec<String>),
    Get(String),
    Set(String, String, Option<SystemTime>),
    GetConfig(String),
    Keys(String),
    Info(String),
}

impl ToString for Command {
    fn to_string(&self) -> String {
        match self {
            Command::Ping => "PING".to_owned(),
            Command::Echo(s) => format!("ECHO {}", s),
            Command::Get(s) => format!("GET {}", s),
            Command::Set(k, v, ex) => {
                if ex.is_some() {
                    format!("SET {} {} {:?}", k, v, ex.clone().unwrap())
                } else {
                    format!("SET {} {}", k, v)
                }
            }
            Command::Info(s) => {
                if !s.is_empty() {
                    format!("INFO {}", s.clone())
                } else {
                    "INFO".to_owned()
                }
            }
            Command::ReplConf(_args) => format!("{} {}", "REPLCONF", "listening-port"),
            Command::Psync(_args) => "PSYNC".to_owned(),
            Command::ReplConfAck => "REPLCONF getack".to_owned(),
            Command::GetConfig(s) => format!("CONFIG GET {}", s),
            Command::Keys(s) => format!("KEYS {}", s),
        }
    }
}

static CRLF: &str = "\r\n";
static CONFIG: Lazy<Arc<RwLock<Config>>> = Lazy::new(|| Arc::new(RwLock::new(Config::new())));

#[derive(Error, Debug)]
pub enum ResponseErrors {
    #[error("Received too many bytes before reaching end of message")]
    MessageTooBig,

    #[error("Unhandled data type: {0}")]
    UnhandledRespDataType(char),

    #[error("Array number of elements specifier is not a valid integer: '{0}'")]
    ArrayNumElementsInvalidLength(String),

    #[error("BulkString length specifier is not a valid integer: '{0}'")]
    BulkStringInvalidLength(String),
}

async fn load_db() -> Result<(), anyhow::Error> {
    let config = CONFIG.read().await;
    if config.dir.is_none() || config.dbfilename.is_none() {
        return Ok(());
    }

    let path = Path::new(config.dir.as_ref().unwrap());
    let path = path.join(config.dbfilename.as_ref().unwrap());
    store::db_load(path).await?;
    Ok(())
}

async fn handle_arguments() -> Result<(), anyhow::Error> {
    let args: Vec<String> = args().collect();
    let mut iter = args.iter();
    let mut config = CONFIG.write().await;
    while let Some(arg) = iter.next() {
        match arg.to_lowercase().as_str() {
            "--dir" => {
                config.dir = iter.next().map(|s| s.to_owned());
            }
            "--dbfilename" => {
                config.dbfilename = iter.next().map(|s| s.to_owned());
            }
            "--port" => {
                config.port = iter
                    .next()
                    .map(|s| s.parse::<u16>().unwrap_or_else(|_| 6379))
                    .unwrap_or(6379);
            }
            "--replicaof" => {
                let masterhost = iter.next().map(|s| s.to_owned());
                let masterport = iter.next().map(|s| s.parse::<u16>());
                if let (Some(masterhost), Some(Ok(masterport))) = (masterhost, masterport) {
                    config.masterhost = Some(masterhost);
                    config.masterport = Some(masterport);
                    config.mode = store::ServerMode::Replica;
                }
            }
            _ => {}
        }
    }

    Ok(())
}

pub async fn handshake(addr: String, port: u16) -> anyhow::Result<()> {
    let mut stream = TcpStream::connect(addr).await?;
    let mut buf = Vec::with_capacity(256).writer();
    let mut readbuf = [0u8; 1024];
    println!("Connected to Master");
    let _ = get_array(&mut buf, vec!["PING".as_bytes().to_vec()]);
    let _ = stream.write_all(buf.get_ref()).await?;
    buf.get_mut().clear();
    stream.flush().await?;
    let _n = stream.read(&mut readbuf[..]).await?;
    let _ = get_array(
        &mut buf,
        vec![
            "REPLCONF".as_bytes().to_vec(),
            "listening-port".as_bytes().to_vec(),
            port.to_string().as_bytes().to_vec(),
        ],
    );
    let _ = stream.write_all(buf.get_ref()).await?;
    buf.get_mut().clear();
    stream.flush().await?;
    let _n = stream.read(&mut readbuf[..]).await?;
    let _ = get_array(
        &mut buf,
        vec![
            "REPLCONF".as_bytes().to_vec(),
            "capa".as_bytes().to_vec(),
            "psync2".as_bytes().to_vec(),
        ],
    );
    let _ = stream.write_all(buf.get_ref()).await?;
    stream.flush().await?;
    buf.get_mut().clear();
    let _n = stream.read(&mut readbuf[..]).await?;
    let _ = get_array(
        &mut buf,
        vec![
            "PSYNC".as_bytes().to_vec(),
            "?".as_bytes().to_vec(),
            "-1".as_bytes().to_vec(),
        ],
    );
    let _ = stream.write_all(buf.get_ref()).await?; // Response will be +FULLRESYNC <REPL_ID> 0\r\n4
                                                    // let _n = stream.read(&mut readbuf[..]).await?; // <- RESP
    stream.flush().await?;
    buf.get_mut().clear();
    stream.read(&mut readbuf[..]).await?;
    tokio::spawn(async move {
        let _ = handle_client(stream, false).await;
    });
    Ok(())
}

async fn replica_connect_to_master() -> anyhow::Result<()> {
    if let (Some(masterhost), Some(masterport), port) = (
        CONFIG.read().await.masterhost.clone(),
        CONFIG.read().await.masterport.clone(),
        CONFIG.read().await.port.clone(),
    ) {
        let replica = Replica::new(0, masterhost, masterport);
        let addr = format!("{}:{}", replica.address, replica.port);
        let _ = handshake(addr, port).await;
    }
    Ok(())
}

async fn handle_client(stream: TcpStream, respond: bool) -> anyhow::Result<()> {
    let (read, write) = stream.into_split();
    let read_guarded = Arc::new(Mutex::new(read));
    let write_guarded = Arc::new(Mutex::new(write));
    loop {
        let mut buff = [0; 512];
        let read_clone = Arc::clone(&read_guarded);
        let bytes_read = {
            let mut stream_lock = read_clone.lock().await;
            stream_lock.read(&mut buff).await?
        };
        if bytes_read == 0 {
            println!("Connection closed");
            return Ok(());
        }
        let commands_vectors = process_buff(&buff[..bytes_read]).await?;
        for cmd_vec in commands_vectors {
            let command = parse_command(cmd_vec)?;
            println!("command is {:?}", command);
            let responses = {
                let write_clone = Arc::clone(&write_guarded);
                handle_connection(write_clone, &command).await
            };

            if !respond {
                continue;
            }

            for response in responses {
                let write_clone = Arc::clone(&write_guarded);
                let mut write_lock = write_clone.lock().await;
                write_lock.write_all(&response).await?;
                write_lock.flush().await?;
            }
        }
    }
}

#[tokio::main]
async fn main() {
    println!("Logs from your program will appear here!");
    let _ = handle_arguments().await;
    let _ = load_db().await;
    let config = CONFIG.read().await;
    let port = config.port;
    let addr = format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(addr).await.unwrap();
    if config.mode == store::ServerMode::Replica {
        tokio::spawn(async move {
            let _ = replica_connect_to_master().await;
        });
    }
    loop {
        let (stream, socket_addr) = listener.accept().await.unwrap();
        println!("Accepted new connection from {}", socket_addr);
        tokio::spawn(async move {
            let _ = handle_client(stream, true).await;
        });
    }
}
