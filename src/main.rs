mod connection;
mod parse;
mod rdb;
mod replica;
mod store;
mod utils;
use crate::{
    connection::handle_connection,
    parse::{parse_buf, parse_command},
    rdb::DataType,
    replica::Replica,
    store::db_set,
    utils::get_array,
};
use bytes::BufMut;
use once_cell::sync::Lazy;
use std::{env::args, path::Path, result::Result::Ok, str, sync::Arc, time::SystemTime};
use store::Config;
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::RwLock,
};
#[derive(Debug, Clone)]
pub enum Command {
    Ping,
    Echo(String),
    ReplConf,
    Psync(Vec<String>),
    Get(String),
    Set(String, String, Option<SystemTime>),
    GetConfig(String),
    Keys(String),
    Info(String),
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
    let selected_db = 0;
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
    let _rdbdata = stream.read(&mut readbuf[..]).await?;
    loop {
        tokio::select! {
            n = stream.read(&mut readbuf[..]) => {
                let n = n.unwrap_or(0);
                if n == 0 {
                    eprintln!("end of master connection");
                    return Ok(());
                }
                dbg!(String::from_utf8_lossy(&readbuf[..n]));

                let parts = if let Ok(parts) = parse_buf(&readbuf[..n]) {
                    parts
                } else {
                    continue;
                };
                for part in parts {
                    let command = "*".to_string() + &part;
                    match parse_command(&command)  {
                        Ok(Command::Set(key, value, expiry)) => {
                            let value = store::Value {
                                value: DataType::String(value),
                                expiry: expiry,
                            };
                            let _ = db_set(selected_db, key, value).await?;

                            stream.flush().await?;
                        }
                        _ => {eprintln!("unexpected command on master connection {:?}", command);}
                    }
                }
            }
        }
    }
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

    while let Ok((stream, socket_addr)) = listener.accept().await {
        println!("Accepted new connection from {}", socket_addr);
        tokio::spawn(async move {
            let _ = handle_connection(stream).await;
        });
    }
}
