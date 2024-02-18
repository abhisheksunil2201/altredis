mod parse;
mod rdb;
mod replica;
mod store;
mod utils;
use crate::{parse::parse_command, rdb::DataType, replica::Replica, utils::EMPTY_RDB_HEX};
use anyhow::Context;
use bytes::{buf::Writer, BufMut};
use once_cell::sync::Lazy;
use std::{env::args, io::Write, path::Path, result::Result::Ok, sync::Arc, time::SystemTime};
use store::{db_get, db_set, Config};
use thiserror::Error;
use tokio::sync::mpsc::Sender;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpListener, TcpStream,
    },
    sync::{mpsc, Mutex, RwLock},
};
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

// pub struct Client {
//     stream: TcpStream,
//     _read_buffer: [u8; 512],
//     _write_index: usize,
//     selected_db: usize,
// }

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

// impl Client {
//     pub const fn new(stream: TcpStream) -> Self {
//         Self {
//             stream,
//             _read_buffer: [0u8; 512],
//             _write_index: 0,
//             selected_db: 0,
//         }
//     }
// }

async fn handle_connection(
    streams: (OwnedReadHalf, OwnedWriteHalf),
    replicas: Arc<Mutex<Vec<Arc<Mutex<OwnedWriteHalf>>>>>,
    tx: Sender<String>,
) {
    let mut streamread = streams.0;
    let stream = Arc::new(Mutex::new(streams.1));
    let mut buf = [0u8; 1024];
    let selected_db = 0;

    while let Ok(n) = streamread.read(&mut buf).await {
        if n == 0 {
            println!("Stream is closed");
            break;
        }
        // let request = String::from_utf8_lossy(&buf[..n]);
        match parse_command(&String::from_utf8((buf[..n]).to_vec()).unwrap()) {
            Ok(cmd) => match cmd {
                Command::Ping => {
                    let _ = stream.lock().await.write_all(b"+PONG\r\n").await;
                }
                Command::Echo(s) => {
                    let _ = stream.lock().await.write_all(s.as_bytes()).await;
                }
                Command::ReplConf => {
                    let _ = stream.lock().await.write_all(b"+OK\r\n").await;
                }
                Command::Psync(args) => {
                    let _replication_id = args.get(0);
                    let _offset = args.get(1);
                    let master_repl_id = CONFIG.read().await.master_replid.clone();
                    let master_repl_offset = CONFIG.read().await.master_repl_offset.clone();
                    let _ = stream
                        .lock()
                        .await
                        .write_all(
                            format!("+FULLRESYNC {} {}\r\n", master_repl_id, master_repl_offset)
                                .as_bytes(),
                        )
                        .await
                        .expect("Error writing to stream");
                    let bytes: Vec<u8> = (0..EMPTY_RDB_HEX.len())
                        .step_by(2)
                        .map(|i| u8::from_str_radix(&EMPTY_RDB_HEX[i..i + 2], 16).unwrap())
                        .collect();
                    let _ = stream
                        .lock()
                        .await
                        .write_all(format!("${}\r\n", bytes.len()).as_bytes())
                        .await;
                    let _ = stream.lock().await.write_all(&bytes).await;
                    stream.lock().await.flush().await.unwrap();
                    replicas.lock().await.push(stream.clone());
                }
                Command::Get(key) => {
                    let value = db_get(selected_db, &key).await;
                    match value {
                        Ok(Some(DataType::String(value))) => {
                            let mut response_buff = Vec::with_capacity(256).writer();
                            let _ = get_bulk_string(&mut response_buff, value.as_bytes());
                            let _ = stream.lock().await.write_all(response_buff.get_ref()).await;
                        }
                        Ok(None) => {
                            let _ = stream.lock().await.write_all(b"$-1\r\n").await;
                        }
                        Err(e) => {
                            let _ = stream
                                .lock()
                                .await
                                .write_all(format!("-{}\r\n", e).as_bytes())
                                .await;
                        }
                        _ => {
                            let _ = stream
                                .lock()
                                .await
                                .write_all(b"-Invalid data type\r\n")
                                .await;
                        }
                    }
                }
                Command::Set(key, value_str, expiry) => {
                    let valueclone = value_str.clone();
                    let keyclone = key.clone();
                    tx.send(
                        format!(
                            "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                            keyclone.len(),
                            keyclone,
                            valueclone.len(),
                            valueclone
                        )
                        .to_string(),
                    )
                    .await
                    .unwrap();
                    println!("Sent to replica");
                    let value = store::Value {
                        value: DataType::String(value_str),
                        expiry: expiry,
                    };
                    let _ = db_set(selected_db, key, value).await;
                    let _ = stream.lock().await.write_all(b"+OK\r\n").await;
                }
                Command::GetConfig(key) => match key.to_lowercase().as_str() {
                    "dir" => {
                        let mut response = String::new();
                        let dir = CONFIG.read().await.dir.clone();
                        match dir {
                            Some(dir) => {
                                response.push_str(format!("*2{}", CRLF).as_str());
                                response.push_str(format!("${}{}", key.len(), CRLF).as_str());
                                response.push_str(key.as_str());
                                response.push_str(CRLF);
                                response.push_str(format!("${}{}", dir.len(), CRLF).as_str());
                                response.push_str(dir.as_str());
                                response.push_str(CRLF);
                                let _ = stream.lock().await.write_all(response.as_bytes()).await;
                            }
                            None => {
                                let _ = stream.lock().await.write_all(b"$-1\r\n").await;
                            }
                        }
                    }
                    "dbfilename" => {
                        let mut response = String::new();
                        let dbfilename = CONFIG.read().await.dbfilename.clone();
                        match dbfilename {
                            Some(dbfilename) => {
                                response.push_str(format!("*2{}", CRLF).as_str());
                                response.push_str(format!("${}{}", key.len(), CRLF).as_str());
                                response.push_str(key.as_str());
                                response.push_str(CRLF);
                                response
                                    .push_str(format!("${}{}", dbfilename.len(), CRLF).as_str());
                                response.push_str(dbfilename.as_str());
                                response.push_str(CRLF);
                                let _ = stream.lock().await.write_all(response.as_bytes()).await;
                            }
                            None => {
                                let _ = stream.lock().await.write_all(b"$-1\r\n").await;
                            }
                        }
                    }
                    _ => {
                        let _ = stream.lock().await.write_all(b"$-1\r\n").await;
                    }
                },
                Command::Keys(pattern) => {
                    if pattern == "*" {
                        let keys = store::db_list_keys(selected_db).await;
                        match keys {
                            Ok(keys) => {
                                let mut response = String::new();
                                response.push_str(format!("*{}{}", keys.len(), CRLF).as_str());
                                for key in keys {
                                    response.push_str(format!("${}{}", key.len(), CRLF).as_str());
                                    response.push_str(key.as_str());
                                    response.push_str(CRLF);
                                }
                                let _ = stream.lock().await.write_all(response.as_bytes()).await;
                            }
                            Err(e) => {
                                let _ = stream
                                    .lock()
                                    .await
                                    .write_all(format!("-{}\r\n", e).as_bytes())
                                    .await;
                            }
                        }
                    }
                }
                Command::Info(arg) => match arg.to_lowercase().as_str() {
                    "replication" => {
                        let masterhost = CONFIG.read().await.masterhost.clone();
                        let master_replid = CONFIG.read().await.master_replid.clone();
                        let master_repl_offset = CONFIG.read().await.master_repl_offset.clone();
                        let role = match masterhost {
                            Some(_) => format!("role:slave"),
                            None => format!("role:master"),
                        };
                        let master_repl_id_string = format!("master_replid:{}", master_replid);
                        let master_repl_offset_string =
                            format!("master_repl_offset:{}", master_repl_offset);
                        let response_str = format!(
                            "{}\n{}\n{}",
                            role, master_repl_id_string, master_repl_offset_string
                        );
                        let response =
                            format!("${}{}{}{}", response_str.len(), CRLF, response_str, CRLF);
                        let _ = stream.lock().await.write_all(response.as_bytes()).await;
                    }
                    _ => {
                        let _ = stream.lock().await.write_all(b"$-1\r\n").await;
                    }
                },
            },
            Err(e) => {
                println!("Error: {}", e);
                println!("{}", e);
                break;
            }
        }
    }
}

fn get_bulk_string(buffer: &mut Writer<Vec<u8>>, string: &[u8]) -> tokio::io::Result<()> {
    let length_str = string.len().to_string();
    buffer.write_all(
        format!("${}\r\n{}\r\n", length_str, String::from_utf8_lossy(string)).as_bytes(),
    )?;
    Ok(())
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
                }
            }
            _ => {}
        }
    }

    Ok(())
}

pub async fn handshake(addr: String, port: u16) -> anyhow::Result<()> {
    let mut stream = TcpStream::connect(addr).await?;
    println!("Connected to Master");
    let _ = stream.write_all("*1\r\n$4\r\nping\r\n".as_bytes()).await?;
    let _ = stream
        .write_all(
            format!(
                "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{}\r\n",
                port
            )
            .as_bytes(),
        )
        .await?;
    let _ = stream
        .write_all("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n".as_bytes())
        .await?;
    let _ = stream
        .write_all("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n".as_bytes())
        .await?; // Response will be +FULLRESYNC <REPL_ID> 0\r\n4
    let mut buf = [0u8; 1024];
    let n = stream.read(&mut buf).await?;
    if n > 0 {
        let response = String::from_utf8_lossy(&buf[..n]);
        println!("Received response from Master: {}", response);
        // Process the response if necessary
    } else {
        println!("No response from Master");
    }

    Ok(())
}
async fn replica_connect() -> anyhow::Result<()> {
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
    let _ = replica_connect().await;
    let replicas: Arc<Mutex<Vec<Arc<Mutex<OwnedWriteHalf>>>>> = Arc::new(Mutex::new(vec![]));
    let (tx, mut rx) = mpsc::channel::<String>(10);
    let replicas_clone = Arc::clone(&replicas);
    tokio::spawn(async move {
        while let Some(resp) = rx.recv().await {
            for replica in replicas.lock().await.iter() {
                println!("Sending to replica");
                replica
                    .lock()
                    .await
                    .write_all(resp.as_bytes())
                    .await
                    .context("failed to propagate")
                    .unwrap();
            }
        }
    });

    while let Ok((stream, socket_addr)) = listener.accept().await {
        println!("Accepted new connection from {}", socket_addr);
        let replicas_clone = Arc::clone(&replicas_clone);
        let tx = tx.clone();
        let stream = stream.into_split();
        tokio::spawn(async move {
            handle_connection(stream, replicas_clone, tx).await;
        });
    }
}
