mod parse;
mod rdb;
mod store;
use crate::rdb::DataType;
use bytes::buf::Writer;
use bytes::BufMut;
use once_cell::sync::Lazy;
use rdb::RdbReader;
use std::io::Write;
use std::sync::Arc;
use std::{path::Path, result::Result::Ok};
use store::{db_get, db_set, Config};
use thiserror::Error;
use tokio::sync::RwLock;

use crate::parse::parse_command;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task,
};
pub enum Command {
    Ping,
    Echo(String),
    Get(String),
    Set(String, store::Value),
    GetConfig(String),
    Keys(String),
}

static CRLF: &str = "\r\n";
static CONFIG: Lazy<Arc<RwLock<Config>>> = Lazy::new(|| Arc::new(RwLock::new(Config::new())));

pub struct Client {
    stream: TcpStream,
    _read_buffer: [u8; 512],
    _write_index: usize,
    selected_db: usize,
}

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

impl Client {
    pub const fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            _read_buffer: [0u8; 512],
            _write_index: 0,
            selected_db: 0,
        }
    }
}

async fn handle_connection(config: &Arc<store::Config>, client: &mut Client) {
    let mut buf = [0u8; 1024];
    while let Ok(n) = client.stream.read(&mut buf).await {
        match parse_command(&String::from_utf8((buf[..n]).to_vec()).unwrap()) {
            Ok(cmd) => match cmd {
                Command::Ping => {
                    let _ = client.stream.write_all(b"+PONG\r\n").await;
                }
                Command::Echo(s) => {
                    let _ = client.stream.write_all(s.as_bytes()).await;
                }
                Command::Get(key) => {
                    let value = db_get(client.selected_db, &key).await;
                    match value {
                        Ok(Some(DataType::String(value))) => {
                            let mut response_buff = Vec::with_capacity(256).writer();
                            let _ = get_bulk_string(&mut response_buff, value.as_bytes());
                            let _ = client.stream.write_all(response_buff.get_ref()).await;
                        }
                        Ok(None) => {
                            let _ = client.stream.write_all(b"$-1\r\n").await;
                        }
                        Err(e) => {
                            let _ = client
                                .stream
                                .write_all(format!("-{}\r\n", e).as_bytes())
                                .await;
                        }
                        _ => {
                            let _ = client.stream.write_all(b"-Invalid data type\r\n").await;
                        }
                    }
                }
                Command::Set(key, value) => {
                    let _ = db_set(client.selected_db, key, value).await;
                    let _ = client.stream.write_all(b"+OK\r\n").await;
                }
                Command::GetConfig(key) => {
                    let value = config.get(&key);
                    match value {
                        Some(value) => {
                            let mut response = String::new();
                            response.push_str(format!("*2{}", CRLF).as_str());
                            response.push_str(format!("${}{}", key.len(), CRLF).as_str());
                            response.push_str(key.as_str());
                            response.push_str(CRLF);
                            response.push_str(format!("${}{}", value.len(), CRLF).as_str());
                            response.push_str(value.as_str());
                            response.push_str(CRLF);
                            let _ = client.stream.write_all(response.as_bytes()).await;
                        }
                        None => {
                            let _ = client.stream.write_all(b"$-1\r\n").await;
                        }
                    }
                }
                Command::Keys(pattern) => {
                    if pattern == "*" {
                        let path = Path::new(&config.get("dir").unwrap_or("".to_string()))
                            .join(config.get("dbfilename").unwrap_or("".to_string()));

                        let rdb_data = RdbReader::read(&path).await;
                        match rdb_data {
                            Ok(rdb_data) => {
                                let mut response = String::new();
                                for (key, _) in rdb_data.databases.get(&0).unwrap() {
                                    response.push_str(format!("*1{}", CRLF).as_str());
                                    response.push_str(format!("${}{}", key.len(), CRLF).as_str());
                                    response.push_str(key.as_str());
                                    response.push_str(CRLF);
                                    println!("{}", response);
                                }
                                let _ = client.stream.write_all(response.as_bytes()).await;
                            }
                            Err(e) => {
                                let _ = client
                                    .stream
                                    .write_all(format!("-{}\r\n", e).as_bytes())
                                    .await;
                            }
                        }
                    }
                }
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

#[tokio::main]
async fn main() {
    println!("Logs from your program will appear here!");
    let mut config = store::Config::new();
    config.set_from_args().await;
    let _ = load_db().await;
    let config = Arc::new(config);
    let listener = TcpListener::bind("127.0.0.1:6379").await;

    match listener {
        Ok(listener) => loop {
            if let Ok((stream, socket_addr)) = listener.accept().await {
                println!("Accepted new connection from {}", socket_addr);
                let config: Arc<store::Config> = Arc::clone(&config);
                task::spawn(async move {
                    let mut client = Client::new(stream);
                    handle_connection(&config, &mut client).await;
                });
            }
        },
        Err(e) => {
            println!("Error binding to listener: {}", e);
            return;
        }
    }
}
