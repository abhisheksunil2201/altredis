mod parse;
mod rdb;
mod store;

use std::sync::Arc;
use std::{path::Path, result::Result::Ok};

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

async fn handle_connection(mut stream: TcpStream, config: &Arc<store::Config>) {
    let mut buf = [0u8; 1024];
    let mut db = store::Database::new();
    let rdb = rdb::RdbReader;
    while let Ok(n) = stream.read(&mut buf).await {
        match parse_command(&String::from_utf8((buf[..n]).to_vec()).unwrap()) {
            Ok(cmd) => match cmd {
                Command::Ping => {
                    let _ = stream.write_all(b"+PONG\r\n").await;
                }
                Command::Echo(s) => {
                    let _ = stream.write_all(s.as_bytes()).await;
                }
                Command::Get(key) => {
                    let value = db.get(&key);
                    match value {
                        Some(value) => {
                            let mut response = String::new();
                            response.push_str(format!("${}{}", value.len(), CRLF).as_str());
                            response.push_str(value.as_str());
                            response.push_str(CRLF);
                            let _ = stream.write_all(response.as_bytes()).await;
                        }
                        None => {
                            let _ = stream.write_all(b"$-1\r\n").await;
                        }
                    }
                }
                Command::Set(key, value) => {
                    db.set(
                        key,
                        store::Value {
                            value: value.value,
                            expiry: value.expiry,
                        },
                    );
                    let _ = stream.write_all(b"+OK\r\n").await;
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
                            let _ = stream.write_all(response.as_bytes()).await;
                        }
                        None => {
                            let _ = stream.write_all(b"$-1\r\n").await;
                        }
                    }
                }
                Command::Keys(pattern) => {
                    if pattern == "*" {
                        let path = Path::new(&config.get("dir").unwrap_or("".to_string()))
                            .join(config.get("dbfilename").unwrap_or("".to_string()));

                        let rdb_data = rdb.read(&path).await;
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
                                let _ = stream.write_all(response.as_bytes()).await;
                            }
                            Err(e) => {
                                let _ = stream.write_all(format!("-{}\r\n", e).as_bytes()).await;
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

#[tokio::main]
async fn main() {
    println!("Logs from altredis will appear here!");
    let mut config = store::Config::new();
    config.set_from_args();
    let config = Arc::new(config);
    let listener = TcpListener::bind("127.0.0.1:6379").await;

    match listener {
        Ok(listener) => loop {
            if let Ok((stream, socket_addr)) = listener.accept().await {
                println!("Accepted new connection from {}", socket_addr);
                let config: Arc<store::Config> = Arc::clone(&config);
                task::spawn(async move {
                    handle_connection(stream, &config).await;
                });
            }
        },
        Err(e) => {
            println!("Error binding to listener: {}", e);
            return;
        }
    }
}
