mod config;
mod db;
mod parse;

use std::sync::Arc;

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
    Set(String, db::Value),
    GetConfig(String),
}

static CRLF: &str = "\r\n";

async fn handle_connection(mut stream: TcpStream, config: &Arc<config::Config>) {
    let mut buf = [0u8; 1024];
    let mut db = db::Database::new();
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
                        db::Value {
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
                            //*2\r\n$3\r\ndir\r\n$16\r\n/tmp/redis-files\r\n
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
    let mut config = config::Config::new();
    config.set_from_args();
    let config = Arc::new(config);
    let listener = TcpListener::bind("127.0.0.1:6379").await;

    match listener {
        Ok(listener) => loop {
            if let Ok((stream, socket_addr)) = listener.accept().await {
                println!("Accepted new connection from {}", socket_addr);
                let config: Arc<config::Config> = Arc::clone(&config);
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
