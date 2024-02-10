mod db;

use std::time::{Duration, SystemTime};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task,
};
enum Command {
    Ping,
    Echo(String),
    Get(String),
    Set(String, db::Value),
}

#[derive(Debug)]
enum Data {
    Array(Vec<Data>),
    BulkStringValue(String),
}

static CRLF: &str = "\r\n";

fn parse_array(data: &str) -> Result<(Data, &str), &str> {
    if let Some((num_elements, mut rest)) = data.split_once(CRLF) {
        let num_elements = num_elements[1..]
            .parse::<usize>()
            .map_err(|_| "Error parsing usize value.")?;
        let mut ret_data: Vec<Data> = Vec::with_capacity(num_elements);
        for _ in 0..num_elements {
            let (data, new_rest) = parse_val(rest)?;
            rest = new_rest;
            ret_data.push(data);
        }
        Ok((Data::Array(ret_data), rest))
    } else {
        Err("Can't parse array.")
    }
}
fn parse_string(data: &str) -> Result<(Data, &str), &str> {
    let data = data.split_once(CRLF);
    if let Some((len, rest)) = data {
        let len = len[1..]
            .parse::<usize>()
            .map_err(|_e| "Error parsing usize value.")?;
        let (val, rest) = rest.split_once(CRLF).ok_or("Invalid string format.")?;
        if len != val.len() {
            return Err("String legths don't match");
        }
        Ok((Data::BulkStringValue(val.to_string()), rest))
    } else {
        Err("Invalid string format.")
    }
}
fn parse_val(data: &str) -> Result<(Data, &str), &str> {
    match data.chars().next() {
        Some('*') => parse_array(data),
        Some('$') => parse_string(data),
        _ => Err("Unknown format"),
    }
}
fn parse_command(data: &str) -> Result<Command, &str> {
    let (data, _) = parse_val(data)?;
    println!("{:?}", data);
    if let Data::Array(cmd_vec) = data {
        if let Data::BulkStringValue(cmd) = cmd_vec.first().ok_or("Invalid command format.")? {
            match cmd.to_uppercase().as_str() {
                "PING" => Ok(Command::Ping),
                "ECHO" => {
                    let arg = cmd_vec.get(1);
                    let mut args = String::new();
                    if let Some(Data::BulkStringValue(arg)) = arg {
                        args.push_str(arg);
                    }
                    let len = args.len();
                    args.insert_str(0, format!("${}{}", len, CRLF).as_str());
                    args.push_str(CRLF);
                    Ok(Command::Echo(args.clone()))
                }
                "GET" => {
                    let key = cmd_vec.get(1);
                    let mut key_str = String::new();
                    if let Some(Data::BulkStringValue(key)) = key {
                        key_str.push_str(key);
                    }
                    Ok(Command::Get(key_str.clone()))
                }
                "SET" => {
                    let key = cmd_vec.get(1);
                    let value = cmd_vec.get(2);
                    let mut key_str = String::new();
                    let mut value_str = String::new();
                    if let Some(Data::BulkStringValue(key)) = key {
                        key_str.push_str(key);
                    }
                    if let Some(Data::BulkStringValue(value)) = value {
                        value_str.push_str(value);
                    }
                    if let Some(Data::BulkStringValue(arg)) = cmd_vec.get(3) {
                        match arg.as_str() {
                            "px" | "ex" => {
                                if let Some(Data::BulkStringValue(expiry)) = cmd_vec.get(4) {
                                    match expiry.parse::<u64>() {
                                        Ok(duration) => Ok(Command::Set(
                                            key_str.clone(),
                                            db::Value {
                                                value: value_str.clone(),
                                                expiry: Some(
                                                    SystemTime::now()
                                                        + Duration::from_millis(duration),
                                                ),
                                            },
                                        )),
                                        Err(_) => return Err("Invalid expiry duration"),
                                    }
                                } else {
                                    Err("Invalid expiry duration")
                                }
                            }
                            _ => Err("Invalid command."),
                        }
                    } else {
                        Ok(Command::Set(
                            key_str.clone(),
                            db::Value {
                                value: value_str.clone(),
                                expiry: None,
                            },
                        ))
                    }
                }
                _ => Err("Command not supported."),
            }
        } else {
            Err("Invalid command format.")
        }
    } else {
        Err("Invalid command.")
    }
}

async fn handle_connection(mut stream: TcpStream) {
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

    let listener = TcpListener::bind("127.0.0.1:6379").await;

    match listener {
        Ok(listener) => loop {
            if let Ok((stream, socket_addr)) = listener.accept().await {
                println!("Accepted new connection from {}", socket_addr);
                task::spawn(async move {
                    handle_connection(stream).await;
                });
            }
        },
        Err(e) => {
            println!("Error binding to listener: {}", e);
            return;
        }
    }
}
