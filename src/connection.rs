use crate::store::{self, db_get, db_set};
use crate::utils::get_bulk_string;
use crate::CRLF;
use crate::{parse::parse_command, rdb::DataType, utils::EMPTY_RDB_HEX, Command, CONFIG};
use bytes::BufMut;
use std::{result::Result::Ok, sync::Arc};
use tokio::sync::mpsc::Sender;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
    sync::Mutex,
};

pub async fn handle_connection(
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
