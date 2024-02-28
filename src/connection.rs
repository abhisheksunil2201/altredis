use crate::store::{self, db_get, db_set};
use crate::utils::get_bulk_string;
use crate::CRLF;
use crate::{parse::parse_command, rdb::DataType, utils::EMPTY_RDB_HEX, Command, CONFIG};
use anyhow;
use bytes::BufMut;
use std::{result::Result::Ok, str};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{self};

pub async fn handle_connection(mut stream: TcpStream) -> anyhow::Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    let mut buf = [0u8; 1024];
    let selected_db = 0;
    loop {
        tokio::select! {
            n = stream.read(&mut buf[..]) => {
                let n = n.unwrap_or(0);
                if n == 0 {
                    if let Ok(addr) = stream.peer_addr() {
                        eprintln!("end of connection with {}", addr);
                    } else {
                        eprintln!("end of connection");
                    }
                    return Ok(());
                }
                let command = parse_command(str::from_utf8(&buf[..n])?);
                match command {
                        Ok(Command::Ping) => {
                            let _ = stream.write_all(b"+PONG\r\n").await;
                        }
                        Ok(Command::Echo(s)) => {
                            let _ = stream.write_all(s.as_bytes()).await;
                        }
                        Ok(Command::ReplConf) => {
                            let config = CONFIG.read().await;
                            config.replicas.lock().await.push(tx.clone());
                            stream.write_all(b"+OK\r\n").await?;
                            stream.flush().await?;
                        }
                        Ok(Command::Psync(args)) => {
                            let _replication_id = args.get(0);
                            let _offset = args.get(1);
                            let master_repl_id = CONFIG.read().await.master_replid.clone();
                            let master_repl_offset = CONFIG.read().await.master_repl_offset.clone();
                            let _ = stream
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
                                .write_all(format!("${}\r\n", bytes.len()).as_bytes())
                                .await;
                            let _ = stream.write_all(&bytes).await;
                            stream.flush().await.unwrap();
                        }
                        Ok(Command::Get(key)) => {
                            let value = db_get(selected_db, &key).await;
                            println!("Received GET command {:?} {:?}", key, value);
                            match value {
                                Ok(Some(DataType::String(value))) => {
                                    let mut response_buff = Vec::with_capacity(256).writer();
                                    let _ = get_bulk_string(&mut response_buff, value.as_bytes());
                                    let _ = stream.write_all(response_buff.get_ref()).await;
                                }
                                _ => {
                                    let _ = stream.write_all(b"$-1\r\n").await;
                                }
                            }
                            stream.flush().await?;
                        }
                        Ok(Command::Set(key, value_str, expiry)) => {
                            let value = store::Value {
                                value: DataType::String(value_str),
                                expiry: expiry,
                            };
                            let _ = db_set(selected_db, key, value).await;
                            let mode = CONFIG.read().await.mode;
                            if mode == store::ServerMode::Master {
                                let _ = stream.write_all(b"+OK\r\n").await;
                            }

                            {
                                let replica_data = str::from_utf8(&buf[..n])?.to_string();
                                eprintln!("Received: {}", &replica_data);
                                for sender in &mut *CONFIG.read().await.replicas.lock().await {
                                    let _ = sender.send(replica_data.clone())?;
                                }
                            }
                            stream.flush().await?;
                        }
                        Ok(Command::GetConfig(key)) => match key.to_lowercase().as_str() {
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
                                        let _ = stream.write_all(response.as_bytes()).await;
                                    }
                                    None => {
                                        let _ = stream.write_all(b"$-1\r\n").await;
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
                                        let _ = stream.write_all(response.as_bytes()).await;
                                    }
                                    None => {
                                        let _ = stream.write_all(b"$-1\r\n").await;
                                    }
                                }
                            }
                            _ => {
                                let _ = stream.write_all(b"$-1\r\n").await;
                            }
                        },
                        Ok(Command::Keys(pattern)) => {
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
                                        let _ = stream.write_all(response.as_bytes()).await;
                                    }
                                    Err(e) => {
                                        let _ = stream.write_all(format!("-{}\r\n", e).as_bytes()).await;
                                    }
                                }
                            }
                        }
                        Ok(Command::Info(arg)) => match arg.to_lowercase().as_str() {
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
                                let _ = stream.write_all(response.as_bytes()).await;
                            }
                            _ => {
                                let _ = stream.write_all(b"$-1\r\n").await;
                            }
                        },
                        _ => {
                            let _ = stream.write_all(b"$-1\r\n").await;
                        }
                    }
            },
            cmd = rx.recv() => {
                if let Some(cmd) = cmd {
                    let _ = stream.write_all(cmd.as_bytes()).await;
                    stream.flush().await?;
                }
            }
        }
    }
}

// pub async fn handle_replica_connection(mut stream: TcpStream) -> anyhow::Result<()> {
//     let mut buf = [0u8; 256];
//     let selected_db = 0;
//     loop {
//         tokio::select! {
//         n = stream.read(&mut buf[..]) => {
//             let n = n.unwrap_or(0);
//             println!("Received {} bytes", n);
//             if n == 0 {
//                 eprintln!("end of master connection");
//                 return Ok(());
//             }
//             eprintln!("read {n} bytes on master connection");
//             dbg!(String::from_utf8_lossy(&buf[..n]));
//             let command = parse_command(str::from_utf8(&buf[..n])?).unwrap_or(Command::Unknown);
//             println!("Received command {:?}", &command);
//             match command  {
//                     Command::Set(key, value, expiry) => {
//                         let value = store::Value {
//                             value: DataType::String(value),
//                             expiry: expiry,
//                         };
//                         let _ = db_set(selected_db, key, value).await;
//                     }
//                     Command::Get(key) => {
//                         println!("Received GET command {:?}", key);
//                         let value = db_get(selected_db, &key).await;
//                         match value {
//                             Ok(Some(DataType::String(value))) => {
//                                 let mut response_buff = Vec::with_capacity(256).writer();
//                                 let _ = get_bulk_string(&mut response_buff, value.as_bytes());
//                                 let _ = stream.write_all(response_buff.get_ref()).await;
//                             }
//                             _ => {
//                                 let _ = stream.write_all(b"$-1\r\n").await;
//                             }
//                         }
//                         stream.flush().await?;
//                     }
//                     _ => {eprintln!("unexpected command on master connection");}
//                 }
//             }
//         }
//     }
// }
