use crate::store::{self, db_get, db_set};
use crate::utils::{build_resp_array, build_resp_string, get_bulk_string};
use crate::CRLF;
use crate::{utils::EMPTY_RDB_HEX, Command, CONFIG};
use bytes::BufMut;
use std::result::Result::Ok;
use std::sync::Arc;
use std::vec;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::Mutex;

pub async fn propagate_command(command: &Command) -> anyhow::Result<()> {
    let config = CONFIG.read().await;
    let replicas = config.replicas.lock().await;
    let msg = build_resp_array(command.to_string().as_str());
    println!("Propagating command to {} replicas", replicas.len());
    for stream in replicas.iter() {
        let stream_clone: Arc<_> = Arc::clone(stream);
        let mut stream_lock = stream_clone.lock().await;
        println!("Sending command to replica {:?}", msg);
        stream_lock.write_all(&msg).await?;
        stream_lock.flush().await?;
    }
    Ok(())
}

pub async fn handle_connection(
    stream: Arc<Mutex<OwnedWriteHalf>>,
    command: &Command,
) -> Vec<Vec<u8>> {
    let selected_db = 0;
    match command {
        Command::Ping => vec![build_resp_string("PONG")],
        Command::Echo(ref s) => vec![s.as_bytes().to_vec()],
        Command::ReplConf(args) => {
            if args == "listening-port" {
                let config = CONFIG.read().await;
                config.replicas.lock().await.push(stream.clone());
            }
            vec![build_resp_string("OK")]
        }
        Command::ReplConfAck => vec![build_resp_string("REPLCONF ACK 0")],
        Command::Psync(ref args) => {
            let _replication_id = args.get(0);
            let _offset = args.get(1);
            let master_repl_id = CONFIG.read().await.master_replid.clone();
            let master_repl_offset = CONFIG.read().await.master_repl_offset.clone();
            let bytes: Vec<u8> = (0..EMPTY_RDB_HEX.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&EMPTY_RDB_HEX[i..i + 2], 16).unwrap())
                .collect();
            let response = format!("FULLRESYNC {} {}", master_repl_id, master_repl_offset);
            let mut bytes_res = format!("${}{}", bytes.len(), CRLF).as_bytes().to_vec();
            bytes_res.extend_from_slice(&bytes);

            vec![build_resp_string(response.as_str()), bytes_res]
        }
        Command::Get(ref key) => {
            let value = db_get(selected_db, &key).await;
            println!("Received GET command {:?} {:?}", key, value);
            match value {
                Ok(Some(value)) => {
                    let mut response_buff = Vec::with_capacity(256).writer();
                    let _ = get_bulk_string(&mut response_buff, value.as_bytes());
                    vec![response_buff.into_inner()]
                }
                _ => {
                    vec![build_resp_string("")]
                }
            }
        }
        Command::Set(ref key, ref value_str, expiry) => {
            let value = store::Value {
                value: value_str.to_string(),
                expiry: *expiry,
            };
            let _ = db_set(selected_db, key.to_string(), value).await;
            println!("Set key to val in master");
            let mode = CONFIG.read().await.mode;
            if mode == store::ServerMode::Master {
                match propagate_command(command).await {
                    Ok(_) => {
                        println!("Command propagated successfully {:?}", command)
                    }
                    Err(e) => {
                        println!("Error propagating command: {:?}", e);
                    }
                }
            }

            vec![build_resp_string("OK")]
        }
        Command::GetConfig(ref key) => match key.to_lowercase().as_str() {
            "dir" => {
                let dir = CONFIG.read().await.dir.clone();
                match dir {
                    Some(dir) => {
                        vec![build_resp_array(
                            format!("{} {}", key.as_str(), dir.as_str()).as_str(),
                        )]
                    }
                    None => {
                        vec![build_resp_string("")]
                    }
                }
            }
            "dbfilename" => {
                let dbfilename = CONFIG.read().await.dbfilename.clone();
                match dbfilename {
                    Some(dbfilename) => {
                        vec![build_resp_array(
                            format!("{} {}", key.as_str(), dbfilename.as_str()).as_str(),
                        )]
                    }
                    None => {
                        vec![build_resp_string("")]
                    }
                }
            }
            _ => {
                vec![build_resp_string("")]
            }
        },
        Command::Keys(ref pattern) => {
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
                        vec![response.as_bytes().to_vec()]
                    }
                    Err(_e) => {
                        vec![build_resp_string("")]
                    }
                }
            } else {
                vec![build_resp_string("")]
            }
        }
        Command::Info(ref arg) => match arg.to_lowercase().as_str() {
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
                let response = format!("${}{}{}{}", response_str.len(), CRLF, response_str, CRLF);
                vec![response.as_bytes().to_vec()]
            }
            _ => {
                vec![build_resp_string("")]
            }
        },
    }
}
