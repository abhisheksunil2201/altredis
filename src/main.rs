mod connection;
mod parse;
mod rdb;
mod replica;
mod store;
mod utils;
use crate::{connection::handle_connection, replica::Replica, utils::get_array};
use anyhow::Context;
use bytes::BufMut;
use once_cell::sync::Lazy;
use std::{env::args, path::Path, result::Result::Ok, sync::Arc, time::SystemTime};
use store::Config;
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{tcp::OwnedWriteHalf, TcpListener, TcpStream},
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
    let mut buf = Vec::with_capacity(256).writer();
    println!("Connected to Master");
    let _ = get_array(&mut buf, vec!["PING".as_bytes().to_vec()]);
    let _ = stream.write_all(buf.get_ref()).await?;
    buf.get_mut().clear();
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
    let _ = get_array(
        &mut buf,
        vec![
            "REPLCONF".as_bytes().to_vec(),
            "capa".as_bytes().to_vec(),
            "psync2".as_bytes().to_vec(),
        ],
    );
    let _ = stream.write_all(buf.get_ref()).await?;
    buf.get_mut().clear();
    let _ = get_array(
        &mut buf,
        vec![
            "PSYNC".as_bytes().to_vec(),
            "?".as_bytes().to_vec(),
            "-1".as_bytes().to_vec(),
        ],
    );
    let _ = stream.write_all(buf.get_ref()).await?; // Response will be +FULLRESYNC <REPL_ID> 0\r\n4
    buf.get_mut().clear();
    let mut readbuf = [0u8; 256];
    let n = stream.read(&mut readbuf).await?;
    if n > 0 {
        let response = String::from_utf8_lossy(&readbuf[..n]);
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
