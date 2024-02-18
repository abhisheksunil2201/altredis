use crate::{handle_connection, handle_connection_master, parse::parse_command, Command, CONFIG};
use anyhow::Result;
use bytes::Bytes;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{tcp::OwnedReadHalf, TcpListener, TcpStream},
    sync::mpsc::{self, UnboundedSender},
};
pub async fn master_listen(listener: TcpListener) -> ! {
    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let (tx, mut rx) = mpsc::unbounded_channel::<&[u8]>();
                let (reader, mut sender) = stream.into_split();
                tokio::spawn(async move {
                    while let Some(i) = rx.recv().await {
                        sender.write_all(&i).await.expect("problem sending")
                    }
                });
                tokio::spawn(async move {
                    master_handle_connection(tx, reader)
                        .await
                        .map_err(|e| println!("error: {}", e))
                });
            }
            Err(e) => println!("error: {}", e),
        }
    }
}
pub async fn replica_listen(listener: TcpListener) -> ! {
    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                tokio::spawn(async move {
                    replica_handle_connection(stream)
                        .await
                        .map_err(|e| println!("error: {}", e))
                });
            }
            Err(e) => println!("error: {}", e),
        }
    }
}
pub async fn master_handle_connection(
    tx: UnboundedSender<&[u8]>,
    mut stream: OwnedReadHalf,
) -> Result<()> {
    let mut buf = [0; 1024];
    loop {
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            continue;
        }
        match parse_command(&String::from_utf8((buf[..n]).to_vec()).unwrap()) {
            Ok(cmd) => match cmd {
                Command::Set(_, _, _) => {
                    // for replica in CONFIG.write().await.replicas.iter_mut() {
                    //     replica.send(dbg!(stream))?;
                    // }
                }
                _ => {}
            },
            Err(e) => {
                println!("Error: {}", e);
                println!("{}", e);
            }
        }
        // let resp = handle_connection_master(&mut stream, &mut tx).await;
        // for response in resp {
        //     tx.send(response.as_bytes().freeze())?;
        // }
        // tx.send(resp.as_bytes().freeze())?;
    }
}
pub async fn replica_handle_connection<'a>(mut stream: TcpStream) -> Result<()> {
    loop {
        handle_connection(&mut stream).await;
    }
}
