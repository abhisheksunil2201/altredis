use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
};

fn handle_stream(mut stream: TcpStream) {
    let mut buf = [0; 1024];
    while let Ok(_) = stream.read(&mut buf) {
        stream.write_all("+PONG\r\n".as_bytes()).unwrap();
    }
}

fn main() {
    println!("Logs from redis will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(_stream) => handle_stream(_stream),
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
