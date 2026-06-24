#![allow(unused_must_use)]

use std::{io};
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt};

use kv_mvcc::protocol::{BitCursor, decode_msg};

async fn handle_client(socket: &mut tokio::net::TcpStream) -> io::Result<()> {
    let mut buf = [0u8; 1024];

    loop {
        let n = socket.read(&mut buf).await?;

        let mut cur = BitCursor::new(&buf);


        // on_close
        if n == 0 { 
            println!("CONNECTION CLOSED SUCCESSFULLY! FOR CLIENT {}", socket.peer_addr().unwrap());
            break;
        }

        if n > 0 {
            let action = decode_msg(&mut cur);

            println!("Action: {action:?}");
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let addr = "0.0.0.0:9000";

    let listener = TcpListener::bind(addr).await?;

    println!("Application started");

    loop {
        match listener.accept().await {
            Ok((mut socket, _)) => {
                println!("NEW CONNECTION! {}", socket.peer_addr().unwrap());

                tokio::spawn(async move {
                    if let Err(e) = handle_client(&mut socket).await { eprintln!("client error: {e}"); }
                });

            }
            Err(e) => println!("Error in acception: {}", e),
        }
    }
}
