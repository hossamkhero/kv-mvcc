#![allow(unused)]
use tokio::{self, io::{self, AsyncReadExt, AsyncWriteExt}, net::{TcpSocket, TcpStream}, time::{Duration, sleep}};

use std::io::Write;
use std::env;

use kv_mvcc::protocol::{write_msg, OP};

use bytes::BytesMut;

use std::pin::Pin;

async fn read_server(stream: &mut TcpStream) -> io::Result<bool> {
    let mut buffer = BytesMut::with_capacity(1024);

    assert!(buffer.is_empty());
    assert!(buffer.capacity() >= 1024);

    stream.read_buf(&mut buffer).await?;

    let text = String::from_utf8_lossy(&buffer);
    print!("{text}");


    Ok(true)
}


async fn new_client() -> io::Result<TcpStream> {
    let addr = "0.0.0.0:9000".parse().unwrap();
    let socket = TcpSocket::new_v4()?;
    let mut stream = socket.connect(addr).await?;

    Ok(stream)
}

fn parse_input() -> Option<(OP, String, String)> {
    print!(">> ");
    std::io::stdout().flush().ok()?;

    let mut line = String::new();
    std::io::stdin().read_line(&mut line).ok()?;

    let line = line.trim();

    if line.is_empty() {
        return None;
    }

    if line == "exit" || line == "quit" {
        std::process::exit(0);
    }

    let mut parts = line.split_whitespace();

    let op = parts.next()?;
    let key = parts.next()?;

    match op {
        "get" => Some((OP::Get, key.to_string(), String::new())),
        "remove" => Some((OP::Remove, key.to_string(), String::new())),
        "add" => {
            let value = parts.next()?;
            Some((OP::Add, key.to_string(), value.to_string()))
        }
        "update" => {
            let value = parts.next()?;
            Some((OP::Update, key.to_string(), value.to_string()))
        }
        _ => {
            println!("unknown command");
            None
        }
    }
}


type TestRetType = Result<(), Box<dyn std::error::Error>>;


// Single Stream Read-Write
async fn test1() -> TestRetType {
    let mut stream = new_client().await?;

    let msg1 = write_msg(OP::Add, "your", "mom")?;
    stream.write_all(&msg1).await?;

    read_server(&mut stream).await?;

    sleep(Duration::from_millis(2000)).await; // wait 2 sec.

    let msg2 = write_msg(OP::Get, "your", "")?;
    stream.write_all(&msg2).await?;

    read_server(&mut stream).await?;

    Ok(())
}

// One stream write. One stream Read
async fn test2() -> TestRetType {
    let mut stream1 = new_client().await?;
    let mut stream2 = new_client().await?;

    let msg1 = write_msg(OP::Add, "your", "mom")?;
    stream1.write_all(&msg1).await?;
    read_server(&mut stream1).await?;

    sleep(Duration::from_millis(2000)).await; // wait 2 sec.

    let msg2 = write_msg(OP::Get, "your", "")?;
    stream2.write_all(&msg2).await?;

    read_server(&mut stream2).await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    let tests: [fn() -> Pin<Box<dyn Future<Output = TestRetType>>>; 2] = [
        || Box::pin(test1()),
        || Box::pin(test2()),
    ];

    if args.len() < 2 {
        println!("Nooooo");

        return Ok(());
    }

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-i" => {
                let mut stream = new_client().await?;

                loop {
                    let Some((op, key, value)) = parse_input() else {
                        continue;
                    };

                    let msg = write_msg(op, &key, &value)?;
                    stream.write_all(&msg).await?;

                    read_server(&mut stream).await?;
                }
            }
            "-t" => {
                if i + 1 >= args.len() {
                    eprintln!("expected a test number after -t");
                    return Ok(());
                }

                let test_num: u32 = args[i + 1]
                    .parse()
                    .expect("expected a number after -t");


                tests[(test_num - 1) as usize]().await?;

                i += 1;
            }
            other => {
                eprintln!("unknown arg: {}", other);
            }
        }

        i += 1;
    }

    Ok(())
}
