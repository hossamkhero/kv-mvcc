#![allow(unused)]
use tokio::{io::{AsyncWriteExt}, net::TcpSocket, self, time::{sleep, Duration}};

use kv_mvcc::protocol::{write_msg, OP};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connecting...
    let addr = "0.0.0.0:9000".parse().unwrap();

    let socket = TcpSocket::new_v4()?;
    let mut stream = socket.connect(addr).await?;

    // Our writing operation test...
    let msg1 = write_msg(OP::Add, "your", "mom")?;
    stream.write_all(&msg1).await?;

    sleep(Duration::from_millis(2000)).await; // wait 2 sec.

    let msg2 = write_msg(OP::Get, "your", "")?;
    stream.write_all(&msg2).await?;
    
    Ok(())
}
