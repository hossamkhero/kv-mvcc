#![allow(unused_must_use)]

use std::{collections::BTreeMap, sync::Arc};
use std::{io};
use tokio::net::TcpListener;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, sync::Mutex};

use kv_mvcc::protocol::{BitCursor, decode_msg, Action, OP};

enum StorageResult {
    Ok,
    UpdateKeyDoesNotExist,
    InsertedKeyAlreadyExists,
    ToBeRemovedKeyDoesNotExist,
    Value(Option<String>),
}

type Storage = Arc<Mutex<BTreeMap<String, String>>>;

fn handle_storage(action: &Action, storage: &mut BTreeMap<String, String>) -> StorageResult {
    let Action { op, key, value } = action;

    match op {
        OP::Get => {
            StorageResult::Value(storage.get(key).cloned())
        }
        OP::Add => {
            if storage.get(key).is_some() { return StorageResult::InsertedKeyAlreadyExists; }

            storage.insert(key.to_string(), value.to_string());

            StorageResult::Ok
        }
        OP::Update => {
            match storage.get_mut(key) {
                Some(v) => {
                    *v = value.to_string();
                    StorageResult::Ok
                }
                None => StorageResult::UpdateKeyDoesNotExist,
            }
        }
        OP::Remove => {
            match storage.remove(key) {
                Some(_) => StorageResult::Ok,
                None => StorageResult::ToBeRemovedKeyDoesNotExist
            }
        }
    }
}

async fn handle_client(socket: &mut tokio::net::TcpStream, thread_storage: Storage) -> io::Result<()> {
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

            let mut storage = thread_storage.lock().await;
            let res = handle_storage(&action, &mut storage);

            match res {
                StorageResult::Ok => {
                    let msg = format!("{:?} went successfully\n", action.op);
                    socket.write_all(msg.as_bytes()).await?;
                }
                StorageResult::UpdateKeyDoesNotExist => {
                    let msg = format!("Update failed: key '{}' does not exist\n", action.key);
                    socket.write_all(msg.as_bytes()).await?;
                }
                StorageResult::InsertedKeyAlreadyExists => {
                    let msg = format!("Add failed: key '{}' already exists\n", action.key);
                    socket.write_all(msg.as_bytes()).await?;
                }
                StorageResult::ToBeRemovedKeyDoesNotExist => {
                    let msg = format!("Remove failed: key '{}' does not exist\n", action.key);
                    socket.write_all(msg.as_bytes()).await?;
                }
                StorageResult::Value(Some(value)) => {
                    let msg = format!("{{{}: {}}}\n", action.key, value);
                    socket.write_all(msg.as_bytes()).await?;
                }
                StorageResult::Value(None) => {
                    let msg = format!("Value for '{}' does not exist\n", action.key);
                    socket.write_all(msg.as_bytes()).await?;
                }
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let storage: Storage = Arc::new(Mutex::new(BTreeMap::new()));

    let addr = "0.0.0.0:9000";

    let listener = TcpListener::bind(addr).await?;

    println!("Application started");

    loop {
        match listener.accept().await {
            Ok((mut socket, _)) => {
                let thread_storage = storage.clone();
                
                println!("NEW CONNECTION! {}", socket.peer_addr().unwrap());

                tokio::spawn(async move {
                    if let Err(e) = handle_client(&mut socket, thread_storage).await { eprintln!("client error: {e}"); }
                });

            }
            Err(e) => println!("Error in acception: {}", e),
        }
    }
}
