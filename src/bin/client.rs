use async_std::io::{self, prelude::BufReadExt};
use bytes::Bytes;
use libp2p::futures::StreamExt;
use mini_redis::client;
use tokio::sync::{
    mpsc::{self, Sender},
    oneshot,
};

#[derive(Debug)]
enum Command {
    Get {
        key: String,
        resp: Responder<Option<Bytes>>,
    },
    Set {
        key: String,
        val: Bytes,
        resp: Responder<()>,
    },
}

type Responder<T> = oneshot::Sender<mini_redis::Result<T>>;

#[tokio::main]
async fn main() {
    let (tx, mut rx) = mpsc::channel(32);
    // let tx2 = tx.clone();

    tokio::spawn(async move {
        let mut client = client::connect("localhost:6379").await.unwrap();

        while let Some(cmd) = rx.recv().await {
            match cmd {
                Command::Get { key, resp } => {
                    let res = client.get(&key).await;
                    let _ = resp.send(res);
                }
                Command::Set { key, val, resp } => {
                    let res = client.set(&key, val).await;
                    let _ = resp.send(res);
                }
            }
        }
    });

    let mut stdin = io::BufReader::new(io::stdin()).lines().fuse();
    loop {
        tokio::select! {
            line = stdin.select_next_some() => {
                let reading_line = line.expect("error reading input");
                // println!("I got line {:? }", reading_line);
                let temp_tx = tx.clone();
                handle_input(reading_line, temp_tx);
            }
        }
    }
}

fn handle_input(line: String, tx: Sender<Command>) {
    let mut args = line.split(' ');
    match args.next() {
        Some("GET") => {
            let key = {
                match args.next() {
                    Some(key) => key,
                    None => {
                        println!("error reading key");
                        return;
                    }
                }
            };
            handle_get(key.to_string(), tx)
            // println!("in put command key {:?}", key);
        }
        Some("PUT") => {
            let key = {
                match args.next() {
                    Some(key) => key,
                    None => {
                        println!("unable to read key");
                        return;
                    }
                }
            };
            let value = {
                match args.next() {
                    Some(val) => val,
                    None => {
                        println!("unable to read val");
                        return;
                    }
                }
            };
            handle_put(key.to_string(), value.to_string(), tx);
        }
        _ => {
            println!("accpets only GET X or PUT X Y");
        }
    };
}

fn handle_get(key: String, tx: Sender<Command>) {
    tokio::spawn(async move {
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = Command::Get {
            key: key.to_string(),
            resp: resp_tx,
        };

        if tx.send(cmd).await.is_err() {
            println!("task shutdown");
            return;
        }

        let res = resp_rx.await;
        println!(
            "Got Get {:?}",
            res.unwrap().unwrap().unwrap_or("no value found".into())
        );
    });
}

fn handle_put(key: String, val: String, tx: Sender<Command>) {
    tokio::spawn(async move {
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = Command::Set {
            key: key,
            val: val.into(),
            resp: resp_tx,
        };
        if tx.send(cmd).await.is_err() {
            println!("error sending command");
            return;
        }

        let res = resp_rx.await;
        println!("Got Set {:?}", res);
    });
}
