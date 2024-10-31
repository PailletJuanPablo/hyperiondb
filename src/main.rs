mod hyperion_db;
mod handler;
mod index;
mod storage;
mod config;
mod shard_manager;

use hyperion_db::HyperionDB;
use config::Config;
use handler::handle_command;
use tokio::net::TcpListener;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use std::error::Error;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config_path = "config.json";
    let config = Config::load_from_file(config_path)?;

    let db = Arc::new(HyperionDB::new(config).await?);

    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    println!("HyperionDB Server running on 127.0.0.1:8080");

    loop {
        let (socket, _) = listener.accept().await?;
        let db = db.clone();

        tokio::spawn(async move {
            let (reader, mut writer) = socket.into_split();
            let mut reader = BufReader::new(reader);
            let mut line = String::new();

            while let Ok(bytes_read) = reader.read_line(&mut line).await {
                if bytes_read == 0 {
                    break;
                }

                let response = match handle_command(&db, line.trim().to_string()).await {
                    Ok(resp) => resp,
                    Err(e) => format!("ERR {}\n", e),
                };

                if let Err(e) = writer.write_all(response.as_bytes()).await {
                    eprintln!("Failed to write to socket: {}", e);
                    break;
                }

                line.clear();
            }
        });
    }
}
