// src/main.rs

mod db;
mod handler;
mod utils;

use db::HyperionDB;
use handler::handle_command;
use tokio::net::TcpListener;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Especifica los campos a indexar
    let indexed_fields = vec![
        "name".to_string(),
        "age".to_string(),
        "address.city".to_string(),
        "category".to_string(),
        "city".to_string(),
        "sku".to_string(),
        "description".to_string(),
        "address.zipcode".to_string(),
        "product_name".to_string(),
        "price".to_string(),
        "currency".to_string(),
        "specs.processor".to_string(),
        "specs.ram".to_string(),
        "specs.battery".to_string(),
        
        "in_stock".to_string(),
        "created_at".to_string(),
        "warehouses.warehouse1".to_string(),
        "warehouses.warehouse2".to_string(),
    ];
    // Initialize the database with persistence and indexed fields
    let db = HyperionDB::new("hyperiondb_data.json".to_string(), indexed_fields).await;

    // Inicializar la base de datos con persistencia y campos indexados
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
