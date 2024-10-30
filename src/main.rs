// src/main.rs

mod hyperion_db;
mod handler;
mod index;
mod storage;
mod shard_manager;
mod utils;

use hyperion_db::HyperionDB;
use handler::handle_command;
use shard_manager::ShardManager;
use tokio::net::TcpListener;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use std::error::Error;
use std::path::Path;
use tokio::fs;

/// Punto de entrada principal de HyperionDB.
/// Inicializa la base de datos con sharding y optimizaciones, y
/// configura el servidor TCP para manejar conexiones entrantes.
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Define los campos que se indexarán para optimizar las consultas.
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

    // Configuración de sharding.
    let num_shards = 8; // Número de shards para distribuir los datos.
    let data_dir = "hyperiondb_data".to_string(); // Directorio donde se almacenarán los datos.

    // Crear el directorio de datos si no existe.
    if !Path::new(&data_dir).exists() {
        fs::create_dir_all(&data_dir).await?;
    }

    // Inicializar el gestor de shards.
    let shard_manager = ShardManager::new(num_shards, data_dir.clone());

    // Inicializar la base de datos con persistencia, campos indexados y sharding.
    let db = HyperionDB::new(
        data_dir.clone(),
        indexed_fields,
        shard_manager,
    ).await;

    // Iniciar el servidor y escuchar conexiones en el puerto 8080.
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    println!("HyperionDB Server running on 127.0.0.1:8080");

    // Bucle principal para aceptar y manejar conexiones entrantes.
    loop {
        let (socket, _) = listener.accept().await?;
        let db = db.clone();

        tokio::spawn(async move {
            let (reader, mut writer) = socket.into_split();
            let mut reader = BufReader::new(reader);
            let mut line = String::new();

            // Leer y procesar comandos línea por línea.
            while let Ok(bytes_read) = reader.read_line(&mut line).await {
                if bytes_read == 0 {
                    break; // Conexión cerrada por el cliente.
                }

                // Manejar el comando recibido.
                let response = match handle_command(&db, line.trim().to_string()).await {
                    Ok(resp) => resp,
                    Err(e) => format!("ERR {}\n", e),
                };

                // Enviar la respuesta al cliente.
                if let Err(e) = writer.write_all(response.as_bytes()).await {
                    eprintln!("Failed to write to socket: {}", e);
                    break;
                }

                line.clear(); // Limpiar la línea para el próximo comando.
            }
        });
    }
}
