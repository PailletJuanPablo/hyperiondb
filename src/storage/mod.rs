use lz4::EncoderBuilder;
use serde_json::Value;
use std::{collections::HashMap, io::Write};
use std::error::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt}; // Import AsyncWriteExt for async file operations
use dashmap::DashMap;
use std::sync::Arc;
use std::error::Error as StdError;
use lz4::Decoder; 
use std::io::Read; 
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};


pub async fn load_from_wal(
    shard: &Arc<DashMap<String, Value>>,
    shard_id: u32,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let wal_file = format!("hyperiondb_data/shard_{}.wal", shard_id);

    // Abre el archivo WAL en modo lectura si existe
    if let Ok(file) = File::open(&wal_file).await {
        let reader = BufReader::new(file);
        let mut lines = reader.lines();

        while let Some(line) = lines.next_line().await? {
            // Intenta deserializar cada línea individualmente
            match serde_json::from_str::<(String, Value)>(&line) {
                Ok((key, value)) => {
                    shard.insert(key, value); // Inserta el dato en el shard
                }
                Err(e) => {
                    eprintln!("Error al deserializar línea en WAL para shard {}: {:?}", shard_id, e);
                    continue; // Ignora esta línea y sigue con la siguiente
                }
            }
        }
    }

    Ok(())
}


pub async fn append_to_wal(
    data_dir: &str,
    shard_id: u32,
    key: String,
    value: Value,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let wal_file_path = format!("{}/shard_{}.wal", data_dir, shard_id);
    let serialized_entry = serde_json::to_string(&(key, value))?;
    
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(wal_file_path)
        .await?;
    file.write_all(serialized_entry.as_bytes()).await?;
    file.write_all(b"\n").await?;  // Añadir nueva línea después de cada entrada
    
    Ok(())
}

pub async fn load_shard_from_disk(
    data_dir: &str,
    shard_id: u32,
) -> Result<HashMap<String, Value>, Box<dyn Error + Send + Sync>> {
    let data_file = format!("{}/shard_{}.bin.lz4", data_dir, shard_id);
    if tokio::fs::try_exists(&data_file).await.unwrap_or(false) {
        let mut file = tokio::fs::File::open(data_file).await?;
        let mut compressed_data = Vec::new();
        file.read_to_end(&mut compressed_data).await?;

        let mut decoder = Decoder::new(&compressed_data[..])?;
        let mut decompressed_data = Vec::new();
        decoder.read_to_end(&mut decompressed_data)?;

        let data: HashMap<String, Value> = serde_json::from_slice(&decompressed_data)?;
        Ok(data)
    } else {
        Ok(HashMap::new())
    }
}



pub async fn save_shard_to_disk(
    data_dir: &str,
    shard_id: u32,
    shard: Arc<DashMap<String, Value>>,
) -> Result<(), Box<dyn StdError + Send + Sync + 'static>> {

    let data_file = format!("{}/shard_{}.bin.lz4", data_dir, shard_id);

    // Convertimos posibles errores a Box<dyn StdError + Send + Sync + 'static>
    let mut encoder = EncoderBuilder::new()
        .level(4)
        .build(Vec::new())
        .map_err(|e| Box::new(e) as Box<dyn StdError + Send + Sync + 'static>)?;

    let data: HashMap<String, Value> = shard
        .iter()
        .map(|kv| (kv.key().clone(), kv.value().clone()))
        .collect();

    let serialized = serde_json::to_string(&data)
        .map_err(|e| Box::new(e) as Box<dyn StdError + Send + Sync + 'static>)?;

    encoder
        .write_all(serialized.as_bytes())
        .map_err(|e| Box::new(e) as Box<dyn StdError + Send + Sync + 'static>)?;

    let (compressed_data, result) = encoder.finish();

    result
        .map_err(|e| Box::new(e) as Box<dyn StdError + Send + Sync + 'static>)?;

    tokio::fs::write(data_file.clone(), compressed_data)
        .await
        .map_err(|e| Box::new(e) as Box<dyn StdError + Send + Sync + 'static>)?;

    Ok(())
}