// src/storage/mod.rs

use serde_json::Value;
use std::collections::HashMap;
use tokio::io::AsyncReadExt;
use std::error::Error;
use lz4::EncoderBuilder;
use lz4::Decoder;
use std::io::prelude::*;
use dashmap::DashMap;
use std::sync::Arc;

/// Guarda los datos de un shard en disco con compresión usando serde_json.
pub async fn save_shard_to_disk(data_dir: &str, shard_id: u32, shard: Arc<DashMap<String, Value>>) -> Result<(), Box<dyn Error>> {
    let data_file = format!("{}/shard_{}.bin.lz4", data_dir, shard_id);
    let mut encoder = EncoderBuilder::new().level(4).build(Vec::new())?;
    let data: HashMap<String, Value> = shard.iter().map(|kv| (kv.key().clone(), kv.value().clone())).collect();
    let serialized = serde_json::to_string(&data)?;
    encoder.write_all(serialized.as_bytes())?;
    let (compressed_data, result) = encoder.finish();
    result?;
    tokio::fs::write(data_file.clone(), compressed_data).await?; // Clonamos data_file
    println!("Shard {} guardado en {}", shard_id, data_file); // Usamos el clone sin problemas
    Ok(())
}

/// Carga los datos de un shard desde disco, descomprimiéndolos usando serde_json.
pub async fn load_shard_from_disk(data_dir: &str, shard_id: u32) -> HashMap<String, Value> {
    let data_file = format!("{}/shard_{}.bin.lz4", data_dir, shard_id);
    let mut contents = Vec::new();
    if let Ok(mut file) = tokio::fs::File::open(&data_file).await {
        file.read_to_end(&mut contents).await.unwrap();
        let mut decoder = Decoder::new(&contents[..]).unwrap();
        let mut decompressed_data = Vec::new();
        decoder.read_to_end(&mut decompressed_data).unwrap();
        let json_str = String::from_utf8_lossy(&decompressed_data);
        if let Ok(data) = serde_json::from_str::<HashMap<String, Value>>(&json_str) {
            println!("Shard {} cargado con {} registros.", shard_id, data.len());
            data
        } else {
            println!("Error al deserializar shard {}. Retornando HashMap vacío.", shard_id);
            HashMap::new()
        }
    } else {
        println!("Archivo de shard {} no encontrado. Retornando HashMap vacío.", shard_id);
        HashMap::new()
    }
}
