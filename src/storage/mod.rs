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

pub async fn save_shard_to_disk(data_dir: &str, shard_id: u32, shard: Arc<DashMap<String, Value>>) -> Result<(), Box<dyn Error>> {
    let data_file = format!("{}/shard_{}.bin.lz4", data_dir, shard_id);
    let mut encoder = EncoderBuilder::new().level(4).build(Vec::new())?;
    let data: HashMap<String, Value> = shard.iter().map(|kv| (kv.key().clone(), kv.value().clone())).collect();
    let serialized = serde_json::to_string(&data)?;
    encoder.write_all(serialized.as_bytes())?;
    let (compressed_data, result) = encoder.finish();
    result?;
    tokio::fs::write(data_file.clone(), compressed_data).await?; 
    Ok(())
}

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
            data
        } else {
            HashMap::new()
        }
    } else {
        HashMap::new()
    }
}
