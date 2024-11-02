use dashmap::DashMap;
use serde_json::Value;
use std::collections::HashMap;
use std::error::Error;
use tokio::sync::RwLock;
use std::sync::Arc;
use crate::storage::load_shard_from_disk; // Import function from storage module

pub struct ShardManager {
    pub data_dir: String,
    pub num_shards: u32,
    pub shards: Arc<RwLock<HashMap<u32, Arc<DashMap<String, Value>>>>>, // Cambiamos aquí el tipo
}

impl std::ops::Deref for ShardManager {
    type Target = Arc<RwLock<HashMap<u32, Arc<DashMap<String, Value>>>>>;

    fn deref(&self) -> &Self::Target {
        &self.shards
    }
}

impl ShardManager {
    pub async fn new(num_shards: u32, data_dir: String) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let shards = Arc::new(RwLock::new(HashMap::new()));

        for shard_id in 0..num_shards {
            // Cargar shard_data desde disco como HashMap
            let shard_data = load_shard_from_disk(&data_dir, shard_id).await?;
            println!("Shard {}: Cargados {} registros desde disco.", shard_id, shard_data.len());

            // Convertimos shard_data a un DashMap y luego lo envolvemos en un Arc
            let shard = Arc::new(DashMap::from_iter(shard_data.into_iter()));
            shards.write().await.insert(shard_id, shard.clone());
        }

        Ok(ShardManager {
            data_dir,
            num_shards,
            shards,
        })
    }

    pub fn get_shard(&self, key: &str) -> u32 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        use std::hash::{Hash, Hasher};
        key.hash(&mut hasher);
        (hasher.finish() as u32) % self.num_shards
    }
}