use dashmap::DashMap;
use serde_json::{self, Value};
use std::collections::HashMap;
use std::error::Error;
use tokio::sync::RwLock;
use std::sync::Arc;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

pub struct ShardManager {
    pub data_dir: String,
    pub num_shards: u32,
    pub shards: Arc<RwLock<HashMap<u32, DashMap<String, Value>>>>,
}

impl std::ops::Deref for ShardManager {
    type Target = Arc<RwLock<HashMap<u32, DashMap<String, Value>>>>;

    fn deref(&self) -> &Self::Target {
        &self.shards
    }
}

impl ShardManager {
    pub async fn new(num_shards: u32, data_dir: String) -> Result<Self, Box<dyn Error>> {
        let shards = Arc::new(RwLock::new(HashMap::new()));

        for shard_id in 0..num_shards {
            let shard_data = DashMap::new();
            shards.write().await.insert(shard_id, shard_data);
        }

        Ok(ShardManager {
            num_shards,
            data_dir,
            shards,
        })
    }

    pub fn get_shard(&self, key: &str) -> u32 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as u32) % self.num_shards
    }
   
}
