use dashmap::DashMap;
use serde_json::{self, Value};
use std::collections::HashMap;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::error::Error;
use std::path::Path;
use tokio::sync::RwLock;
use std::sync::Arc;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

pub struct ShardManager {
    pub num_shards: u32,
    pub data_dir: String,
    pub shards: Arc<RwLock<HashMap<u32, DashMap<String, Value>>>>,
}

impl ShardManager {
    /// Crea una nueva instancia del `ShardManager` con un número específico de shards.
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

    /// Guarda un registro en el shard correspondiente.
    pub async fn save_to_shard(
        &self,
        shard_id: u32,
        key: String,
        value: Value,
    ) -> Result<(), Box<dyn Error>> {
        let mut shards_write = self.shards.write().await;
        let shard = shards_write.get_mut(&shard_id).ok_or("Shard not found")?;
        shard.insert(key.clone(), value.clone());

        let shard_path = format!("{}/shard_{}.json", self.data_dir, shard_id);
        self.save_shard_data(&shard, &shard_path).await?;
        Ok(())
    }

    /// Carga los datos de un shard desde el disco.
    pub async fn load_shard(&self, shard_id: u32) -> Result<(), Box<dyn Error>> {
        let shard_path = format!("{}/shard_{}.json", self.data_dir, shard_id);
        if !Path::new(&shard_path).exists() {
            return Ok(());
        }

        let mut shard_file = File::open(&shard_path).await?;
        let mut contents = Vec::new();
        shard_file.read_to_end(&mut contents).await?;

        let shard_data: HashMap<String, Value> =
            serde_json::from_slice(&contents).unwrap_or_default();
        let mut shards_write = self.shards.write().await;
        let shard = shards_write.get_mut(&shard_id).ok_or("Shard not found")?;

        for (key, value) in shard_data.into_iter() {
            shard.insert(key, value);
        }

        Ok(())
    }

    /// Obtiene un registro desde un shard en memoria.
    pub async fn load_from_shard(&self, shard_id: u32, key: &str) -> Option<Value> {
        let shards_read = self.shards.read().await;
        shards_read
            .get(&shard_id)
            .and_then(|shard| shard.get(key).map(|v| v.clone()))
    }
    pub fn get_shard(&self, key: &str) -> u32 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as u32) % self.num_shards
    }
    /// Elimina un registro de un shard.
    pub async fn delete_from_shard(&self, shard_id: u32, key: &str) -> Result<(), Box<dyn Error>> {
        let mut shards_write = self.shards.write().await;
        let shard = shards_write.get_mut(&shard_id).ok_or("Shard not found")?;
        shard.remove(key);

        let shard_path = format!("{}/shard_{}.json", self.data_dir, shard_id);
        self.save_shard_data(shard, &shard_path).await?;
        Ok(())
    }

    /// Guarda los datos de un shard en un archivo.
    async fn save_shard_data(
        &self,
        shard: &DashMap<String, Value>,
        shard_path: &str,
    ) -> Result<(), Box<dyn Error>> {
        let data: HashMap<String, Value> = shard
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();
        let serialized_data = serde_json::to_vec(&data)?;

        let mut shard_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(shard_path)
            .await?;
        shard_file.write_all(&serialized_data).await?;
        Ok(())
    }
}
