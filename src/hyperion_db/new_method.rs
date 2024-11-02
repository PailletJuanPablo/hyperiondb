use super::hyperion_db_struct::HyperionDB;
use crate::config::Config;
use crate::storage::load_shard_from_disk;
use crate::{index::update_indices_on_insert, shard_manager::ShardManager};
use dashmap::DashMap;
use serde_json::Value;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use crate::storage::load_from_wal;

impl HyperionDB {
    
    
    pub async fn new(config: Config) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let shards = Arc::new(DashMap::new());
        let indices = Arc::new(DashMap::new());
        let shard_manager = Arc::new(ShardManager::new(config.num_shards, config.data_dir.clone()).await?);

        for shard_id in 0..shard_manager.num_shards {
            // Ahora `load_shard_from_disk` devuelve un HashMap<String, Value>
            let shard_data: HashMap<String, Value> = load_shard_from_disk(&config.data_dir, shard_id).await?;

            // Convertimos el HashMap a DashMap y lo insertamos en los shards
            let shard = Arc::new(DashMap::from_iter(shard_data.into_iter()));
            shards.insert(shard_id, shard.clone());

            load_from_wal(&shard, shard_id).await?; // Llamada corregida a `load_from_wal`

            for entry in shard.iter() {
                update_indices_on_insert(
                    &indices,
                    entry.key(),
                    entry.value(),
                    &config.indexed_fields,
                ).await;
            }
        }

        Ok(HyperionDB {
            shards,
            indices,
            shard_manager,
            indexed_fields: config.indexed_fields,
        })
    }
}