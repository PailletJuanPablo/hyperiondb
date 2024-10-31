
use super::hyperion_db_struct::HyperionDB;
use crate::{index::update_indices_on_insert, shard_manager::ShardManager};
use crate::storage::load_shard_from_disk;
use dashmap::DashMap;
use std::error::Error;
use std::sync::Arc;
use crate::config::Config;

impl HyperionDB {
    pub async fn new(config: Config) -> Result<Self, Box<dyn Error>> {
        let shards = Arc::new(DashMap::new());
        let indices = Arc::new(DashMap::new());

        let shard_manager =
            Arc::new(ShardManager::new(config.num_shards, config.data_dir.clone()).await?);

        for shard_id in 0..shard_manager.num_shards {
            let shard_data = load_shard_from_disk(&config.data_dir, shard_id).await;
            let shard = Arc::new(DashMap::from_iter(shard_data.into_iter()));
            shards.insert(shard_id, shard.clone());

            for entry in shard.iter() {
                update_indices_on_insert(
                    &indices,
                    &entry.key().clone(),
                    &entry.value().clone(),
                    &config.indexed_fields,
                )
                .await;
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
