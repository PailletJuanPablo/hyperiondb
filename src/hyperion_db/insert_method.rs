
use super::hyperion_db_struct::HyperionDB;
use crate::index::update_indices_on_insert;
use crate::storage::save_shard_to_disk;
use serde_json::Value;
use std::error::Error;

impl HyperionDB {
    pub async fn insert(&self, key: String, value: Value) -> Result<(), Box<dyn Error>> {
        let shard_id = self.shard_manager.get_shard(&key);
        if let Some(shard) = self.shards.get(&shard_id) {
            shard.insert(key.clone(), value.clone());
            update_indices_on_insert(&self.indices, &key, &value, &self.indexed_fields).await;
            save_shard_to_disk(&self.shard_manager.data_dir, shard_id, shard.clone()).await?;
        }
        Ok(())
    }
}
