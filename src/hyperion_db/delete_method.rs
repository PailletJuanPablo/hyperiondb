
use super::hyperion_db_struct::HyperionDB;
use crate::index::update_indices_on_delete;
use crate::storage::save_shard_to_disk;
use std::collections::HashMap;
use std::error::Error as StdError;

impl HyperionDB {
    pub async fn delete(&self, key: String) -> Result<(), Box<dyn StdError + Send + Sync>> {
        let shard_id = self.shard_manager.get_shard(&key);
        if let Some(shard) = self.shards.get(&shard_id) {
            if let Some((_, value)) = shard.remove(&key) {
                update_indices_on_delete(&self.indices, &key, &value, &self.indexed_fields).await;
                save_shard_to_disk(&self.shard_manager.data_dir, shard_id, shard.clone()).await?;
                return Ok(());
            }
        }
        Err("Key not found".into())
    }

    pub async fn delete_many(&self, keys: Vec<String>) -> Result<(), Box<dyn StdError + Send + Sync>> {
        let mut shard_batches: HashMap<u32, Vec<String>> = HashMap::new();

        for key in keys {
            let shard_id = self.shard_manager.get_shard(&key);
            shard_batches.entry(shard_id).or_insert_with(Vec::new).push(key);
        }

        for (shard_id, batch_keys) in shard_batches {
            if let Some(shard) = self.shards.get(&shard_id) {
                for key in &batch_keys {
                    if let Some((_, value)) = shard.remove(key) {
                        update_indices_on_delete(&self.indices, key, &value, &self.indexed_fields).await;
                    }
                }
                save_shard_to_disk(&self.shard_manager.data_dir, shard_id, shard.clone()).await?;
            }
        }
        Ok(())
    }
}
