
use super::hyperion_db_struct::HyperionDB;
use crate::index::update_indices_on_delete;
use crate::storage::save_shard_to_disk;
use std::error::Error;

impl HyperionDB {
    pub async fn delete(&self, key: String) -> Result<(), Box<dyn Error>> {
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
}
