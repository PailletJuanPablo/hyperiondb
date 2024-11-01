use super::hyperion_db_struct::HyperionDB;
use crate::index::update_indices_on_delete;
use crate::storage::save_shard_to_disk;
use std::error::Error;
impl HyperionDB {
  pub async fn delete_all(&self) -> Result<(), Box<dyn Error>> {
    for shard_entry in self.shards.iter() {
      let shard_id = *shard_entry.key();
      if let Some(shard) = self.shards.get(&shard_id) {
        // Itera sobre cada clave en el shard y elimina cada entrada
        for key in shard
          .iter()
          .map(|entry| entry.key().clone())
          .collect::<Vec<String>>()
        {
          if let Some((_, value)) = shard.remove(&key) {
            // Actualiza los índices después de eliminar la entrada
            update_indices_on_delete(&self.indices, &key, &value, &self.indexed_fields).await;
          }
        }
        // Guarda el shard vacío en el disco
        save_shard_to_disk(&self.shard_manager.data_dir, shard_id, shard.clone()).await?;
      }
    }
    Ok(())
  }
}
