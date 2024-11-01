use crate::{hyperion_db::hyperion_db_struct::HyperionDB, index::update_indices_on_insert, storage::save_shard_to_disk};
use serde_json::Value;
use std::{collections::HashMap, error::Error};

impl HyperionDB {
    pub async fn insert_or_update(&self, key: String, value: Value) -> Result<(), Box<dyn Error>> {
        let shard_id = self.shard_manager.get_shard(&key);

        if let Some(shard) = self.shards.get(&shard_id) {
            // Usa `insert` para agregar o actualizar la clave con el valor proporcionado
            shard.insert(key.clone(), value.clone());

            update_indices_on_insert(&self.indices, &key, &value, &self.indexed_fields).await;
            save_shard_to_disk(&self.shard_manager.data_dir, shard_id, shard.clone()).await?;
        }
        Ok(())
    }

    pub async fn insert_or_update_many(&self, items: Vec<(String, Value)>) -> Result<(), Box<dyn Error>> {
        // Agrupamos los elementos por shard_id para procesarlos en batch
        let mut shard_batches: HashMap<u32, Vec<(String, Value)>> = HashMap::new();

        for (key, value) in items {
            let shard_id = self.shard_manager.get_shard(&key);
            shard_batches.entry(shard_id).or_insert_with(Vec::new).push((key, value));
        }

        // Procesamos cada shard en batch
        for (shard_id, batch_items) in shard_batches {
            if let Some(shard) = self.shards.get(&shard_id) {
                for (key, value) in &batch_items {
                    shard.insert(key.clone(), value.clone());
                    update_indices_on_insert(&self.indices, key, value, &self.indexed_fields).await;
                }
                // Guardamos el shard completo en disco solo una vez
                save_shard_to_disk(&self.shard_manager.data_dir, shard_id, shard.clone()).await?;
            }
        }
        Ok(())
    }
    
}
