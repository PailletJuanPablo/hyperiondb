use crate::index::{update_indices_on_delete, update_indices_on_insert, Index};
use crate::shard_manager::ShardManager;
use crate::storage::{load_shard_from_disk, save_shard_to_disk};
use dashmap::DashMap;
use serde_json::Value;
use std::collections::HashSet;
use std::error::Error;
use std::sync::Arc;
use crate::config::{Config, IndexedField};

#[derive(Clone)]
pub struct HyperionDB {
    pub shards: Arc<DashMap<u32, Arc<DashMap<String, Value>>>>,
    pub indices: Arc<DashMap<String, Index>>,
    pub shard_manager: Arc<ShardManager>,
    pub indexed_fields: Vec<IndexedField>,
}

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

    pub async fn insert(&self, key: String, value: Value) -> Result<(), Box<dyn Error>> {
        let shard_id = self.shard_manager.get_shard(&key);
        if let Some(shard) = self.shards.get(&shard_id) {
            shard.insert(key.clone(), value.clone());
            update_indices_on_insert(&self.indices, &key, &value, &self.indexed_fields).await;
            save_shard_to_disk(&self.shard_manager.data_dir, shard_id, shard.clone()).await?;
        }
        Ok(())
    }

    pub async fn get(&self, key: &str) -> Option<Value> {
        let shard_id = self.shard_manager.get_shard(key);
        self.shards.get(&shard_id)?.get(key).map(|v| v.clone())
    }

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

    /// Realiza una consulta en la base de datos utilizando los índices.
    pub async fn query(&self, field: &str, operator: &str, value: &str) -> Vec<Value> {
        let mut result_set = HashSet::new();

        // Verificar si el índice existe para el campo
        if let Some(index) = self.indices.get(field) {
            let keys = index.query_keys(operator, value);

            // Recorre cada clave obtenida en el índice para recuperar el valor
            for key in keys.iter() {
                if let Some(value) = self.get(key).await {
                    result_set.insert(value);
                }
            }
        } else {
            println!("No se encontró índice para el campo: {}", field);
        }

        result_set.into_iter().collect()
    }

    /// Obtiene todos los registros de todos los shards de la base de datos.
    pub async fn get_all_records(&self) -> Vec<Value> {
        let mut all_records = Vec::new();
        println!("Iniciando recopilación de todos los registros...");

        // Recorrer cada shard y recolectar todos los registros
        for shard_entry in self.shards.iter() {
            let shard = shard_entry.value();
            for record in shard.iter() {
                all_records.push(record.value().clone());
            }
        }

        println!("Total de registros recopilados: {}", all_records.len());
        all_records
    }
}
