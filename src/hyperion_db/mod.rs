// src/hyperion_db/mod.rs

use serde_json::Value;
use std::collections::HashSet;
use std::sync::Arc;
use dashmap::DashMap;
use std::error::Error;
use crate::index::{Index, update_indices_on_insert, update_indices_on_delete};
use crate::storage::{save_shard_to_disk, load_shard_from_disk};
use crate::shard_manager::ShardManager;

/// Representa la base de datos HyperionDB con soporte para sharding y optimizaciones.
#[derive(Clone)]
pub struct HyperionDB {
    pub shards: Arc<DashMap<u32, Arc<DashMap<String, Value>>>>, // Shards gestionados por ID
    pub indices: Arc<DashMap<String, Index>>,
    pub shard_manager: ShardManager,
    pub indexed_fields: Vec<String>,
}

impl HyperionDB {
    /// Crea una nueva instancia de HyperionDB.
    pub async fn new(data_dir: String, indexed_fields: Vec<String>, shard_manager: ShardManager) -> Self {
        let shards = Arc::new(DashMap::new());
        let indices = Arc::new(DashMap::new());

        // Cargar datos de cada shard y actualizar los índices
        for shard_id in 0..shard_manager.num_shards {
            let shard_data = load_shard_from_disk(&data_dir, shard_id).await;
            let shard = Arc::new(DashMap::from_iter(shard_data.into_iter()));
            shards.insert(shard_id, shard.clone());

            println!("Shard ID {} cargado con {} registros.", shard_id, shard.len());

            // Actualizar índices con los datos del shard
            for entry in shard.iter() {
                update_indices_on_insert(&indices, &entry.key().clone(), &entry.value().clone(), &indexed_fields).await;
            }
        }

        HyperionDB {
            shards,
            indices,
            shard_manager,
            indexed_fields,
        }
    }

    /// Inserta un registro en la base de datos.
    pub async fn insert(&self, key: String, value: Value) -> Result<(), Box<dyn Error>> {
        let shard_id = self.shard_manager.get_shard(&key);
        if let Some(shard) = self.shards.get(&shard_id) {
            shard.insert(key.clone(), value.clone());
            update_indices_on_insert(&self.indices, &key, &value, &self.indexed_fields).await;
            save_shard_to_disk(&self.shard_manager.data_dir, shard_id, shard.clone()).await?;
            println!("Registro insertado en Shard ID {}: {}", shard_id, key);
        }
        Ok(())
    }

    /// Obtiene un registro por su clave.
    pub async fn get(&self, key: &str) -> Option<Value> {
        let shard_id = self.shard_manager.get_shard(key);
        self.shards.get(&shard_id)?.get(key).map(|v| v.clone())
    }

    /// Elimina un registro por su clave.
    pub async fn delete(&self, key: String) -> Result<(), Box<dyn Error>> {
        let shard_id = self.shard_manager.get_shard(&key);
        if let Some(shard) = self.shards.get(&shard_id) {
            if let Some((_, value)) = shard.remove(&key) {
                update_indices_on_delete(&self.indices, &key, &value, &self.indexed_fields).await;
                save_shard_to_disk(&self.shard_manager.data_dir, shard_id, shard.clone()).await?;
                println!("Registro eliminado de Shard ID {}: {}", shard_id, key);
                return Ok(());
            }
        }
        Err("Key not found".into())
    }

    /// Realiza una consulta en la base de datos.
    pub async fn query(&self, field: &str, operator: &str, value: &str) -> Vec<Value> {
        let mut result_set = HashSet::new();
        if let Some(index) = self.indices.get(field) {
            let keys = index.query_keys(operator, value);
            println!("Consulta encontrada {} claves para la consulta.", keys.len());
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

    /// Obtiene todos los registros de la base de datos.
    pub async fn get_all_records(&self) -> Vec<Value> {
        let mut all_records = Vec::new();
        println!("Iniciando recopilación de todos los registros...");
        for shard_entry in self.shards.iter() {
            let shard_id = shard_entry.key();
            let shard = shard_entry.value();
            println!("Recopilando registros del Shard ID: {}", shard_id);
            for record in shard.iter() {
                println!("Registro encontrado en Shard {}: {}", shard_id, record.key());
                all_records.push(record.value().clone());
            }
        }
        println!("Total de registros recopilados: {}", all_records.len());
        all_records
    }
}
