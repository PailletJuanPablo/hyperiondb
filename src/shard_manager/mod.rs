// src/shard_manager/mod.rs

use seahash::hash;

/// Gestiona la asignación de claves a shards.

#[derive(Clone)]
pub struct ShardManager {
    pub num_shards: u32,
    pub data_dir: String,
}

impl ShardManager {
    /// Crea una nueva instancia de ShardManager.
    pub fn new(num_shards: u32, data_dir: String) -> Self {
        ShardManager {
            num_shards,
            data_dir,
        }
    }

    /// Determina a qué shard pertenece una clave dada.
    pub fn get_shard(&self, key: &str) -> u32 {
        let hash_value = hash(key.as_bytes());
        (hash_value % self.num_shards as u64) as u32
    }
}
