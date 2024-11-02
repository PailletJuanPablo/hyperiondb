use crate::{
    hyperion_db::hyperion_db_struct::HyperionDB,
    index::update_indices_on_insert,
    storage::append_to_wal,
};
use serde_json::Value;
use std::error::Error;
use futures::stream::{self, StreamExt}; // Usar StreamExt de futures
use std::sync::Arc;

impl HyperionDB {
    pub async fn insert_or_update(&self, key: String, value: Value) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        let shard_id = self.shard_manager.get_shard(&key);

        if let Some(shard) = self.shards.get(&shard_id) {
            shard.insert(key.clone(), value.clone());

            let data_dir = self.shard_manager.data_dir.clone();
            let key_clone = key.clone();
            let value_clone = value.clone();

            // Append the operation to the write-ahead log asynchronously
            tokio::spawn({
                let data_dir = data_dir.clone();
                async move {
                    append_to_wal(&data_dir, shard_id, key_clone, value_clone).await.unwrap();
                }
            });

            // Update indices if necessary
            update_indices_on_insert(&self.indices, &key, &value, &self.indexed_fields).await;
        }
        Ok(())
    }

    pub async fn insert_or_update_many(
        &self,
        data: Vec<(String, Value)>,
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        let batch_size = 250; // Procesaremos en lotes de 1000 por defecto
        let data_dir = self.shard_manager.data_dir.clone();
        let indices = self.indices.clone();
        let indexed_fields = self.indexed_fields.clone();
        let shard_manager = Arc::clone(&self.shard_manager);
        let shards = Arc::clone(&self.shards);

        stream::iter(data.chunks(batch_size).map(|chunk| chunk.to_vec())) // Clonamos los datos de cada chunk
            .for_each_concurrent(None, |chunk| {
                let data_dir = data_dir.clone();
                let indices = indices.clone();
                let indexed_fields = indexed_fields.clone();
                let shard_manager = Arc::clone(&shard_manager);
                let shards = Arc::clone(&shards);

                async move {
                    for (key, value) in chunk {
                        let shard_id = shard_manager.get_shard(&key);
                        if let Some(shard) = shards.get(&shard_id) {
                            shard.insert(key.clone(), value.clone());

                            // Intentamos escribir en el WAL
                            match append_to_wal(&data_dir, shard_id, key.clone(), value.clone()).await {
                                Ok(_) => {}
                                Err(e) => {
                                    eprintln!("Error al escribir en el WAL: {}", e);
                                }
                            }

                            // Actualizamos los índices
                            update_indices_on_insert(&indices, &key, &value, &indexed_fields).await;
                        }
                    }
                }
            })
            .await;

        Ok(())
    }
}
