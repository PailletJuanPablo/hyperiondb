use crate::{
    index::update_indices_on_insert,
    storage::WalManager,
};
use serde_json::Value;
use std::error::Error;
use futures::stream::{self, StreamExt};
use std::sync::Arc;
use super::hyperion_db_struct::HyperionDB;

impl HyperionDB {
    pub async fn insert_or_update(&self, key: String, value: Value) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        let shard_id = self.shard_manager.get_shard(&key);

        if let Some(shard) = self.shards.get(&shard_id) {
            shard.insert(key.clone(), value.clone());

            let data_dir = self.shard_manager.data_dir.clone();
            let key_clone = key.clone();
            let value_clone = value.clone();

            // Utiliza WalManager para gestionar la concurrencia en la escritura al WAL
            tokio::spawn({
                let wal_manager: Arc<WalManager> = Arc::clone(&self.wal_manager);
                let data_dir = data_dir.clone();
                async move {
                    wal_manager.append_to_wal(&data_dir, shard_id, key_clone, value_clone).await.unwrap();
                }
            });

            // Actualiza los índices si es necesario
            update_indices_on_insert(&self.indices, &key, &value, &self.indexed_fields).await;
        }
        Ok(())
    }

    pub async fn insert_or_update_many(
        &self,
        data: Vec<(String, Value)>,
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        let batch_size = 10000;
        let data_dir = self.shard_manager.data_dir.clone();
        let indices = self.indices.clone();
        let indexed_fields = self.indexed_fields.clone();
        let shard_manager = Arc::clone(&self.shard_manager);
        let shards = Arc::clone(&self.shards);
        let wal_manager: Arc<WalManager> = Arc::clone(&self.wal_manager);

        stream::iter(data.chunks(batch_size).map(|chunk| chunk.to_vec()))
            .for_each_concurrent(None, move |chunk| {
                let data_dir = data_dir.clone();
                let indices = indices.clone();
                let indexed_fields = indexed_fields.clone();
                let shard_manager = Arc::clone(&shard_manager);
                let shards = Arc::clone(&shards);
                let wal_manager = Arc::clone(&wal_manager);

                async move {
                    for (key, value) in chunk {
                        let shard_id = shard_manager.get_shard(&key);
                        if let Some(shard) = shards.get(&shard_id) {
                            shard.insert(key.clone(), value.clone());

                            // Utiliza WalManager para la escritura en el WAL
                            if let Err(e) = wal_manager.append_to_wal(&data_dir, shard_id, key.clone(), value.clone()).await {
                                eprintln!("Error al escribir en el WAL: {}", e);
                            }

                            // Actualiza los índices
                            update_indices_on_insert(&indices, &key, &value, &indexed_fields).await;
                        }
                    }
                }
            })
            .await;

        Ok(())
    }
}
