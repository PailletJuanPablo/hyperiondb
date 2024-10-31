
use super::hyperion_db_struct::HyperionDB;
use serde_json::Value;

impl HyperionDB {
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
