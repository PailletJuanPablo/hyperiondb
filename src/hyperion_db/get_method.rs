
use super::hyperion_db_struct::HyperionDB;
use serde_json::Value;

impl HyperionDB {
    pub async fn get(&self, key: &str) -> Option<Value> {
        let shard_id = self.shard_manager.get_shard(key);
        self.shards.get(&shard_id)?.get(key).map(|v| v.clone())
    }
}
