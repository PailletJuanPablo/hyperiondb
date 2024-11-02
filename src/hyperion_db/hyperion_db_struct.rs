
use crate::index::Index;
use crate::shard_manager::ShardManager;
use crate::storage::WalManager;
use dashmap::DashMap;
use serde_json::Value;
use std::sync::Arc;
use crate::config::IndexedField;

#[derive(Clone)]
pub struct HyperionDB {
    pub shards: Arc<DashMap<u32, Arc<DashMap<String, Value>>>>,
    pub indices: Arc<DashMap<String, Index>>,
    pub shard_manager: Arc<ShardManager>,
    pub indexed_fields: Vec<IndexedField>,
    pub wal_manager: Arc<WalManager>,

}
