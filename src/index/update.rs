
use serde_json::Value;
use dashmap::DashMap;
use std::collections::{BTreeMap, HashSet};
use crate::config::{IndexedField, IndexType};
use crate::index::Index;
use crate::index::utils::get_nested_field;


/// Updates the indices when a new key-value pair is inserted.
///
/// This function iterates over each indexed field specified in `indexed_fields` and updates the corresponding index
/// in `indices` based on the value of that field in the provided `value`. Supports both numeric and string index types.
///
/// # Arguments
///
/// * `indices` - A concurrent map (`DashMap`) that holds the indices for different fields.
/// * `key` - The key associated with the value being inserted.
/// * `value` - The JSON `Value` being inserted into the database.
/// * `indexed_fields` - A vector of `IndexedField` specifying the fields to be indexed and their index types.
///
/// # Example
///
/// ```rust
/// use serde_json::json;
/// use dashmap::DashMap;
/// use crate::config::{IndexedField, IndexType};
/// use crate::index::Index;
/// use crate::index::update::update_indices_on_insert;
///
/// #[tokio::main]
/// async fn main() {
///     let indices = DashMap::new();
///     let key = "key1".to_string();
///     let value = json!({"field1": 42, "field2": "value"});
///     let indexed_fields = vec![
///         IndexedField { field: "field1".to_string(), index_type: IndexType::Numeric },
///         IndexedField { field: "field2".to_string(), index_type: IndexType::String },
///     ];
///
pub async fn update_indices_on_insert(
    indices: &DashMap<String, Index>,
    key: &String,
    value: &Value,
    indexed_fields: &Vec<IndexedField>,
) {
    if let Value::Object(map) = value {
        for indexed_field in indexed_fields.iter() {
            let field = &indexed_field.field;
            let index_type = &indexed_field.index_type;

            if let Some(field_value) = get_nested_field(map, field) {
                match index_type {
                    IndexType::Numeric => {
                        if let Value::Number(num) = field_value {
                            if let Some(n) = num.as_f64() {
                                let int_value = (n * 1000.0) as i64; // Convert to integer
                                indices
                                    .entry(field.clone())
                                    .or_insert_with(|| Index::Numeric(BTreeMap::new()));
                                if let Some(mut index) = indices.get_mut(field) {
                                    if let Index::Numeric(ref mut btree_map) = *index {
                                        btree_map
                                            .entry(int_value)
                                            .or_insert_with(HashSet::new)
                                            .insert(key.clone());
                                    }
                                }
                            }
                        }
                    }
                    IndexType::String => {
                        if let Value::String(s) = field_value {
                            indices
                                .entry(field.clone())
                                .or_insert_with(|| Index::String(BTreeMap::new()));
                            if let Some(mut index) = indices.get_mut(field) {
                                if let Index::String(ref mut btree_map) = *index {
                                    btree_map
                                        .entry(s.clone())
                                        .or_insert_with(HashSet::new)
                                        .insert(key.clone());
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}


/// Updates the indices when a key-value pair is deleted.
///
/// This function iterates over each indexed field specified in `indexed_fields` and updates the corresponding index
/// in `indices` based on the value of that field in the provided `value`. Supports both numeric and string index types.
///
/// # Arguments
///
/// * `indices` - A concurrent map (`DashMap`) that holds the indices for different fields.
/// * `key` - The key associated with the value being deleted.
/// * `value` - The JSON `Value` being deleted from the database.
/// * `indexed_fields` - A vector of `IndexedField` specifying the fields to be indexed and their index types.
///
/// # Example
///
/// ```rust
/// use serde_json::json;
/// use dashmap::DashMap;
/// use crate::config::{IndexedField, IndexType};
/// use crate::index::Index;
/// use crate::index::update::update_indices_on_delete;
///
/// #[tokio::main]
/// async fn main() {
///     let indices = DashMap::new();
///     let key = "key1".to_string();
///     let value = json!({"field1": 42, "field2": "value"});
///     let indexed_fields = vec![
///         IndexedField { field: "field1".to_string(), index_type: IndexType::Numeric },
///         IndexedField { field: "field2".to_string(), index_type: IndexType::String },
///     ];
///
pub async fn update_indices_on_delete(
    indices: &DashMap<String, Index>,
    key: &String,
    value: &Value,
    indexed_fields: &Vec<IndexedField>,
) {
    if let Value::Object(map) = value {
        for indexed_field in indexed_fields.iter() {
            let field = &indexed_field.field;
            let index_type = &indexed_field.index_type;

            if let Some(field_value) = get_nested_field(map, field) {
                match index_type {
                    IndexType::Numeric => {
                        if let Value::Number(num) = field_value {
                            if let Some(n) = num.as_f64() {
                                let int_value = (n * 1000.0) as i64;
                                if let Some(mut index) = indices.get_mut(field) {
                                    if let Some(btree_map) = index.as_numeric_mut() {
                                        if let Some(keys_set) = btree_map.get_mut(&int_value) {
                                            keys_set.remove(key);
                                            if keys_set.is_empty() {
                                                btree_map.remove(&int_value);
                                            }
                                        }
                                        if btree_map.is_empty() {
                                            indices.remove(field);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    IndexType::String => {
                        if let Value::String(s) = field_value {
                            if let Some(mut index) = indices.get_mut(field) {
                                if let Some(btree_map) = index.as_string_mut() {
                                    if let Some(keys_set) = btree_map.get_mut(s) {
                                        keys_set.remove(key);
                                        if keys_set.is_empty() {
                                            btree_map.remove(s);
                                        }
                                    }
                                    if btree_map.is_empty() {
                                        indices.remove(field);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
