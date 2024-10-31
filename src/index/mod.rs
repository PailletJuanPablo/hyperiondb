use serde_json::Value;
use std::collections::{BTreeMap, HashSet};
use dashmap::DashMap;
use crate::config::{IndexedField, IndexType};

/// Representa los tipos de índices en la base de datos.
pub enum Index {
    Numeric(BTreeMap<i64, HashSet<String>>),
    String(BTreeMap<String, HashSet<String>>),
}

impl Index {
    pub fn as_numeric_mut(&mut self) -> Option<&mut BTreeMap<i64, HashSet<String>>> {
        if let Index::Numeric(ref mut map) = self {
            Some(map)
        } else {
            None
        }
    }

    pub fn as_string_mut(&mut self) -> Option<&mut BTreeMap<String, HashSet<String>>> {
        if let Index::String(ref mut map) = self {
            Some(map)
        } else {
            None
        }
    }
    pub fn query_keys(&self, operator: &str, value: &str) -> HashSet<String> {
        match self {
            Index::Numeric(btree_map) => query_numeric(btree_map, operator, value),
            Index::String(btree_map) => query_string(btree_map, operator, value),
        }
    }
}

/// Ejecuta una consulta en índices numéricos.
fn query_numeric(map: &BTreeMap<i64, HashSet<String>>, operator: &str, value: &str) -> HashSet<String> {
    let mut result = HashSet::new();
    if let Ok(v) = value.parse::<f64>() {
        let int_value = (v * 1000.0) as i64; // Convertir a entero para el índice
        match operator {
            "=" => {
                if let Some(set) = map.get(&int_value) {
                    result.extend(set.clone());
                }
            }
            "!=" => {
                for (_, set) in map.iter() {
                    result.extend(set.clone());
                }
            }
            ">" => {
                for (_, set) in map.range((int_value + 1)..) {
                    result.extend(set.clone());
                }
            }
            ">=" => {
                for (_, set) in map.range(int_value..) {
                    result.extend(set.clone());
                }
            }
            "<" => {
                for (_, set) in map.range(..int_value) {
                    result.extend(set.clone());
                }
            }
            "<=" => {
                for (_, set) in map.range(..=int_value) {
                    result.extend(set.clone());
                }
            }
            _ => {}
        }
    }
    result
}

/// Ejecuta una consulta en índices de cadena.
fn query_string(map: &BTreeMap<String, HashSet<String>>, operator: &str, value: &str) -> HashSet<String> {
    let mut result = HashSet::new();
    match operator {
        "=" => {
            if let Some(set) = map.get(value) {
                result.extend(set.clone());
            }
        }
        "!=" => {
            for (_, set) in map.iter() {
                result.extend(set.clone());
            }
        }
        "CONTAINS" => {
            for (k, set) in map.iter() {
                if k.contains(value) {
                    result.extend(set.clone());
                }
            }
        }
        _ => {}
    }
    result
}

/// Actualiza los índices al insertar un registro.
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
                                let int_value = (n * 1000.0) as i64; // Convertir a entero
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


/// Obtiene el campo anidado según el índice.
fn get_nested_field<'a>(map: &'a serde_json::Map<String, Value>, field: &str) -> Option<&'a Value> {
    let parts: Vec<&str> = field.split('.').collect();
    let mut current = map;
    for part in parts.iter() {
        if let Some(Value::Object(ref obj)) = current.get(*part) {
            current = obj;
        } else {
            return current.get(*part);
        }
    }
    None
}
