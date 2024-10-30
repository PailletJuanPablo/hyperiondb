// src/index/mod.rs

use serde_json::Value;
use std::collections::{BTreeMap, HashSet};
use dashmap::DashMap;

/// Enum para representar diferentes tipos de índices.
pub enum Index {
    Numeric(BTreeMap<i64, HashSet<String>>),
    String(BTreeMap<String, HashSet<String>>),
}

impl Index {
    /// Retorna los keys que coinciden con la consulta.
    pub fn query_keys(&self, operator: &str, value: &str) -> HashSet<String> {
        match self {
            Index::Numeric(btree_map) => query_numeric(btree_map, operator, value),
            Index::String(btree_map) => query_string(btree_map, operator, value),
        }
    }
}

/// Consulta para índices numéricos.
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
                for (_, set) in map.iter() { // Renombrado 'k' a '_'
                    result.extend(set.clone());
                }
            }
            ">" => {
                for (_, set) in map.range((int_value + 1)..) { // Renombrado 'k' a '_'
                    result.extend(set.clone());
                }
            }
            ">=" => {
                for (_, set) in map.range(int_value..) { // Renombrado 'k' a '_'
                    result.extend(set.clone());
                }
            }
            "<" => {
                for (_, set) in map.range(..int_value) { // Renombrado 'k' a '_'
                    result.extend(set.clone());
                }
            }
            "<=" => {
                for (_, set) in map.range(..=int_value) { // Renombrado 'k' a '_'
                    result.extend(set.clone());
                }
            }
            _ => {}
        }
    }
    result
}

/// Consulta para índices de cadena.
fn query_string(map: &BTreeMap<String, HashSet<String>>, operator: &str, value: &str) -> HashSet<String> {
    let mut result = HashSet::new();
    match operator {
        "=" => {
            if let Some(set) = map.get(value) {
                result.extend(set.clone());
            }
        }
        "!=" => {
            for (_, set) in map.iter() { // Renombrado 'k' a '_'
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
    indexed_fields: &Vec<String>,
) {
    if let Value::Object(map) = value {
        for field in indexed_fields.iter() {
            if let Some(field_value) = get_nested_field(map, field) {
                match field_value {
                    Value::Number(num) => {
                        if let Some(n) = num.as_f64() {
                            let int_value = (n * 1000.0) as i64; // Convertir a entero
                            indices.entry(field.clone()).or_insert_with(|| Index::Numeric(BTreeMap::new()));
                            if let Index::Numeric(ref mut btree_map) = *indices.get_mut(field).unwrap() {
                                btree_map.entry(int_value).or_insert_with(HashSet::new).insert(key.clone());
                            }
                        }
                    }
                    Value::String(s) => {
                        indices.entry(field.clone()).or_insert_with(|| Index::String(BTreeMap::new()));
                        if let Index::String(ref mut btree_map) = *indices.get_mut(field).unwrap() {
                            btree_map.entry(s.clone()).or_insert_with(HashSet::new).insert(key.clone());
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}

/// Actualiza los índices al eliminar un registro.
pub async fn update_indices_on_delete(
    indices: &DashMap<String, Index>,
    key: &str,
    value: &Value,
    indexed_fields: &Vec<String>,
) {
    if let Value::Object(map) = value {
        for field in indexed_fields.iter() {
            if let Some(field_value) = get_nested_field(map, field) {
                match field_value {
                    Value::Number(num) => {
                        if let Some(n) = num.as_f64() {
                            let int_value = (n * 1000.0) as i64; // Convertir a entero
                            if let Some(mut index) = indices.get_mut(field) {
                                if let Index::Numeric(ref mut btree_map) = *index {
                                    if let Some(set) = btree_map.get_mut(&int_value) {
                                        set.remove(key);
                                        if set.is_empty() {
                                            btree_map.remove(&int_value);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Value::String(s) => {
                        if let Some(mut index) = indices.get_mut(field) {
                            if let Index::String(ref mut btree_map) = *index {
                                if let Some(set) = btree_map.get_mut(s) {
                                    set.remove(key);
                                    if set.is_empty() {
                                        btree_map.remove(s);
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}

/// Función auxiliar para obtener campos anidados.
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
