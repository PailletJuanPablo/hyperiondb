// src/utils.rs

use serde_json::Value;
use std::collections::HashMap;

/// Aplana una estructura JSON en un mapa de claves aplanadas y valores.
pub fn flatten_json(
    value: &Value,
    prefix: Option<String>,
    map: &mut HashMap<String, Value>,
) {
    match value {
        Value::Object(obj) => {
            for (k, v) in obj {
                let new_key = match &prefix {
                    Some(p) => format!("{}.{}", p, k),
                    None => k.clone(),
                };
                flatten_json(v, Some(new_key), map);
            }
        }
        Value::Array(arr) => {
            for (i, v) in arr.iter().enumerate() {
                let new_key = match &prefix {
                    Some(p) => format!("{}.{}", p, i),
                    None => i.to_string(),
                };
                flatten_json(v, Some(new_key), map);
            }
        }
        _ => {
            if let Some(p) = prefix {
                map.insert(p, value.clone());
            }
        }
    }
}
