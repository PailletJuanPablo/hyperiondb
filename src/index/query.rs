use std::collections::{BTreeMap, HashSet};

/// Queries a numeric index map with the given operator and value.
pub fn query_numeric(
    map: &BTreeMap<i64, HashSet<String>>,
    operator: &str,
    value: &str,
) -> HashSet<String> {
    let mut result = HashSet::new();
    if let Ok(v) = value.parse::<f64>() {
        let int_value = (v * 1000.0) as i64; // Convert to integer for indexing
        match operator {
            "=" => {
                if let Some(set) = map.get(&int_value) {
                    result.extend(set.clone());
                }
            }
            "!=" => {
                for (&k, set) in map.iter() {
                    if k != int_value {
                        result.extend(set.clone());
                    }
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

/// Queries a string index map with the given operator and value.
pub fn query_string(
    map: &BTreeMap<String, HashSet<String>>,
    operator: &str,
    value: &str,
) -> HashSet<String> {
    let mut result = HashSet::new();
    match operator {
        "=" => {
            if let Some(set) = map.get(value) {
                result.extend(set.clone());
            }
        }
        "!=" => {
            for (k, set) in map.iter() {
                if k != value {
                    result.extend(set.clone());
                }
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
