use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::error::Error;
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::RwLock;

use crate::utils::flatten_json;

pub enum Index {
    Numeric(BTreeMap<i64, HashSet<String>>),
    String(BTreeMap<String, HashSet<String>>),
}

impl Index {
    fn as_numeric_mut(&mut self) -> Option<&mut BTreeMap<i64, HashSet<String>>> {
        if let Index::Numeric(ref mut map) = self {
            Some(map)
        } else {
            None
        }
    }

    fn as_string_mut(&mut self) -> Option<&mut BTreeMap<String, HashSet<String>>> {
        if let Index::String(ref mut map) = self {
            Some(map)
        } else {
            None
        }
    }
}

fn convert_f64_to_i64(value: f64) -> i64 {
    (value * 1000.0) as i64 // Multiplicar para mantener precisión y convertir a entero
}

pub async fn update_indices_on_insert(
    indices: &Arc<RwLock<HashMap<String, Index>>>,
    key: &String,
    value: &Value,
    indexed_fields: &Vec<String>,
) {
    let mut indices_write = indices.write().await;
    if let Value::Object(map) = value {
        let mut flat_map = HashMap::new();
        flatten_json(value, None, &mut flat_map);

        for (field, field_value) in flat_map.iter() {
            if indexed_fields.contains(field) {
                match field_value {
                    Value::Number(num) => {
                        if let Some(n) = num.as_f64() {
                            let int_value = convert_f64_to_i64(n);
                            indices_write
                                .entry(field.clone())
                                .or_insert_with(|| Index::Numeric(BTreeMap::new()))
                                .as_numeric_mut()
                                .unwrap()
                                .entry(int_value)
                                .or_insert_with(HashSet::new)
                                .insert(key.clone());
                        }
                    }
                    Value::String(s) => {
                        indices_write
                            .entry(field.clone())
                            .or_insert_with(|| Index::String(BTreeMap::new()))
                            .as_string_mut()
                            .unwrap()
                            .entry(s.clone())
                            .or_insert_with(HashSet::new)
                            .insert(key.clone());
                    }
                    _ => {}
                }
            }
        }
    }
}

pub async fn update_indices_on_delete(
    indices: &Arc<RwLock<HashMap<String, Index>>>,
    key: &str,
    value: &Value,
    indexed_fields: &Vec<String>,
) {
    let mut indices_write = indices.write().await;
    if let Value::Object(map) = value {
        let mut flat_map = HashMap::new();
        flatten_json(value, None, &mut flat_map);

        for (field, field_value) in flat_map.iter() {
            if indexed_fields.contains(field) {
                match field_value {
                    Value::Number(num) => {
                        if let Some(n) = num.as_f64() {
                            let int_value = convert_f64_to_i64(n);
                            if let Some(Index::Numeric(ref mut btree_map)) =
                                indices_write.get_mut(field)
                            {
                                if let Some(keys_set) = btree_map.get_mut(&int_value) {
                                    keys_set.remove(key);
                                    if keys_set.is_empty() {
                                        btree_map.remove(&int_value);
                                    }
                                }
                                if btree_map.is_empty() {
                                    indices_write.remove(field);
                                }
                            }
                        }
                    }
                    Value::String(s) => {
                        if let Some(Index::String(ref mut btree_map)) = indices_write.get_mut(field)
                        {
                            if let Some(keys_set) = btree_map.get_mut(s) {
                                keys_set.remove(key);
                                if keys_set.is_empty() {
                                    btree_map.remove(s);
                                }
                            }
                            if btree_map.is_empty() {
                                indices_write.remove(field);
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct HyperionDB {
    pub storage: Arc<RwLock<HashMap<String, Value>>>,
    pub indices: Arc<RwLock<HashMap<String, Index>>>,
    pub data_file: String,
    pub indexed_fields: Vec<String>,
}

impl HyperionDB {
    /// Crea una nueva instancia de HyperionDB y carga datos desde el archivo si existe.
    pub async fn new(data_file: String, indexed_fields: Vec<String>) -> Self {
        let storage;
        let indices = Arc::new(RwLock::new(HashMap::new()));

        if let Ok(mut file) = File::open(&data_file).await {
            // Leer datos desde el archivo
            let mut contents = String::new();
            file.read_to_string(&mut contents).await.unwrap();
            let map: HashMap<String, Value> = serde_json::from_str(&contents).unwrap_or_default();
            storage = Arc::new(RwLock::new(map));

            // Reconstruir índices a partir de los datos cargados
            let storage_read = storage.read().await;
            for (key, value) in storage_read.iter() {
                update_indices_on_insert(&indices, key, value, &indexed_fields).await;
            }
        } else {
            // Si el archivo no existe, iniciar con un HashMap vacío
            storage = Arc::new(RwLock::new(HashMap::new()));
        }

        HyperionDB {
            storage,
            indices,
            data_file,
            indexed_fields,
        }
    }

    /// Inserta un registro en la base de datos.
    pub async fn insert(&self, key: String, value: Value) -> Result<(), Box<dyn Error>> {
        {
            let mut storage = self.storage.write().await;
            // Almacenar el valor original
            storage.insert(key.clone(), value.clone());
        }

        // Aplanar el JSON y actualizar índices
        update_indices_on_insert(&self.indices, &key, &value, &self.indexed_fields).await;

        // Guardar datos en el archivo
        self.save_to_disk().await?;

        Ok(())
    }

    /// Recupera un registro por su clave.
    pub async fn get(&self, key: &str) -> Option<Value> {
        let storage = self.storage.read().await;
        storage.get(key).cloned()
    }

    /// Elimina un registro de la base de datos y actualiza los índices correspondientes.
    pub async fn delete(&self, key: &str) -> Result<(), Box<dyn Error>> {
        let value_option = {
            let mut storage = self.storage.write().await;
            storage.remove(key)
        };

        if let Some(value) = value_option {
            // Aplanar el JSON y actualizar índices
            update_indices_on_delete(&self.indices, key, &value, &self.indexed_fields).await;

            // Guardar datos en el archivo
            self.save_to_disk().await?;

            Ok(())
        } else {
            Err("Key not found".into())
        }
    }

    /// Realiza una consulta en la base de datos con el campo, operador y valor dados.
    pub async fn query(&self, field: &str, operator: &str, value: &str) -> Vec<Value> {
        let indices = self.indices.read().await;

        if let Some(index) = indices.get(field) {
            let keys = match index {
                Index::Numeric(btree_map) => {
                    if let Ok(v) = value.parse::<f64>() {
                        let int_value = convert_f64_to_i64(v);
                        match operator {
                            "=" => btree_map.get(&int_value).cloned().unwrap_or_default(),
                            "!=" => {
                                let mut result = HashSet::new();
                                for (k, set) in btree_map {
                                    if *k != int_value {
                                        result.extend(set.clone());
                                    }
                                }
                                result
                            }
                            ">" => btree_map
                                .range((int_value + 1)..)
                                .flat_map(|(_, set)| set.clone())
                                .collect(),
                            ">=" => btree_map
                                .range(int_value..)
                                .flat_map(|(_, set)| set.clone())
                                .collect(),
                            "<" => btree_map
                                .range(..int_value)
                                .flat_map(|(_, set)| set.clone())
                                .collect(),
                            "<=" => btree_map
                                .range(..=int_value)
                                .flat_map(|(_, set)| set.clone())
                                .collect(),
                            _ => HashSet::new(),
                        }
                    } else {
                        HashSet::new()
                    }
                }
                Index::String(btree_map) => match operator {
                    "=" => btree_map.get(value).cloned().unwrap_or_default(),
                    "!=" => {
                        let mut result = HashSet::new();
                        for (k, set) in btree_map {
                            if k != value {
                                result.extend(set.clone());
                            }
                        }
                        result
                    }
                    "CONTAINS" => {
                        let mut result = HashSet::new();
                        for (_k, set) in btree_map
                            .range(value.to_string()..)
                            .take_while(|(key, _)| key.starts_with(value))
                        {
                            result.extend(set.clone());
                        }
                        result
                    }
                    _ => HashSet::new(),
                },
            };

            // Obtener los registros originales
            self.get_records_by_keys(&keys).await
        } else {
            Vec::new()
        }
    }

    /// Recupera los registros por sus claves.
    async fn get_records_by_keys(&self, keys: &HashSet<String>) -> Vec<Value> {
        let storage = self.storage.read().await;
        keys.iter()
            .filter_map(|key| storage.get(key).cloned())
            .collect()
    }

    /// Guarda el estado actual de la base de datos en disco
    pub async fn save_to_disk(&self) -> Result<(), Box<dyn Error>> {
        let storage = self.storage.read().await;
        let data = serde_json::to_string(&*storage)?;
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.data_file)
            .await?;
        file.write_all(data.as_bytes()).await?;
        Ok(())
    }

    /// Recupera todos los registros en la base de datos.
    pub async fn get_all_records(&self) -> Vec<Value> {
        let storage = self.storage.read().await;
        storage.values().cloned().collect()
    }
}