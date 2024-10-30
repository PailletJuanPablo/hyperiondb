use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::fs::{File, OpenOptions};
use std::error::Error;
use tokio::io::AsyncReadExt;

#[derive(Clone)]
struct HyperionDB {
    storage: Arc<RwLock<HashMap<String, Value>>>,
    indices: Arc<RwLock<HashMap<String, HashMap<String, HashSet<String>>>>>,
    data_file: String,
}

impl HyperionDB {
    async fn new(data_file: String) -> Self {
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
                update_indices_on_insert(&indices, key, value).await;
            }
        } else {
            // Si el archivo no existe, iniciar con un HashMap vacío
            storage = Arc::new(RwLock::new(HashMap::new()));
        }

        HyperionDB {
            storage,
            indices,
            data_file,
        }
    }

    async fn insert(&self, key: String, value: Value) -> Result<(), Box<dyn Error>> {
        {
            let mut storage = self.storage.write().await;
            storage.insert(key.clone(), value.clone());
        }
        update_indices_on_insert(&self.indices, &key, &value).await;
        self.save_to_disk().await?;
        Ok(())
    }

    async fn get(&self, key: &str) -> Option<Value> {
        let storage = self.storage.read().await;
        storage.get(key).cloned()
    }

    async fn delete(&self, key: &str) -> Result<(), Box<dyn Error>> {
        let value_opt;
        {
            let mut storage = self.storage.write().await;
            value_opt = storage.remove(key);
        }
        if let Some(value) = value_opt {
            update_indices_on_delete(&self.indices, key, &value).await;
        }
        self.save_to_disk().await?;
        Ok(())
    }

    async fn save_to_disk(&self) -> Result<(), Box<dyn Error>> {
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
}

// Funciones independientes para actualizar los índices
async fn update_indices_on_insert(
    indices: &Arc<RwLock<HashMap<String, HashMap<String, HashSet<String>>>>>,
    key: &String,
    value: &Value,
) {
    let mut indices_write = indices.write().await;
    if let Value::Object(map) = value {
        for (field, field_value) in map.iter() {
            let field_value_str = field_value.to_string();
            indices_write
                .entry(field.clone())
                .or_insert_with(HashMap::new)
                .entry(field_value_str)
                .or_insert_with(HashSet::new)
                .insert(key.clone());
        }
    }
}

async fn update_indices_on_delete(
    indices: &Arc<RwLock<HashMap<String, HashMap<String, HashSet<String>>>>>,
    key: &str,
    value: &Value,
) {
    let mut indices_write = indices.write().await;
    if let Value::Object(map) = value {
        for (field, field_value) in map.iter() {
            let field_value_str = field_value.to_string();
            if let Some(field_map) = indices_write.get_mut(field) {
                if let Some(keys_set) = field_map.get_mut(&field_value_str) {
                    keys_set.remove(key);
                    if keys_set.is_empty() {
                        field_map.remove(&field_value_str);
                    }
                }
                if field_map.is_empty() {
                    indices_write.remove(field);
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let db = HyperionDB::new("hyperiondb_data.json".to_string()).await;
    let listener = TcpListener::bind("127.0.0.1:8080").await?;

    println!("HyperionDB Server running on 127.0.0.1:8080");

    loop {
        let (socket, _) = listener.accept().await?;
        let db = db.clone();

        tokio::spawn(async move {
            let (reader, mut writer) = socket.into_split();
            let mut reader = BufReader::new(reader);
            let mut line = String::new();

            while let Ok(bytes_read) = reader.read_line(&mut line).await {
                if bytes_read == 0 {
                    break;
                }

                let response = match handle_command(&db, line.trim().to_string()).await {
                    Ok(resp) => resp,
                    Err(e) => format!("ERR {}\n", e),
                };

                if let Err(e) = writer.write_all(response.as_bytes()).await {
                    eprintln!("Failed to write to socket: {}", e);
                    break;
                }

                line.clear();
            }
        });
    }
}

async fn handle_command(db: &HyperionDB, command: String) -> Result<String, Box<dyn Error>> {
    let parts: Vec<&str> = command.trim().splitn(3, ' ').collect();
    let cmd = parts.get(0).unwrap_or(&"").to_uppercase();

    match cmd.as_str() {
        "INSERT" => {
            if let (Some(key), Some(value_str)) = (parts.get(1), parts.get(2)) {
                match serde_json::from_str::<Value>(value_str) {
                    Ok(value) => {
                        db.insert(key.to_string(), value).await?;
                        Ok("OK\n".to_string())
                    }
                    Err(_) => Ok("ERR Invalid JSON\n".to_string()),
                }
            } else {
                Ok("ERR Usage: INSERT <key> <json>\n".to_string())
            }
        }
        "GET" => {
            if let Some(key) = parts.get(1) {
                match db.get(key).await {
                    Some(value) => Ok(format!("{}\n", value.to_string())),
                    None => Ok("NULL\n".to_string()),
                }
            } else {
                Ok("ERR Usage: GET <key>\n".to_string())
            }
        }
        "DELETE" => {
            if let Some(key) = parts.get(1) {
                db.delete(key).await?;
                Ok("OK\n".to_string())
            } else {
                Ok("ERR Usage: DELETE <key>\n".to_string())
            }
        }
        "LIST" => {
            let storage = db.storage.read().await;
            let all_keys: Vec<String> = storage.keys().cloned().collect();
            Ok(format!("{}\n", serde_json::to_string(&all_keys)?))
        }
        "QUERY" => {
            if let (Some(field), Some(value)) = (parts.get(1), parts.get(2)) {
                let indices = db.indices.read().await;
                if let Some(field_map) = indices.get(*field) {
                    if let Some(keys_set) = field_map.get(&value.to_string()) {
                        let keys_vec: Vec<String> = keys_set.iter().cloned().collect();
                        Ok(format!("{}\n", serde_json::to_string(&keys_vec)?))
                    } else {
                        Ok("[]\n".to_string())
                    }
                } else {
                    Ok("[]\n".to_string())
                }
            } else {
                Ok("ERR Usage: QUERY <field> <value>\n".to_string())
            }
        }
        "EXIT" => Ok("BYE\n".to_string()),
        _ => Ok("ERR Unknown command\n".to_string()),
    }
}
