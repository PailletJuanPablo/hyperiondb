// src/lib.rs

use napi::bindgen_prelude::*;
use napi_derive::napi;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::net::TcpStream;
use config::{IndexedField, IndexType};

use tokio::io::{AsyncWriteExt, AsyncReadExt};

mod hyperion_db;
mod handler;
mod index;
mod storage;
mod config;
mod shard_manager;

#[napi]
pub struct HyperionDBWrapper {
    db: Arc<Mutex<Option<hyperion_db::HyperionDB>>>,
    address: Arc<Mutex<Option<String>>>,
}

#[napi]
impl HyperionDBWrapper {
    #[napi(constructor)]
    pub fn new() -> Self {
        HyperionDBWrapper {
            db: Arc::new(Mutex::new(None)),
            address: Arc::new(Mutex::new(None)),
        }
    }

    #[napi]
    pub async fn initialize(
        &self,
        num_shards: u32,
        data_dir: String,
        indexed_fields: Vec<(String, String)>,  // Cada índice como una tupla (nombre, tipo)
        address: String,
    ) -> Result<()> {
        let indexed_fields = indexed_fields
            .into_iter()
            .map(|(field, index_type)| {
                let index_type = match index_type.as_str() {
                    "Numeric" => IndexType::Numeric,
                    "String" => IndexType::String,
                    _ => return Err(napi::Error::from_reason(format!("Invalid index type: {}", index_type))),
                };
                Ok(IndexedField { field, index_type })
            })
            .collect::<Result<Vec<_>, _>>()?;  // Recoge los resultados en un Vec<IndexedField>
    
        let config = config::Config {
            num_shards,
            data_dir,
            indexed_fields,
        };
        
        let db = hyperion_db::HyperionDB::new(config).await
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        
        let mut db_lock = self.db.lock().await;
        *db_lock = Some(db);
    
        let mut addr_lock = self.address.lock().await;
        *addr_lock = Some(address);
        
        Ok(())
    }
    #[napi]
    pub async fn handle_command(&self, command: String) -> Result<String> {
        // Obtener la dirección configurada
        let addr_lock = self.address.lock().await;
        let address = addr_lock.as_ref()
            .ok_or_else(|| napi::Error::from_reason("Address not configured".to_string()))?;

        // Conectar al servidor HyperionDB usando la dirección configurada
        let mut stream = TcpStream::connect(address).await
            .map_err(|e| napi::Error::from_reason(format!("Error connecting to HyperionDB: {}", e)))?;
        
        // Enviar el comando
        stream.write_all(command.as_bytes()).await
            .map_err(|e| napi::Error::from_reason(format!("Error sending command: {}", e)))?;
        
        // Leer la respuesta
        let mut buffer = Vec::new();
        stream.read_to_end(&mut buffer).await
            .map_err(|e| napi::Error::from_reason(format!("Error reading response: {}", e)))?;
        
        // Convertir la respuesta a String
        let response = String::from_utf8(buffer)
            .map_err(|e| napi::Error::from_reason(format!("Error converting response: {}", e)))?;
        
        Ok(response)
    }
}
