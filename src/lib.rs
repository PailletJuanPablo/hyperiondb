#![deny(clippy::all)]

use config::{IndexType, IndexedField};
use handler::handle_command;
use napi::bindgen_prelude::*;
use napi_derive::napi;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

use tokio::io::{AsyncReadExt, AsyncBufReadExt, AsyncWriteExt, BufReader};

mod config;
mod handler;
mod hyperion_db;
mod index;
mod shard_manager;
mod storage;

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
    indexed_fields: Vec<(String, String)>,
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
      .collect::<Result<Vec<_>, _>>()?;

    let config = config::Config {
      num_shards,
      data_dir,
      indexed_fields,
    };

    let db = hyperion_db::HyperionDB::new(config)
      .await
      .map_err(|e| napi::Error::from_reason(e.to_string()))?;

    let mut db_lock = self.db.lock().await;
    *db_lock = Some(db);

    let mut addr_lock = self.address.lock().await;
    *addr_lock = Some(address);

    Ok(())
  }

  #[napi]
  pub async fn start_server(&self, port: u16) -> Result<()> {
    let db_lock = self.db.clone();
    let address = format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(&address).await
      .map_err(|e| napi::Error::from_reason(format!("Error binding to address {}: {}", address, e)))?;

    println!("HyperionDB Server running on {}", address);

    tokio::spawn(async move {
      loop {
        let (socket, _) = match listener.accept().await {
          Ok(connection) => connection,
          Err(e) => {
            eprintln!("Failed to accept connection: {}", e);
            continue;
          }
        };

        let db_lock = db_lock.clone();
        tokio::spawn(async move {
          let (reader, mut writer) = socket.into_split();
          let mut reader = BufReader::new(reader);
          let mut line = String::new();

          while let Ok(bytes_read) = reader.read_line(&mut line).await {
            if bytes_read == 0 {
              break;
            }

            let db = db_lock.lock().await;
            let db_ref = db.as_ref().expect("Database not initialized");

            let response = match handle_command(db_ref, line.trim().to_string()).await {
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
    });

    Ok(())
  }

  #[napi]
  pub async fn query(&self, query_str: String) -> Result<String> {
    println!("Ejecutando query: {}", query_str);

    let addr_lock = self.address.lock().await;
    let address = addr_lock
      .as_ref()
      .ok_or_else(|| napi::Error::from_reason("Address not configured".to_string()))?;

    let mut stream = TcpStream::connect(address)
      .await
      .map_err(|e| napi::Error::from_reason(format!("Error connecting to HyperionDB: {}", e)))?;

    let command = format!("QUERY {}\n", query_str);
    stream
      .write_all(command.as_bytes())
      .await
      .map_err(|e| napi::Error::from_reason(format!("Error sending command: {}", e)))?;

    let mut buffer = Vec::new();
    stream
      .read_to_end(&mut buffer)
      .await
      .map_err(|e| napi::Error::from_reason(format!("Error reading response: {}", e)))?;

    let response = String::from_utf8(buffer)
      .map_err(|e| napi::Error::from_reason(format!("Error converting response: {}", e)))?;

    println!("Respuesta recibida: {}", response);
    Ok(response)
  }
}
