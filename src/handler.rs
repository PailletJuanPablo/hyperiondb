// src/handler.rs

use crate::db::HyperionDB;
use serde_json::Value;
use std::error::Error;

/// Maneja los comandos recibidos y ejecuta las operaciones correspondientes
pub async fn handle_command(db: &HyperionDB, command: String) -> Result<String, Box<dyn Error>> {
    let parts: Vec<&str> = command.trim().splitn(4, ' ').collect();
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
                match db.get(*key).await {
                    Some(value) => Ok(format!("{}\n", value.to_string())),
                    None => Ok("NULL\n".to_string()),
                }
            } else {
                Ok("ERR Usage: GET <key>\n".to_string())
            }
        }
        "DELETE" => {
            if let Some(key) = parts.get(1) {
                db.delete(*key).await?;
                Ok("OK\n".to_string())
            } else {
                Ok("ERR Usage: DELETE <key>\n".to_string())
            }
        }

        "LIST" => {
            let records = db.get_all_records().await;
            Ok(format!("{}\n", serde_json::to_string(&records)?))
        }
        "QUERY" => {
            if let (Some(field), Some(operator), Some(value)) =
                (parts.get(1), parts.get(2), parts.get(3))
            {
                let results = db.query(field, operator, value).await;
                Ok(format!("{}\n", serde_json::to_string(&results)?))
            } else {
                Ok("ERR Usage: QUERY <field> <operator> <value>\n".to_string())
            }
        }
        "EXIT" => Ok("BYE\n".to_string()),
        _ => Ok("ERR Unknown command\n".to_string()),
    }
}
