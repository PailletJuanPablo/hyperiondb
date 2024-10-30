// src/handler.rs

use crate::db::HyperionDB;
use std::error::Error;

/// Maneja los comandos recibidos y ejecuta las operaciones correspondientes
pub async fn handle_command(db: &HyperionDB, command: String) -> Result<String, Box<dyn Error>> {
    let cmd_line = command.trim();
    let cmd_parts: Vec<&str> = cmd_line.splitn(2, ' ').collect();
    let cmd = cmd_parts.get(0).unwrap_or(&"").to_uppercase();

    match cmd.as_str() {
          "INSERT" => {
            if let Some(rest) = cmd_parts.get(1) {
                let insert_parts: Vec<&str> = rest.trim().splitn(2, ' ').collect();
                if let (Some(key), Some(value_str)) = (insert_parts.get(0), insert_parts.get(1)) {
                    let value: serde_json::Value = serde_json::from_str(value_str)?;
                    db.insert(key.to_string(), value).await?;
                    Ok("OK\n".to_string())
                } else {
                    Ok("ERR Usage: INSERT <key> <value>\n".to_string())
                }
            } else {
                Ok("ERR Usage: INSERT <key> <value>\n".to_string())
            }
        }
        "GET" => {
            if let Some(key) = cmd_parts.get(1) {
                match db.get(*key).await {
                    Some(value) => Ok(format!("{}\n", value.to_string())),
                    None => Ok("NULL\n".to_string()),
                }
            } else {
                Ok("ERR Usage: GET <key>\n".to_string())
            }
        }
        "DELETE" => {
            if let Some(key) = cmd_parts.get(1) {
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
            if let Some(rest) = cmd_parts.get(1) {
                let rest = rest.trim();
                let mut parts = rest.split_whitespace();

                if let (Some(field), Some(operator)) = (parts.next(), parts.next()) {
                    // Obtener el valor restante después del operador
                    let value_start = field.len() + operator.len() + 2; // +2 por los espacios
                    if value_start <= rest.len() {
                        let value = &rest[value_start..].trim();
                        let results = db.query(field, operator, value).await;
                        Ok(format!("{}\n", serde_json::to_string(&results)?))
                    } else {
                        Ok("ERR Usage: QUERY <field> <operator> <value>\n".to_string())
                    }
                } else {
                    Ok("ERR Usage: QUERY <field> <operator> <value>\n".to_string())
                }
            } else {
                Ok("ERR Usage: QUERY <field> <operator> <value>\n".to_string())
            }
        }
        "EXIT" => Ok("BYE\n".to_string()),
        _ => Ok("ERR Unknown command\n".to_string()),
    }
}
