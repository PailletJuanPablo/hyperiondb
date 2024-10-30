// src/handler/mod.rs

use crate::hyperion_db::HyperionDB;
// Eliminado: use serde_json::json;
use std::error::Error;

/// Maneja los comandos recibidos y ejecuta las operaciones correspondientes.
pub async fn handle_command(db: &HyperionDB, command: String) -> Result<String, Box<dyn Error>> {
    let cmd_line = command.trim();
    let cmd_parts: Vec<&str> = cmd_line.splitn(2, ' ').collect();
    let cmd = cmd_parts.get(0).unwrap_or(&"").to_uppercase();

    match cmd.as_str() {
        "INSERT" => {
            if let Some(rest) = cmd_parts.get(1) {
                let insert_parts: Vec<&str> = rest.trim().splitn(2, ' ').collect();
                if let (Some(key), Some(value_str)) = (insert_parts.get(0), insert_parts.get(1)) {
                    // Intentamos deserializar el valor JSON
                    let value: serde_json::Value = serde_json::from_str(value_str)?;
                    // Insertamos el registro en la base de datos
                    db.insert(key.to_string(), value).await?;
                    Ok("OK\n".to_string())
                } else {
                    // Formato incorrecto de comando INSERT
                    Ok("ERR Usage: INSERT <key> <value>\n".to_string())
                }
            } else {
                // Formato incorrecto de comando INSERT
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
                // Formato incorrecto de comando GET
                Ok("ERR Usage: GET <key>\n".to_string())
            }
        }
        "DELETE" => {
            if let Some(key) = cmd_parts.get(1) {
                db.delete(key.to_string()).await?; // Convertimos &str a String
                Ok("OK\n".to_string())
            } else {
                // Formato incorrecto de comando DELETE
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
                // Dividimos el comando en 3 partes: campo, operador y valor
                let mut parts = rest.splitn(3, ' ');
                let field = parts.next();
                let operator = parts.next();
                let value = parts.next();

                if let (Some(field), Some(operator), Some(value)) = (field, operator, value) {
                    // Ejecutamos la consulta en la base de datos
                    let results = db.query(field, operator, value).await;
                    Ok(format!("{}\n", serde_json::to_string(&results)?))
                } else {
                    // Formato incorrecto de comando QUERY
                    Ok("ERR Usage: QUERY <field> <operator> <value>\n".to_string())
                }
            } else {
                // Formato incorrecto de comando QUERY
                Ok("ERR Usage: QUERY <field> <operator> <value>\n".to_string())
            }
        }
        "EXIT" => Ok("BYE\n".to_string()),
        _ => Ok("ERR Unknown command\n".to_string()),
    }
}
