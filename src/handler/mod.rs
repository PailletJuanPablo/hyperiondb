use serde_json::Value;

use crate::hyperion_db::HyperionDB;
use std::error::Error;

pub struct Condition {
    pub field: String,
    pub operator: String,
    pub value: String,
}

pub enum Expr {
    Condition(Condition),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Group(Box<Expr>),
}

fn tokenize(s: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut chars = s.chars().peekable();
    while let Some(&c) = chars.peek() {
        if c.is_whitespace() {
            chars.next();
            continue;
        } else if c == '"' {
            chars.next(); 
            let mut token = String::new();
            while let Some(&ch) = chars.peek() {
                if ch == '"' {
                    chars.next(); 
                    break;
                } else {
                    token.push(ch);
                    chars.next();
                }
            }
            tokens.push(token);
        } else {
            let mut token = String::new();
            while let Some(&ch) = chars.peek() {
                if ch.is_whitespace() {
                    break;
                } else {
                    token.push(ch);
                    chars.next();
                }
            }
            let upper_token = token.to_uppercase();
            if upper_token == "AND"
                || upper_token == "OR"
                || upper_token == "("
                || upper_token == ")"
            {
                tokens.push(upper_token);
            } else {
                tokens.push(token);
            }
        }
    }
    tokens
}

fn parse_expression(tokens: &[String]) -> Result<(Expr, usize), String> {
    let mut i = 0;
    parse_or(tokens, &mut i)
}

fn parse_or(tokens: &[String], i: &mut usize) -> Result<(Expr, usize), String> {
    let (mut left, _) = parse_and(tokens, i)?;
    while let Some(token) = tokens.get(*i) {
        if token.to_uppercase() == "OR" {
            *i += 1;
            let (right, _) = parse_and(tokens, i)?;
            left = Expr::Or(Box::new(left), Box::new(right));
        } else {
            break;
        }
    }
    Ok((left, *i))
}

fn parse_and(tokens: &[String], i: &mut usize) -> Result<(Expr, usize), String> {
    let (mut left, _) = parse_term(tokens, i)?;
    while let Some(token) = tokens.get(*i) {
        if token.to_uppercase() == "AND" {
            *i += 1;
            let (right, _) = parse_term(tokens, i)?;
            left = Expr::And(Box::new(left), Box::new(right));
        } else {
            break;
        }
    }
    Ok((left, *i))
}

fn parse_term(tokens: &[String], i: &mut usize) -> Result<(Expr, usize), String> {
    if let Some(token) = tokens.get(*i) {
        if token == "(" {
            *i += 1;
            let (expr, _) = parse_expression(tokens)?;
            if tokens.get(*i) == Some(&")".to_string()) {
                *i += 1;
                Ok((Expr::Group(Box::new(expr)), *i))
            } else {
                Err("Falta el paréntesis de cierre".to_string())
            }
        } else {
            let condition = parse_condition(tokens, i)?;
            Ok((Expr::Condition(condition), *i))
        }
    } else {
        Err("Expresión inesperada al analizar la consulta".to_string())
    }
}

fn parse_condition(tokens: &[String], i: &mut usize) -> Result<Condition, String> {
    let field = tokens.get(*i).ok_or("Falta el campo en la condición")?;
    *i += 1;
    let operator = tokens.get(*i).ok_or("Falta el operador en la condición")?;
    *i += 1;
    let value = tokens.get(*i).ok_or("Falta el valor en la condición")?;
    *i += 1;

    Ok(Condition {
        field: field.clone(),
        operator: operator.clone(),
        value: value.clone(),
    })
}
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
                let tokens = tokenize(rest);
                match parse_expression(&tokens) {
                    Ok((expr, _)) => {
                        // Ejecutamos la consulta con la expresión lógica
                        let results = db.query_expression(&expr).await;
                        Ok(format!("{}\n", serde_json::to_string(&results)?))
                    },
                    Err(err) => Ok(format!("ERR {}\n", err)),
                }
            } else {
                Ok("ERR Usage: QUERY <conditions>\n".to_string())
            }
        }
        "INSERT_OR_UPDATE" => {
            if let Some(rest) = cmd_parts.get(1) {
                let insert_parts: Vec<&str> = rest.trim().splitn(2, ' ').collect();
                if let (Some(key), Some(value_str)) = (insert_parts.get(0), insert_parts.get(1)) {
                    let value: serde_json::Value = serde_json::from_str(value_str)?;
                    db.insert_or_update(key.to_string(), value).await?;
                    Ok("OK\n".to_string())
                } else {
                    // Formato incorrecto de comando INSERT_OR_UPDATE
                    Ok("ERR Usage: INSERT_OR_UPDATE <key> <value>\n".to_string())
                }
            } else {
                // Formato incorrecto de comando INSERT_OR_UPDATE
                Ok("ERR Usage: INSERT_OR_UPDATE <key> <value>\n".to_string())
            }
        }
        "DELETE ALL" => {
            db.delete_all().await?;
            Ok("OK\n".to_string())
        }
        "INSERT_OR_UPDATE_MANY" => {
            if let Some(rest) = cmd_parts.get(1) {
                let items: Vec<(String, Value)> = serde_json::from_str(rest)?;
                db.insert_or_update_many(items).await?;
                Ok("OK\n".to_string())
            } else {
                Ok("ERR Usage: INSERT_OR_UPDATE_MANY <[key, value]...>\n".to_string())
            }
        }
        "DELETE_MANY" => {
            if let Some(rest) = cmd_parts.get(1) {
                let keys: Vec<String> = serde_json::from_str(rest)?;
                db.delete_many(keys).await?;
                Ok("OK\n".to_string())
            } else {
                Ok("ERR Usage: DELETE_MANY <[key1, key2, ...]>\n".to_string())
            }
        }
        "EXIT" => Ok("BYE\n".to_string()),
        _ => Ok("ERR Unknown command\n".to_string()),
    }
}
