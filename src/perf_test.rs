// src/perf_test.rs

use std::error::Error;
use tokio::net::TcpStream;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use serde_json::json;
use rand::Rng;
use tokio::time::{Instant};

/// Configuración para la prueba de rendimiento
struct PerfConfig {
    server_addr: String,
    num_records: usize,
    batch_size: usize,
}

/// Genera un registro de usuario aleatorio
fn generate_random_user(id: usize) -> serde_json::Value {
    let mut rng = rand::thread_rng();
    let age: u8 = rng.gen_range(18..65);
    let cities = vec![
        "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
        "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose",
    ];
    let city = cities[rng.gen_range(0..cities.len())];
    json!({
        "name": format!("User{}", id),
        "age": age,
        "city": city
    })
}

/// Envía un comando al servidor de HyperionDB
async fn send_command(
    writer: &mut BufWriter<tokio::net::tcp::OwnedWriteHalf>,
    command: &str,
) -> Result<(), Box<dyn Error>> {
    writer.write_all(command.as_bytes()).await?;
    writer.flush().await?;
    Ok(())
}

/// Lee una respuesta del servidor de HyperionDB
async fn read_response(
    reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
) -> Result<String, Box<dyn Error>> {
    let mut response = String::new();
    reader.read_line(&mut response).await?;
    Ok(response.trim().to_string())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Configuración
    let config = PerfConfig {
        server_addr: "127.0.0.1:8080".to_string(),
        num_records: 10000, // Cantidad de registros a insertar
        batch_size: 500,    // Cantidad de registros por lote
    };

    // Conectar al servidor de HyperionDB
    let stream = TcpStream::connect(&config.server_addr).await?;
    let (read_half, write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut writer = BufWriter::new(write_half);

    println!("Conectado al servidor HyperionDB en {}", config.server_addr);

    // Medir el tiempo de inserción
    let start_insert = Instant::now();

    for batch_start in (1..=config.num_records).step_by(config.batch_size) {
        let mut batch_commands = Vec::new();

        for id in batch_start..=usize::min(batch_start + config.batch_size - 1, config.num_records) {
            let user = generate_random_user(id);
            let command = format!("INSERT user{} {}\n", id, user.to_string());
            batch_commands.push(command);
        }

        // Enviar todos los comandos de inserción en el lote
        for command in &batch_commands {
            send_command(&mut writer, command).await?;
        }

        // Leer todas las respuestas
        for _ in &batch_commands {
            let response = read_response(&mut reader).await?;
            if response != "OK" {
                eprintln!("Error al insertar el registro: {}", response);
            }
        }

        println!(
            "Registros insertados desde {} hasta {}...",
            batch_start,
            usize::min(batch_start + config.batch_size - 1, config.num_records)
        );
    }

    let duration_insert = start_insert.elapsed();
    println!(
        "Insertados {} registros en {:.2?}",
        config.num_records, duration_insert
    );

    // Medir el tiempo del comando LIST
    let start_list = Instant::now();
    send_command(&mut writer, "LIST\n").await?;
    let response = read_response(&mut reader).await?;
    let list_duration = start_list.elapsed();
    println!("LIST completado en {:.2?}", list_duration);

    // Parsear la respuesta LIST
    let list: Vec<serde_json::Value> = serde_json::from_str(&response)?;
    println!("Total de registros listados: {}", list.len());

    // Medir el tiempo del comando QUERY
    let query_field = "age";
    let query_value = "30";
    let start_query = Instant::now();
    let query_command = format!("QUERY {} {}\n", query_field, query_value);
    send_command(&mut writer, &query_command).await?;
    let query_response = read_response(&mut reader).await?;
    let query_duration = start_query.elapsed();
    let query_field = "age";
    let query_operator = ">";
    let query_value = "30";
    let start_query = Instant::now();
    let query_command = format!("QUERY {} {} {}\n", query_field, query_operator, query_value);
    send_command(&mut writer, &query_command).await?;
    let query_response = read_response(&mut reader).await?;
    let query_duration = start_query.elapsed();
    println!(
        "QUERY '{}' '{}' '{}' completed in {:.2?}",
        query_field, query_operator, query_value, query_duration
    );

    // Parse the QUERY response
    let query_result: Vec<serde_json::Value> = serde_json::from_str(&query_response)?;
    println!(
        "Total records matching '{} {} {}': {}",
        query_field,
        query_operator,
        query_value,
        query_result.len()
    );

    // Test QUERY with 'city CONTAINS "San"'
    let query_field = "city";
    let query_operator = "CONTAINS";
    let query_value = "San";
    let start_query = Instant::now();
    let query_command = format!("QUERY {} {} {}\n", query_field, query_operator, query_value);
    send_command(&mut writer, &query_command).await?;
    let query_response = read_response(&mut reader).await?;
    let query_duration = start_query.elapsed();
    println!(
        "QUERY '{}' '{}' '{}' completed in {:.2?}",
        query_field, query_operator, query_value, query_duration
    );

    // Parse the QUERY response
    let query_result: Vec<serde_json::Value> = serde_json::from_str(&query_response)?;
    println!(
        "Total records matching '{} {} {}': {}",
        query_field,
        query_operator,
        query_value,
        query_result.len()
    );

    Ok(())
}
