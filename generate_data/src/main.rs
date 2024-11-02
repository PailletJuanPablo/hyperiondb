use fake::faker::name::raw::*;
use fake::faker::address::raw::*;
use fake::faker::chrono::raw::DateTime;
use fake::faker::lorem::raw::*;
use fake::faker::number::raw::NumberWithFormat;
use fake::faker::boolean::raw::Boolean;
use fake::faker::currency::raw::CurrencyCode;
use fake::{Fake, locales::EN};
use rand::Rng;
use serde_json::json;
use serde_json::Value;
use std::error::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::time::Instant;

/// Configuración para la prueba de rendimiento
struct PerfConfig {
    server_addr: String,
    num_records: usize,
    batch_size: usize,
}

/// Genera un producto con datos de ejemplo, utilizando `fake` para generar campos específicos.
fn generate_product(id: usize) -> Value {
    let mut rng = rand::thread_rng();

    let categories = vec!["Electronics", "Clothing", "Books", "Home", "Toys"];
    let processors = vec!["Quad-core", "Octa-core", "Dual-core"];
    let ram_options = vec!["8GB", "16GB", "32GB"];
    let name: String = Name(EN).fake();
    let description: String = Sentence(EN, 5..10).fake();
    let price: f64 = rng.gen_range(10.0..500.0);
    let category = categories[rng.gen_range(0..categories.len())].to_string();
    let stock: u32 = rng.gen_range(0..200);
    let sku: String = NumberWithFormat(EN, "SKU######").fake(); // SKU con formato específico

    let specs = json!({
        "processor": processors[rng.gen_range(0..processors.len())],
        "ram": ram_options[rng.gen_range(0..ram_options.len())],
        "battery": format!("{} mAh", rng.gen_range(2000..5000)),
    });

    let warehouses = json!({
        "warehouse1": rng.gen_range(0..100),
        "warehouse2": rng.gen_range(0..100),
    });

    json!({
        "id": format!("prod{}", id),
        "name": name,
        "description": description,
        "price": price,
        "currency": CurrencyCode(EN).fake::<String>(),
        "category": category,
        "sku": sku,
        "stock": stock,
        "city": CityName(EN).fake::<String>(),
        "specs": specs,
        "warehouses": warehouses,
        "in_stock": Boolean(EN, 50).fake::<bool>(),
        "created_at": DateTime(EN).fake::<String>()
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
        batch_size: 1000,    // Cantidad de registros por lote
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
        let mut batch_data = Vec::new();

        for id in batch_start..=usize::min(batch_start + config.batch_size - 1, config.num_records) {
            let product = generate_product(id);
            batch_data.push((format!("prod{}", id), product));
        }

        // Crear el comando INSERT_MANY con el lote de productos en formato JSON
        let insert_many_command = format!("INSERT_OR_UPDATE_MANY {}\n", serde_json::to_string(&batch_data)?);
        send_command(&mut writer, &insert_many_command).await?;

        // Leer la respuesta del servidor
        let response = read_response(&mut reader).await?;
        if response != "OK" {
            eprintln!("Error al insertar el lote: {}", response);
        }

        println!(
            "Lote de registros insertados desde {} hasta {}...",
            batch_start,
            usize::min(batch_start + config.batch_size - 1, config.num_records)
        );
    }

    let duration_insert = start_insert.elapsed();
    println!(
        "Insertados {} registros en {:.2?}",
        config.num_records, duration_insert
    );

    // Medir el tiempo del comando QUERY sobre campo anidado
    let query_field = "address.city";
    let query_operator = "=";
    let query_value = "San Antonio";
    let start_query = Instant::now();
    let query_command = format!("QUERY {} {} {}\n", query_field, query_operator, query_value);
    send_command(&mut writer, &query_command).await?;
    let query_response = read_response(&mut reader).await?;
    let query_duration = start_query.elapsed();
    println!(
        "QUERY '{}' '{}' '{}' completed in {:.2?}",
        query_field, query_operator, query_value, query_duration
    );

    // Parsear la respuesta de QUERY
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
