use serde::Deserialize;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::error::Error;

#[derive(Debug, Deserialize, Clone)]
pub enum IndexType {
    Numeric,
    String,
}

#[derive(Debug, Deserialize, Clone)]
/// Configuration settings for the HyperionDB.
///
/// # Fields
///
/// * `data_dir` - A string representing the directory where data will be stored.
/// * `num_shards` - An unsigned 32-bit integer specifying the number of shards to use.
/// * `indexed_fields` - A vector of `IndexedField` structs representing the fields that will be indexed.
pub struct Config {
    pub data_dir: String,           
    pub num_shards: u32,            
    pub indexed_fields: Vec<IndexedField>, 
}

#[derive(Debug, Deserialize, Clone)]
/// Represents a field that is indexed within a database.
/// 
/// # Fields
/// 
/// * `field` - The name of the field that is indexed.
/// * `index_type` - The type of index applied to the field.
pub struct IndexedField {
    pub field: String,              
    pub index_type: IndexType,    
}

/// Loads the configuration from a file.
///
/// # Arguments
///
/// * `path` - A path to the configuration file.
///
/// # Returns
///
/// * `Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>>` - On success, returns an instance of `Config`. On failure, returns an error.
///
/// # Errors
///
/// This function will return an error if:
/// - The file cannot be opened.
/// - The file contents cannot be read.
/// - The file contents cannot be parsed as JSON.
impl Config {
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn Error + Send + Sync + 'static>> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let config = serde_json::from_reader(reader)?;
        Ok(config)
    }
}
