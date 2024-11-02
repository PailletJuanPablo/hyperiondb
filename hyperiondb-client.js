const { HyperionDbWrapper } = require('./index.js');
const net = require('net');
const fs = require('fs');
/**
 * Class representing a client for HyperionDB.
 */
class HyperionDBClient {
    /**
 * 🔧 **HyperionDBClient Constructor**
 * 
 * Instantiates a new `HyperionDBClient` instance to interact with the HyperionDB database.
 * This class provides an interface to perform CRUD operations on HyperionDB, including querying, 
 * inserting, updating, and deleting records. Upon initialization, it sets up database connection 
 * parameters such as sharding, indexing, and server configuration.
 * 
 * **Primary Use**: 🛠 The client is used to initialize and configure the connection with HyperionDB, 
 * defining database structure through `numShards`, `dataDir`, `indexedFields`, and `address`.
 * 
 * **Configuration Requirements**: 📋 The configuration object passed to this constructor must
 * include all the following fields:
 * 
 * @param {Object} config - ⚙️ Configuration options for initializing the database connection.
 * @param {number} config.numShards - 💾 **Number of Shards**: Defines how many shards the database should use. 
 * Higher values allow parallel processing, but may increase complexity. Recommended to balance load and performance.
 * @param {string} config.dataDir - 🗂 **Data Directory**: Specifies the directory path where database shards are stored.
 * Ensure this path has sufficient storage and access permissions.
 * @param {Array.<Array.<string>>} config.indexedFields - 📑 **Indexed Fields**: Defines an array of fields to be indexed 
 * for faster lookup. Each entry should be an array containing two strings: `[fieldName, indexType]`.
 *    - **fieldName**: The name of the field to be indexed (e.g., `"name"`, `"price"`).
 *    - **indexType**: The type of index, either `"String"` or `"Numeric"`.
 *    - Example: `indexedFields: [["name", "String"], ["age", "Numeric"]]`
 * @param {string} config.address - 🌐 **Server Address**: The network address and port of the HyperionDB server, 
 * formatted as `"127.0.0.1:8080"`. This address must be reachable from the environment where the client is running.
 * @param {string} primaryKey - 🗝 **Primary Key Field**: Defines the primary key field to uniquely identify each record 
 * (e.g., `"id"`). This key will be used in insert and update operations unless overridden.
 *
 * **Error Handling**: ❗ Throws an error if any required configuration field is missing or invalid.
 *
 * @example
 * // Example of creating a new HyperionDBClient instance
 * const config = {
 *   numShards: 8,
 *   dataDir: './hyperiondb_data',
 *   indexedFields: [
 *     ["name", "String"],
 *     ["price", "Numeric"],
 *     ["city", "String"]
 *   ],
 *   address: '127.0.0.1:8080'
 * };
 * const primaryKey = 'id';
 * 
 * const client = new HyperionDBClient(config, primaryKey);
 * console.log('HyperionDB client initialized:', client);
 */
    constructor(config, primaryKey) {
        // Initialize HyperionDbWrapper instance to manage database operations
        this.db = new HyperionDbWrapper();

        // Store configuration settings
        this.config = config;

        // If the dataDir not exist, create it
        if (!fs.existsSync(config.dataDir)) {
            fs.mkdirSync(config.dataDir);
        }


        // Primary key for unique identification of records
        this.primaryKey = primaryKey;
    }


    /**
     * Initializes the database with the provided configuration.
     * @returns {Promise<void>}
     */
    async initialize() {
        await this.db.initialize(
            this.config.numShards,
            this.config.dataDir,
            this.config.indexedFields,
            this.config.address
        );
        await this.startServer();
    }

    /**
     * Starts the HyperionDB server.
     * @returns {Promise<void>}
     */
    async startServer() {
        const [_, portStr] = this.config.address.split(':');
        const port = parseInt(portStr, 10);
        await this.db.startServer(port);
    }

    /**
     * Sends a command to the HyperionDB server and returns the response.
     * @private
     * @param {string} command - The command to send.
     * @returns {Promise<string>} - The response from the server.
     */
    /**
 * Sends a command to the HyperionDB server and returns the full response, handling chunked data.
 * @private
 * @param {string} command - The command to send.
 * @returns {Promise<string>} - The complete response from the server.
 */
/**
 * Sends a command to the HyperionDB server and waits for the response to complete.
 * The method receives data in chunks and stops when a newline character `\n` is detected.
 * @private
 * @param {string} command - The command to send.
 * @returns {Promise<string>} - The complete response from the server.
 */
_sendCommand(command) {
    return new Promise((resolve, reject) => {
        const [host, portStr] = this.config.address.split(':');
        const port = parseInt(portStr, 10);
        const client = new net.Socket();

        let response = '';

        client.connect(port, host, () => {
            client.write(`${command}\n`);
        });

        client.on('data', (chunk) => {
            response += chunk.toString();

            // Check if the response includes the newline character
            if (response.includes('\n')) {
                client.end(); // Close the connection once the full response is received
            }
        });

        client.on('end', () => {
            resolve(response.trim()); // Trim any extra whitespace or newline
        });

        client.on('error', (err) => {
            reject(new Error(`Error in TCP connection: ${err.message}`));
        });
    });
}



    /**
  * 🚀 **Write (Insert or Update) a Record in HyperionDB**
  * 
  * This method allows inserting a new record or updating an existing record.
  * If a record with the same key already exists, it merges the existing data with 
  * the new data, ensuring only new or updated fields are changed.
  * 
  * @async
  * @param {Object} record - 📝 The record to write, as a JavaScript object. Should contain at least one unique identifier field (`id` or the primary key defined in the config).
  * @param {string} [key] - Optional. The unique key to use for this record. If not provided, defaults to `record.id` or primary key in config.
  * 
  * @throws {Error} If no key is provided, either as `record.id` or `key` parameter.
  * 
  * @returns {Promise<string>} - ✅ A message confirming the write operation or an error if the operation fails.
  * 
  * @example
  * // Example usage:
  * const record = {
  *   id: 'prod1748',
  *   name: 'Lexie Luettgen',
  *   price: 355.00,
  *   in_stock: true
  * };
  * 
  * const response = await client.writeRecord(record);
  * console.log(response); // ✅ 'OK' (successful write)
  * 
  * // Update example with new fields
  * const updatedRecord = {
  *   id: 'prod1748',
  *   price: 360.00,
  *   category: 'Updated Electronics'
  * };
  * const response = await client.writeRecord(updatedRecord);
  * console.log(response); // ✅ 'OK' (successful update with merged fields)
  */
    async writeRecord(record, key = null) {
        // Use the specified key if provided; otherwise, try to use `id` from the record or primary key in config
        const recordKey = key || record[this.primaryKey] || record.id;

        if (!recordKey) {
            throw new Error('A key is required: specify a key parameter or ensure the record has an "id" or primary key field.');
        }

        // Attempt to fetch the existing record to merge with new data if it exists
        let existingRecord = {};
        try {
            existingRecord = await this.get(recordKey);
        } catch (error) {
            // If the record does not exist, it will proceed with the new record only
        }

        // Merge existing data with new data (new data overwrites where conflicts exist)
        const mergedRecord = { ...existingRecord, ...record };

        // Convert merged record to JSON and format the INSERT command
        const recordJson = JSON.stringify(mergedRecord).replace(/[\n\r]/g, '');
        const command = `INSERT ${recordKey} ${recordJson}`;

        // Execute the command and return the response
        const response = await this._sendCommand(command);
        return response.trim() === 'OK' ? 'Record written successfully!' : response;
    }



    /**
     * Queries the database.
     * @param {string} queryStr - The query string (e.g., 'name CONTAINS "John" AND age > 30').
     * @returns {Promise<string>} - The response from the server.
     */
    async query(queryStr) {
        const command = `QUERY ${queryStr}`;
        const response = await this._sendCommand(command);
        return JSON.parse(response);
    }

    /**
  * 🚨 **Delete Records from HyperionDB**
  *
  * Deletes records from the database based on a specified condition.
  * Use this method to remove entries that meet particular criteria.
  *
  * **Error Handling**: ❗ Ensure the condition syntax is correct; otherwise, the database might return an error.
  *
  * @async
  * @param {string} condition - 📝 The condition for deletion (e.g., `'age < 18'` or `'city = "New York"'`).
  * @returns {Promise<boolean>} - ✅ Returns `true` if deletion was successful, `false` if it failed.
  * 
  * @example
  * // Delete all records where age is less than 18
  * const wasDeleted = await client.delete('age < 18');
  * console.log(wasDeleted); // true (if deletion succeeded)
  */
    async delete(condition) {
        const command = `DELETE ${condition}`;
        const response = await this._sendCommand(command);
        return response.trim() === 'OK';
    }

    /**
     * 📋 **List All Records in HyperionDB**
     *
     * Retrieves a list of all records currently stored in the database. This method
     * is useful for viewing the full contents of the database.
     *
     * **Output Format**: Returns a JSON array of records.
     *
     * @async
     * @returns {Promise<Array>} - ✅ An array of records, each represented as an object.
     * 
     * @example
     * // List all records in the database
     * const allRecords = await client.list();
     * console.log(allRecords); // [{...}, {...}, ...]
     */
    async list() {
        const command = `LIST`;
        const response = await this._sendCommand(command);
        return response;
    }

    /**
     * 🔍 **Retrieve a Record by ID from HyperionDB**
     *
     * Fetches a single record from the database using a unique identifier.
     * Use this method when you need to retrieve a specific entry by ID.
     *
     * **Error Handling**: ❗ An error is thrown if the ID does not exist.
     *
     * @async
     * @param {string} id - 🆔 The unique identifier for the record.
     * @returns {Promise<Object>} - ✅ An object representing the record, if found.
     * 
     * @example
     * // Get a record by its ID
     * const record = await client.get('prod1748');
     * console.log(record); // { id: 'prod1748', name: 'Sample Product', ... }
     */
    async get(id) {
        const command = `GET ${id}`;
        const response = await this._sendCommand(command);
        return JSON.parse(response);
    }


    /**
     * 🔎 **Query the Database with Complex Conditions**
     *
     * Executes a query on the database using a specific query string, allowing you to filter
     * records based on conditions. Supports logical operators (AND, OR) and comparison operators.
     *
     * **Supported Query Syntax**: Use `AND`, `OR`, `<`, `>`, `=` for filtering.
     *
     * **Output Format**: Returns an array of matching records.
     *
     * @async
     * @param {string} queryStr - 📝 The query string (e.g., `'name CONTAINS "John" AND age > 30'`).
     * @returns {Promise<Array>} - ✅ An array of matching records.
     * 
     * @example
     * // Query for all records where the name contains "John" and age is greater than 30
     * const results = await client.query('name CONTAINS "John" AND age > 30');
     * console.log(results); // [{...}, {...}, ...] (matching records)
     */
    async query(queryStr) {
        const command = `QUERY ${queryStr}`;
        const response = await this._sendCommand(command);
        return JSON.parse(response);
    }

    /**
 * 🔄 **Insert or Update Multiple Records in HyperionDB**
 *
 * This method allows inserting multiple records or updating them if they already exist.
 * The method accepts an array of items, which are converted to JSON format and sent as a
 * single batch command to the HyperionDB server.
 *
 * **Error Handling**: ❗ Ensure that the items parameter is a valid array of objects.
 *
 * @async
 * @param {Array<Object>} items - 📝 Array of records to insert or update. Each item should 
 * be a JavaScript object with fields to insert or update.
 * @returns {Promise<string>} - ✅ Confirmation message for successful batch insert/update or an error message.
 * 
 * @example
 * // Insert or update multiple records
 * const items = [
 *   { id: 'prod1', name: 'Product One', price: 100 },
 *   { id: 'prod2', name: 'Product Two', price: 200 }
 * ];
 * const response = await client.insertOrUpdateMany(items);
 * console.log(response); // 'Records inserted or updated successfully!' or error message
 */
    async insertOrUpdateMany(items) {
        if (!Array.isArray(items) || items.length === 0) {
            throw new Error('The items parameter must be a non-empty array.');
        }
    
        // Convert each item to a tuple using the primary key or id field
        const itemsTuples = items.map(item => {
            const key = item[this.primaryKey] || item.id;
            if (!key) {
                throw new Error(`Each item must have a unique key field "${this.primaryKey}" or "id".`);
            }
            const data = { ...item }; // Clone item to avoid mutating the original
            delete data[this.primaryKey]; // Remove the key field from the data object
            delete data.id; // Ensure 'id' is not included if it's the primary key
            return [key, data];
        });
    
        // Convert the array of tuples to JSON
        const itemsJson = JSON.stringify(itemsTuples);
    
        // Construct the INSERT_OR_UPDATE_MANY command with the JSON payload
        const command = `INSERT_OR_UPDATE_MANY ${itemsJson}`;
    
        // Send the command and await the response
        const response = await this._sendCommand(command);
    
        return response.trim() === 'OK' ? 'Records inserted or updated successfully!' : response;
    }
    /**
 * 🚨 **Delete Multiple Records from HyperionDB by Keys**
 *
 * This method allows deleting multiple records by accepting an array of keys.
 * The keys are converted to JSON format and sent as a single batch command
 * to the HyperionDB server.
 *
 * **Error Handling**: ❗ Ensure that the keys parameter is a valid array with at least one key.
 *
 * @async
 * @param {Array<string>} keys - 🔑 Array of keys for records to delete.
 * @returns {Promise<string>} - ✅ Confirmation message for successful batch deletion or an error message.
 * 
 * @example
 * // Delete multiple records by keys
 * const keys = ['prod1', 'prod2', 'prod3'];
 * const response = await client.deleteMany(keys);
 * console.log(response); // 'Records deleted successfully!' or error message
 */
    async deleteMany(keys) {
        if (!Array.isArray(keys) || keys.length === 0) {
            throw new Error('The keys parameter must be a non-empty array.');
        }

        // Convert the array of keys to JSON
        const keysJson = JSON.stringify(keys);

        // Construct the DELETE_MANY command with the JSON payload
        const command = `DELETE_MANY ${keysJson}`;

        // Send the command and await the response
        const response = await this._sendCommand(command);

        return response;
    }





}

module.exports = HyperionDBClient;
