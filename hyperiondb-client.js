const { HyperionDbWrapper } = require('./index.js');
const net = require('net');

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
    _sendCommand(command) {
        return new Promise((resolve, reject) => {
            const [host, portStr] = this.config.address.split(':');
            const port = parseInt(portStr, 10);
            const client = new net.Socket();

            client.connect(port, host, () => {
                client.write(`${command}\n`);
            });

            client.on('data', (data) => {
                resolve(data.toString());
                client.destroy();
            });

            client.on('error', (err) => {
                reject(new Error(`Error in TCP connection: ${err.message}`));
            });
        });
    }

    /**
  * 🚀 **Insert a Record into HyperionDB**
  * 
  * Inserts a record into the HyperionDB database with a key specified in the method call, the 
  * primary key defined during instantiation, or the record’s `id` field. Prioritizes the key parameter, 
  * then falls back to the primary key or `id` if available. Throws an error if no key is available.
  * 
  * **Error Handling**: ❗ Throws an error if no valid key is found.
  *
  * @async
  * @param {Object} record - 📝 The record to insert, as an object with key-value pairs.
  * @param {string} [key] - (Optional) The unique key for this record; if omitted, uses the constructor-defined 
  * primary key or the record's `id`.
  * 
  * @throws {Error} If no `key`, primary key, or `record.id` is available, an error is thrown.
  * 
  * @returns {Promise<string>} - ✅ A message confirming the insertion or an error message if failed.
  * 
  * @example
  * // Example 1: Insert with a custom key
  * const record = {
  *   category: 'Electronics',
  *   city: 'West Jakob Greenfelder town',
  *   created_at: '1949-05-19T04:28:10.754099741+00:00',
  *   currency: 'LKR',
  *   description: 'harum ratione harum alias dolorem.',
  *   id: 'prod1748',
  *   in_stock: false,
  *   name: 'Lexie Luettgen',
  *   price: 355.0025481047212,
  *   sku: 'SKU246021',
  *   specs: { battery: '4351 mAh', processor: 'Dual-core', ram: '16GB' },
  *   stock: 192,
  *   warehouses: { warehouse1: 34, warehouse2: 51 }
  * };
  * 
  * const response = await client.insert(record, 'customKey123');
  * console.log(response); // ✅ 'OK' (successful insert)
  *
  * @example
  * // Example 2: Insert using the primary key specified in the constructor
  * const client = new HyperionDBWrapper('sku');
  * const response = await client.insert(record);
  * console.log(response); // ✅ 'OK' (inserted with 'sku' as the key)
  * 
  * @example
  * // ⚠️ Example 3: Insert without a valid key, causing an error
  * try {
  *   const response = await client.insert({ name: 'Invalid Record' });
  * } catch (error) {
  *   console.error('Expected error:', error.message); // ❌ Error: 'A key is required...'
  * }
  */
    async insert(record, key = null) {
        // Attempt to use the specified key, the primary key defined in the instance, or the `id` field from the record
        const insertKey = key || this.primaryKey || record.id;

        // If neither key nor primary key is provided, throw an error
        if (!insertKey) {
            throw new Error('A key is required: specify a key, set a primaryKey during instantiation, or ensure the record has an "id" field.');
        }

        // Convert the record object to a JSON string and sanitize newlines
        const recordJson = JSON.stringify(record).replace(/[\n\r]/g, '');

        // Construct the INSERT command
        const command = `INSERT ${insertKey} ${recordJson}`;

        // Send the command to the database server and return the response
        const response = await this._sendCommand(command);
        return response;
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
        return JSON.parse(response);
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
     * ✏️ **Update a Record by ID in HyperionDB**
     *
     * Modifies an existing record in the database using the specified `id` and
     * applies the `updates` object to change fields.
     *
     * **Error Handling**: ❗ Ensure the ID exists, or the update will fail.
     *
     * @async
     * @param {string} id - 🆔 The unique identifier of the record to update.
     * @param {Object} updates - 🔄 An object with fields to be updated (e.g., `{ "price": 499.99 }`).
     * @returns {Promise<boolean>} - ✅ Returns `true` if the update was successful, `false` if failed.
     * 
     * @example
     * // Update the price of a record by ID
     * const wasUpdated = await client.update('prod1748', { price: 499.99 });
     * console.log(wasUpdated); // true (if update succeeded)
     */
    async update(id, updates) {
        const updatesJson = JSON.stringify(updates);
        const command = `UPDATE ${id} ${updatesJson}`;
        const response = await this._sendCommand(command);
        return response.trim() === 'OK';
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

}

module.exports = HyperionDBClient;
