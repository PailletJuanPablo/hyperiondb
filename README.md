# HyperionDB

[![npm version](https://img.shields.io/npm/v/hyperiondb.svg)](https://www.npmjs.com/package/hyperiondb)
[![license](https://img.shields.io/npm/l/hyperiondb.svg)](https://github.com/yourusername/hyperiondb/blob/main/LICENSE)
[![downloads](https://img.shields.io/npm/dt/hyperiondb.svg)](https://www.npmjs.com/package/hyperiondb)

![HyperionDB Logo](./assets/top.png)

A minimalist Rust-based sharded database client for Node.js. HyperionDB offers high-performance data storage and retrieval with sharding support, making it suitable for scalable applications.

---

## 🚀 Features

- **High Performance**: Built with Rust for speed and efficiency.
- **Sharding Support**: Distribute data across multiple shards for scalability.
- **Easy Integration**: Simple API for Node.js applications.
- **Custom Indexing**: Define indexed fields for faster queries.
- **Cross-Platform**: Works on Windows, macOS, and Linux.

---

## 📦 Installation

Install HyperionDB via npm:

```bash
npm install hyperiondb
```


## 🛠️ Setup and Usage

### **1. Import HyperionDBClient**

```javascript
const HyperionDBClient = require('hyperiondb/hyperiondb-client');
``` 

### **2. Configuration**

Create a configuration object specifying the number of shards, data directory, indexed fields, server address, and primary key field.

```javascript
const config = {
  numShards: 8,
  dataDir: './hyperiondb_data',
  indexedFields: [
    ["name", "String"],
    ["price", "Numeric"],
    ["city", "String"],
    // Add other indexed fields as needed
  ],
  address: '127.0.0.1:8080'
};
const primaryKey = 'id';` 
```
### **3. Initialize the Client**

Instantiate the HyperionDBClient with the configuration and primary key.

```javascript
const client = new HyperionDBClient(config, primaryKey);` 
```
### **4. Initialize the Database and Start the Server**


```javascript
(async () => {
  try {
    // Initialize the database
    await client.initialize();
    console.log('Database initialized.');

    // Start the server
    await client.startServer();
    console.log('Server started.');
  } catch (error) {
    console.error('Initialization error:', error);
  }
})();
```
----------

## 🔄 CRUD Operations

### **Insert a Record**

```javascript
const record = {
  id: 'prod1748',
  name: 'Sample Product',
  price: 355.00,
  // other fields...
};

(async () => {
  const response = await client.insert(record);
  console.log('Insert response:', response); // Output: 'OK'
})();
```
### **Retrieve a Record**

```javascript
(async () => {
  const record = await client.get('prod1748');
  console.log('Retrieved record:', record);
})();
```

### **Update a Record**

```javascript
(async () => {
  const updates = { price: 399.99 };
  const success = await client.update('prod1748', updates);
  console.log('Update successful:', success); // Output: true
})();
```

### **Delete a Record**

```javascript
(async () => {
  const success = await client.delete('id = "prod1748"');
  console.log('Delete successful:', success); // Output: true
})();
``` 

----------

## 🔍 Querying the Database

```javascript
(async () => {
  const results = await client.query('price > 300 AND city = "New York"');
  console.log('Query results:', results);
})();
``` 

----------

## 📄 API Reference

### **Constructor**

#### `new HyperionDBClient(config, primaryKey)`

-   **config**: An object containing configuration settings.
    -   **numShards**: Number of shards.
    -   **dataDir**: Directory where shards are stored.
    -   **indexedFields**: Array of indexed fields, each as `[fieldName, indexType]`.
    -   **address**: Address and port of the HyperionDB server (e.g., `'127.0.0.1:8080'`).
-   **primaryKey**: The primary key field for records (e.g., `'id'`).

### **Methods**

#### `initialize()`

Initializes the database with the provided configuration.

#### `startServer()`

Starts the HyperionDB server on the specified address and port.

#### `insert(record, key)`

Inserts a record into the database.

-   **record**: The record object to insert.
-   **key** (optional): Custom key for the record. If not provided, uses `primaryKey` or `record.id`.

#### `get(id)`

Retrieves a record by its ID.

-   **id**: The ID of the record to retrieve.

#### `update(id, updates)`

Updates a record with the specified ID.

-   **id**: The ID of the record to update.
-   **updates**: An object containing the fields to update.

#### `delete(condition)`

Deletes records matching the specified condition.

-   **condition**: A string representing the deletion condition (e.g., `'price < 100'`).

#### `list()`

Lists all records in the database.

#### `query(queryStr)`

Queries the database with complex conditions.

-   **queryStr**: The query string (e.g., `'name CONTAINS "John" AND age > 30'`).

----------

## 📝 Examples

### **Example: Insert and Query**

```javascript
(async () => {
  // Insert multiple records
  await client.insert({ id: '1', name: 'Alice', age: 30 });
  await client.insert({ id: '2', name: 'Bob', age: 25 });
  await client.insert({ id: '3', name: 'Charlie', age: 35 });

  // Query records where age is greater than 28
  const results = await client.query('age > 28');
  console.log('Query results:', results);
  // Output: [{ id: '1', name: 'Alice', age: 30 }, { id: '3', name: 'Charlie', age: 35 }]
})();
```
----------

## ⚙️ Configuration Options

-   **Sharding**: Adjust `numShards` based on your performance and scaling needs.
-   **Indexed Fields**: Index fields that are frequently used in queries to improve performance.
-   **Data Directory**: Ensure the `dataDir` has sufficient permissions and storage space.

----------

## 📌 Notes

-   **Server Readiness**: After starting the server, ensure it's ready before performing operations.
-   **Error Handling**: Wrap your operations in `try...catch` blocks to handle exceptions.
-   **Data Types**: Ensure data types of fields match those specified in `indexedFields`.

----------

## 🛠️ Development

### **Building from Source**

Clone the repository:

```bash
git clone https://github.com/yourusername/hyperiondb.git
```
Install dependencies:


```bash
cd hyperiondb
npm install
```

Build the module:

```bash
npm run build
```

----------

## 🖥️ Compatibility

-   **Node.js**: Requires Node.js version >= 10.
-   **Platforms**: Windows, macOS, Linux (including ARM architectures).

----------

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](https://github.com/yourusername/hyperiondb/blob/main/LICENSE) file for details.

----------

## 🙏 Acknowledgements

-   Built with ❤️ using Rust and Node.js.
-   Thanks to the open-source community for their contributions.

----------

## 📫 Contact

For questions or support, please open an issue on [GitHub](https://github.com/yourusername/hyperiondb/issues) or contact me at hi@pailletjp.com

----------

## 🌟 Contributions

Contributions are welcome! Please read the [contributing guidelines](https://github.com/yourusername/hyperiondb/blob/main/CONTRIBUTING.md) before submitting a pull request.

----------

## 🛡️ Security

If you discover any security-related issues, please email your.email@example.com instead of using the issue tracker.

----------

## 🗺️ Roadmap

-   Implement authentication and authorization.
-   Add support for more data types.
-   Improve documentation and examples.
-   Integrate with popular frameworks.

----------

## 💡 Tips

-   **Performance**: For better performance, adjust the number of shards based on your system's capabilities.
-   **Data Backup**: Regularly backup your `dataDir` to prevent data loss.
-   **Logging**: Implement logging mechanisms for debugging and monitoring.

----------

## ❓ FAQ

**Q:** _Can I use HyperionDB in a production environment?_

**A:** While HyperionDB is designed for high performance, please thoroughly test it in your environment before deploying to production.

**Q:** _Does HyperionDB support transactions?_

**A:** Currently, HyperionDB does not support transactions, but this feature is planned for future releases.

----------

_Happy coding!_