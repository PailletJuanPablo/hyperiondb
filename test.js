const HyperionDBClient = require('./hyperiondb-client.js');
const net = require('net'); // Agregar si no está incluido para usar el query directo

// Ejecuta un comando directamente a través de TCP usando Node.js
async function query(command) {
  return new Promise((resolve, reject) => {
    const client = new net.Socket();
    client.connect(8080, '127.0.0.1', () => {
      client.write(`${command}\n`);
    });

    client.on('data', (data) => {
      resolve(data.toString());
      client.destroy(); // Cierra la conexión después de recibir la respuesta
    });

    client.on('error', (err) => {
      reject(`Error en la conexión TCP: ${err.message}`);
    });
  });
}

(async () => {
  try {
    const config = {
      numShards: 8,
      dataDir: './hyperiondb_data',
      indexedFields: [
        ["name", "String"],
        ["age", "Numeric"],
        ["address.city", "String"],
        ["category", "String"],
        ["city", "String"],
        ["sku", "String"],
        ["description", "String"],
        ["address.zipcode", "Numeric"],
        ["product_name", "String"],
        ["price", "Numeric"],
        ["currency", "String"],
        ["specs.processor", "String"],
        ["specs.ram", "String"],
        ["specs.battery", "Numeric"],
        ["in_stock", "Numeric"],
        ["created_at", "String"],
        ["warehouses.warehouse1", "Numeric"],
        ["warehouses.warehouse2", "Numeric"]
      ],
      address: "127.0.0.1:8080" // Dirección y puerto del servidor HyperionDB
    };
    
    const client = new HyperionDBClient(config);
    await client.initialize();

    console.log("Base de datos inicializada correctamente.");

    // Define un nuevo registro para insertar
    const record = {
      category: 'Electronics',
      city: 'West Jakob Greenfelder town',
      created_at: '1949-05-19T04:28:10.754099741+00:00',
      currency: 'LKR',
      description: 'harum ratione harum alias dolorem.',
      id: 'prod1748',
      in_stock: false,
      name: 'Lexie Luettgen',
      price: 355.0025481047212,
      sku: 'SKU246021',
      specs: {
        battery: '4351 mAh',
        processor: 'Dual-core',
        ram: '16GB',
      },
      stock: 192,
      warehouses: {
        warehouse1: 34,
        warehouse2: 51,
      },
    };

    // Inserta el registro con writeRecord (primero verifica si existe y luego inserta o actualiza)
    const writeResponse = await client.writeRecord(record);
    console.log('Respuesta de escritura (inserción):', writeResponse);

    // Consulta el registro insertado para verificar la inserción
    const queriedRecord = await client.get('prod1748');
    console.log('Registro consultado después de inserción:', queriedRecord);

    // Actualiza el registro existente utilizando writeRecord
    const updatedData = {
      id: 'prod1748',  // Asegura que use la misma ID para actualización
      in_stock: true,
      price: 400
    };
    const updateResponse = await client.writeRecord(updatedData);
    console.log('Respuesta de escritura (actualización):', updateResponse);

    // Consulta el registro nuevamente para verificar los cambios
    const updatedRecord = await client.get('prod1748');
    console.log('Registro consultado después de actualización:', updatedRecord);

  } catch (error) {
    console.error("Error durante la prueba:", error);
  }
})();
