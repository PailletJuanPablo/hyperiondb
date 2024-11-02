import socket
import time
import json
import matplotlib.pyplot as plt

class HyperionDBClient:
    def __init__(self, host="127.0.0.1", port=8080):
        self.host = host
        self.port = port

    def send_command(self, command):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.host, self.port))
            s.sendall(command.encode() + b'\n')

            response = b""
            while True:
                chunk = s.recv(4096)
                if not chunk:
                    break
                response += chunk
                if b'\n' in chunk:
                    break

        return response.decode().strip()

    def insert_or_update_many(self, items):
        items_json = json.dumps(items)
        command = f"INSERT_OR_UPDATE_MANY {items_json}"
        return self.send_command(command)

    def delete_many(self, keys):
        keys_json = json.dumps(keys)
        command = f"DELETE_MANY {keys_json}"
        return self.send_command(command)

    def list(self):
        command = "LIST"
        return self.send_command(command)


# Función para realizar benchmarks en batch
def perform_operation_benchmark(client, operation, data, batch_size):
    times = []
    for _ in range(5):  # Ejecutamos el benchmark 5 veces
        start_time = time.time()

        if operation == "insert":
            items = [{"_id": item["_id"], **item} for item in data[:batch_size]]
            client.insert_or_update_many(items)

        elif operation == "query":
            client.list()

        elif operation == "update":
            items = [{"_id": item["_id"], **item, "content": "Updated Content"} for item in data[:batch_size]]
            client.insert_or_update_many(items)
            
        

     

        elapsed_time = (time.time() - start_time) * 1000  # Convert to milliseconds
        times.append(elapsed_time)

    average_time = sum(times) / len(times)
    print(f"Operación '{operation.upper()}' para {batch_size} registros -> Tiempo promedio: {average_time} ms")
    return average_time


# Ejecución de benchmarks
if __name__ == "__main__":
    client = HyperionDBClient("127.0.0.1", 8080)

    # Datos de prueba
    test_data = [
        {
            "_id": f"id_{i}",
            "article_id": f"article_{i}",
            "author": f"author_{i}",
            "email": f"author_{i}@example.com",
            "created_at": "2013-06-24T12:47:01.308Z",
            "title": f"title_{i}",
            "content": "Plunketts Creek Bridge..."
        }
        for i in range(100000)  # Aumentamos la cantidad de datos de prueba si es necesario
    ]

    # Tamaños de lote para los benchmarks
    batch_sizes = [1000, 10000, 100000]
    operations = ["insert", "query", "update", "delete"]

    # Diccionario para almacenar los tiempos promedio
    benchmark_results = {op: [] for op in operations}

    # Ejecuta benchmarks para cada operación y tamaño de lote
    for batch_size in batch_sizes:
        for operation in operations:
            avg_time = perform_operation_benchmark(client, operation, test_data, batch_size)
            benchmark_results[operation].append(avg_time)

    # Datos del documento de referencia para las demás bases de datos
    databases = ["SQL Server", "MySQL", "PostgreSQL", "MongoDB", "CouchDB", "Couchbase", "HyperionDB"]
    insert_times = [
        [530.1, 5516.2, 51075.7],  # SQL Server
        [757.1, 7326.4, 76705.7],  # MySQL
        [80.9, 798.7, 10476.7],    # PostgreSQL
        [54.9, 533.8, 5282.5],     # MongoDB
        [1.39, 19.7, 141.95],      # CouchDB
        [77.5, 783.67, 9188.13],   # Couchbase
        benchmark_results["insert"] # HyperionDB results
    ]
    update_times = [
        [36.1, 286.5, 2764.8],     # SQL Server
        [87.7, 1264, 10620.5],     # MySQL
        [77.3, 2385.2, 25421.5],   # PostgreSQL
        [17.3, 265.4, 2875.9],     # MongoDB
        [1.56, 18.64, 266.68],     # CouchDB
        [73.16, 731.39, 10414.85], # Couchbase
        benchmark_results["update"] # HyperionDB results
    ]
    delete_times = [
        [127, 482.9, 5715.4],      # SQL Server
        [78.3, 825.8, 18794.4],    # MySQL
        [35.5, 582.6, 11479.8],    # PostgreSQL
        [9, 133.8, 1530.9],        # MongoDB
        [1.19, 15.57, 132.7],      # CouchDB
        [39.37, 405.57, 6579.23],  # Couchbase
        benchmark_results["delete"] # HyperionDB results
    ]
    select_times = [
        [35.3, 243.6, 2313.4],     # SQL Server
        [4.1, 117.8, 844.8],       # MySQL
        [3.7, 19.4, 663.5],        # PostgreSQL
        [1, 6, 43.5],              # MongoDB
        [2.14, 30.44, 307.54],     # CouchDB
        [4.34, 34.89, 345.77],     # Couchbase
        benchmark_results["query"] # HyperionDB results
    ]
    labels = ["1000 ops", "10000 ops", "100000 ops"]

    # Función para generar gráficos con etiquetas solo para HyperionDB
    def plot_with_hyperion_labels(title, y_label, data):
        plt.figure(figsize=(10, 6))
        for db, times in zip(databases, data):
            plt.plot(labels, times, marker='o', label=db)
            # Etiquetas solo para HyperionDB
            if db == "HyperionDB":
                for x, y in zip(labels, times):
                    plt.text(x, y, f"{y:.2f} ms", fontsize=9, ha='right', color='blue')

        plt.xlabel("Number of Operations")
        plt.ylabel(y_label)
        plt.title(title)
        plt.legend()
        plt.grid()
        plt.show()

    # Generar gráficos con etiquetas solo para HyperionDB
    plot_with_hyperion_labels("Benchmark results for INSERT", "Mean Time (ms)", insert_times)
    plot_with_hyperion_labels("Benchmark results for UPDATE", "Mean Time (ms)", update_times)
    plot_with_hyperion_labels("Benchmark results for DELETE", "Mean Time (ms)", delete_times)
    plot_with_hyperion_labels("Benchmark results for SELECT", "Mean Time (ms)", select_times)
