import socket
import time
import json

class HyperionDBClient:
    def __init__(self, host="127.0.0.1", port=8080):
        self.host = host
        self.port = port

    def send_command(self, command):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.host, self.port))
            s.sendall(command.encode() + b'\n')  # Envía el comando seguido de una nueva línea

            # Recibe la respuesta completa hasta encontrar el carácter de nueva línea final
            response = b""
            while True:
                chunk = s.recv(4096)
                if not chunk:
                    break
                response += chunk
                if b'\n' in chunk:  # Detiene la lectura al encontrar una nueva línea
                    break

        return response.decode().strip()  # Retorna la respuesta sin espacios adicionales

    def query(self, field, operator, value):
        command = f"QUERY {field} {operator} {value}"
        return self.send_command(command)

    def list(self):
        command = "LIST"
        return self.send_command(command)


# Función para realizar consultas y medir tiempos
def perform_query_and_measure_time(client, field, operator, value):
    start_time = time.time()
    try:
        result = client.query(field, operator, value)
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        # Convertir la respuesta a JSON y contar los registros
        result_data = json.loads(result)  # convierte la respuesta a JSON
        num_records = len(result_data)    # cuenta los registros
        
        print(f"Consulta: {field} {operator} {value}")
        print(f"Total de registros obtenidos: {num_records}")
        print(f"Tiempo de consulta: {elapsed_time:.4f} segundos\n")
        
    except Exception as e:
        print(f"Error en la consulta {field} {operator} {value}: {e}")


# Ejecución de consultas avanzadas
if __name__ == "__main__":
    client = HyperionDBClient("127.0.0.1", 8080)

    # Consultas avanzadas de ejemplo basadas en la estructura de datos
    queries = [
        ("currency", "=", "PKR"),                            # Moneda específica
        ("name", "CONTAINS", "Gaston"),                      # Consulta por nombre
        ("price", ">", "100"),                               # Consulta por precio mínimo
        ("price", "<", "300"),                               # Consulta por precio máximo
        ("specs.processor", "=", "Dual-core"),               # Procesador específico en JSON anidado
    ]

    # Ejecuta y mide cada consulta
    for field, operator, value in queries:
        perform_query_and_measure_time(client, field, operator, value)
