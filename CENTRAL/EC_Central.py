import socket
import threading
import json
# from kafka import KafkaProducer, KafkaConsumer  # Comentado el uso de Kafka
#from CityMap import CityMap

class ECCentral:
    def __init__(self, port, kafka_ip_port):
        self.port = port
        self.kafka_ip_port = kafka_ip_port
        self.taxis = {}  # Diccionario para almacenar taxis (ID, estado, posición)
        self.customers = []  # Lista para almacenar las solicitudes de clientes
        #self.citymap = CityMap('mapa.txt')  # Carga el mapa de la ciudad

    def handle_authentication(self, client_socket, request):
        try:
            taxi_id = request['taxi_id']
            status = request['status']
            position = request['position']
            # Autenticación exitosa y registro del taxi
            self.taxis[taxi_id] = {'status': status, 'position': position}
            self.save_taxis_to_json()
            response = {"status": "OK"}
            client_socket.send(json.dumps(response).encode('utf-8'))
        except Exception as e:
            print(f"Error during authentication: {e}")
            response = {"status": "KO", "message": "Authentication failed"}
            client_socket.send(json.dumps(response).encode('utf-8'))
        finally:
            client_socket.close()

    def save_taxis_to_json(self):
        # Guarda el estado de los taxis en un archivo JSON
        with open('taxis_status.json', 'w') as file:
            json.dump(self.taxis, file, indent=4)

    def handle_client(self, client_socket):
        try:
            message = client_socket.recv(1024).decode('utf-8')
            if message:
                request = json.loads(message)
                if request['type'] == 'auth_request':
                    self.handle_authentication(client_socket, request)
                elif request['type'] == 'position_update':
                    # La actualización de posición se comentará por ahora
                    pass
        except Exception as e:
            print(f"Error handling client: {e}")
        finally:
            client_socket.close()

    def start(self):
        # Iniciar servidor socket
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        # Obtener la IP del host utilizando gethostbyname
        host_ip = socket.gethostbyname(socket.gethostname())
        server.bind((host_ip, self.port))
        
        server.listen(5)
        print(f"EC_Central escuchando en el puerto {self.port}, IP: {host_ip}")

        while True:
            client_socket, addr = server.accept()
            print(f"Conexión recibida de {addr}")
            client_handler = threading.Thread(target=self.handle_client, args=(client_socket,))
            client_handler.start()

if __name__ == "__main__":
    # Parámetros de ejemplo, deben ser ajustados según los argumentos de línea de comandos
    central = ECCentral(port=5000, kafka_ip_port="127.0.0.1:9092")
    central.start()
