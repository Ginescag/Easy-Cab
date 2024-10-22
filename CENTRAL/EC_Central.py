import socket
import threading
import json
from kafka import KafkaProducer, KafkaConsumer
from CityMap import CityMap


class ECCentral:
    def __init__(self, port, kafka_ip_port):
        self.port = port
        self.kafka_ip_port = kafka_ip_port
        self.taxis = {}  # Diccionario para almacenar taxis (ID, estado, posición)
        self.customers = []  # Lista para almacenar las solicitudes de clientes
        self.citymap = CityMap('mapa.txt')  # Carga el mapa de la ciudad

        # Inicializa el productor y consumidor Kafka
        self.kafka_producer = KafkaProducer(bootstrap_servers=self.kafka_ip_port, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        self.kafka_consumer = KafkaConsumer('central_topic', bootstrap_servers=self.kafka_ip_port, value_deserializer=lambda v: json.loads(v.decode('utf-8')))

    def handle_customer_request(self, request):
        # Lógica para manejar la solicitud de un cliente (solicitar taxi)
        destination = request['destination']
        taxi_id = self.assign_taxi_to_customer(destination)
        if taxi_id is not None:
            # Enviar un mensaje Kafka al taxi asignado
            self.kafka_producer.send('taxi_topic', {'taxi_id': taxi_id, 'destination': destination})
            return {"status": "OK", "taxi_id": taxi_id}
        else:
            return {"status": "KO", "message": "No hay taxis disponibles"}

    def handle_taxi_update(self, request):
        # Lógica para manejar la actualización del estado de un taxi
        taxi_id = request['taxi_id']
        new_position = request['position']
        self.update_taxi_position(taxi_id, new_position)
        return {"status": "OK"}

    def assign_taxi_to_customer(self, destination):
        # Asigna un taxi disponible a un cliente
        for taxi_id, taxi_info in self.taxis.items():
            if taxi_info['status'] == 'free':
                self.taxis[taxi_id]['status'] = 'busy'
                self.taxis[taxi_id]['destination'] = destination
                return taxi_id
        return None

    def update_taxi_position(self, taxi_id, new_position):
        # Actualiza la posición del taxi en el mapa
        self.taxis[taxi_id]['position'] = new_position
        self.save_taxis_to_json()
        print(f"Taxi {taxi_id} se ha movido a {new_position}")

    def save_taxis_to_json(self):
        # Guarda el estado de los taxis en un archivo JSON
        with open('taxis_status.json', 'w') as file:
            json.dump(self.taxis, file, indent=4)

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

    def handle_client(self, client_socket):
        try:
            message = client_socket.recv(1024).decode('utf-8')
            if message:
                request = json.loads(message)
                if request['type'] == 'auth_request':
                    self.handle_authentication(client_socket, request)
                elif request['type'] == 'position_update':
                    self.handle_taxi_update(request)
        except Exception as e:
            print(f"Error handling client: {e}")
        finally:
            client_socket.close()

    def start(self):
        # Hilo para consumir mensajes Kafka
        threading.Thread(target=self.consume_kafka_messages, daemon=True).start()

        # Iniciar servidor socket
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(('0.0.0.0', self.port))
        server.listen(5)
        print(f"EC_Central escuchando en el puerto {self.port}")

        while True:
            client_socket, addr = server.accept()
            print(f"Conexión recibida de {addr}")
            client_handler = threading.Thread(target=self.handle_client, args=(client_socket,))
            client_handler.start()

    def consume_kafka_messages(self):
        # Método para consumir mensajes Kafka
        for message in self.kafka_consumer:
            print(f"Mensaje recibido de Kafka: {message.value}")
            # Aquí puedes procesar los mensajes Kafka recibidos

if __name__ == "__main__":
    # Parámetros de ejemplo, deben ser ajustados según los argumentos de línea de comandos
    central = ECCentral(port=5000, kafka_ip_port="127.0.0.1:9092")
    central.start()
