import socket
import threading
import json
from kafka import KafkaProducer, KafkaConsumer

# Constantes de los tópicos de Kafka
TOPIC_SOLICITUDES_TAXIS = 'solicitudes-taxis' #consume solicitudes de clientes para que les recojan
TOPIC_RESPUESTAS_TAXIS = 'respuestas-taxis' #produce una respuesta para los clientes
TOPIC_ASIGNACION_TAXIS = 'asignacion-taxis' #produce una respuesta para los taxis cuando se le asigna un cliente
TOPIC_TAXI_UPDATES = 'taxi_updates' #consume para obtener el estado y posicion de los taxis
TOPIC_TAXI_END_CENTRAL = 'taxi-end-central' #envia a central el fin de servicio

class ECCentral:
    def __init__(self, port, kafka_ip_port):
        self.port = port
        self.kafka_ip_port = kafka_ip_port
        self.taxis = {}  # Diccionario para almacenar taxis (ID, estado, posición)
        self.customers = []  # Lista para almacenar las solicitudes de clientes

        # Inicializa el productor y consumidor Kafka
        self.kafka_producer = KafkaProducer(bootstrap_servers=self.kafka_ip_port, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        self.kafka_consumer = KafkaConsumer(TOPIC_SOLICITUDES_TAXIS, bootstrap_servers=self.kafka_ip_port, value_deserializer=lambda v: json.loads(v.decode('utf-8')))
        self.kafka_consumer_taxiUP = KafkaConsumer(TOPIC_TAXI_UPDATES, bootstrap_servers=self.kafka_ip_port, value_deserializer=lambda v: json.loads(v.decode('utf-8')))

  #------------------socketAUTH--------------------------------------------------------------------------  
    def handle_authentication(self, client_socket, request):
        try:
            taxi_id = request['taxi_id']
            status = request['status']
            position = request['position']
            available = request['available']
            # Autenticación exitosa y registro del taxi
            self.taxis[taxi_id] = {'status': status, 'position': position, 'available': available}  # Añadir estado disponible
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

#--------------------------------------------------------------------------------------------------------
    def start(self):
        # Iniciar servidor socket
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        # Obtener la IP del host utilizando gethostbyname
        host_ip = socket.gethostbyname(socket.gethostname())
        server.bind((host_ip, self.port))
        
        server.listen(5)
        print(f"EC_Central escuchando en el puerto {self.port}, IP: {host_ip}")

        # Iniciar un hilo para consumir mensajes Kafka
        threading.Thread(target=self.consume_kafka_messages, daemon=True).start()

        while True:
            client_socket, addr = server.accept()
            print(f"Conexión recibida de {addr}")
            client_handler = threading.Thread(target=self.handle_client, args=(client_socket,))
            client_handler.start()

    def consume_kafka_messages(self):
        # Método para consumir mensajes Kafka
        threading.Thread(target=self.consume_taxi_updates, daemon=True).start()
        for message in self.kafka_consumer:
            print(f"Mensaje recibido de Kafka: {message.value}")
            request = message.value
            if request['type'] == 'customer_request':
                self.handle_customer_request(request)

    def consume_taxi_updates(self):
        for message in self.kafka_consumer_taxiUP:
            print(f"Mensaje de actualización de posición recibido: {message.value}")
            self.handle_position_update(message.value)

    def handle_position_update(self, request):
        try:
            taxi_id = request['taxi_id']
            position = request['position']
            status = request['status']
            available = request['available']
            # Actualizar la información del taxi
            if taxi_id in self.taxis:
                self.taxis[taxi_id]['position'] = position
                self.taxis[taxi_id]['status'] = status
                self.taxis[taxi_id]['available'] = available
                self.save_taxis_to_json()
                print(f"Estado del taxi {taxi_id} actualizado en el archivo JSON")
            else:
                print(f"Taxi ID {taxi_id} no está registrado")
        except Exception as e:
            print(f"Error al manejar la actualización de posición: {e}")

    def handle_customer_request(self, request):
        # Lógica para manejar la solicitud de un cliente (solicitar taxi)
        destination = request['destination']
        customer_id = request['customer_id']
        taxi_id = self.assign_taxi_to_customer(destination)
        if taxi_id is not None:
            # Enviar un mensaje Kafka al taxi asignado
            self.kafka_producer.send(TOPIC_ASIGNACION_TAXIS, {'taxi_id': taxi_id, 'destination': destination})
            # Responder al cliente con el taxi asignado
            response = {"status": "OK", "taxi_id": taxi_id, "destination": destination, "customer_id": customer_id}
        else:
            response = {"status": "KO", "message": "No hay taxis disponibles", "destination": destination, "customer_id": customer_id}
        # Publicar la respuesta del cliente en Kafka
        self.kafka_producer.send(TOPIC_RESPUESTAS_TAXIS, response)
        self.kafka_producer.flush()

    def assign_taxi_to_customer(self, destination):
        # Asigna un taxi disponible a un cliente
        for taxi_id, taxi_info in self.taxis.items():
            if taxi_info['available'] and taxi_info['status'] == 'OK':
                self.taxis[taxi_id]['available'] = False
                self.taxis[taxi_id]['destination'] = destination
                self.save_taxis_to_json()
                return taxi_id
        return None

if __name__ == "__main__":
    # Parámetros de ejemplo, deben ser ajustados según los argumentos de línea de comandos
    central = ECCentral(port=5000, kafka_ip_port="127.0.0.1:9092")
    central.start()
