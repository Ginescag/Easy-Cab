import socket
import sys
import threading
import json
from kafka import KafkaConsumer, KafkaProducer

#tt se inicializa con las ips y puertos para la central y sensores
class DigitalEngine:
    def __init__(self, ec_central_ip, ec_central_port, kafka_ip_port, ec_s_ip, ec_s_port, taxi_id):
        self.ec_central_addr = (ec_central_ip, ec_central_port)
        self.kafka_ip_port = kafka_ip_port
        self.ec_s_addr = (ec_s_ip, ec_s_port)
        self.taxi_id = taxi_id
        self.authenticated = False
        self.producer = KafkaProducer(bootstrap_servers=kafka_ip_port, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        self.consumer = KafkaConsumer(self.taxi_id, bootstrap_servers=kafka_ip_port, value_deserializer=lambda v: json.loads(v.decode('utf-8')))
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

#al conectar con central, envía solicitud de autenticacion y recibe confirmacion
    def connect_to_central(self):
        """Establish a connection to EC_Central and authenticate."""
        self.client_socket.connect(self.ec_central_addr)
        print(f"Connected to EC_Central at {self.ec_central_addr}")
        self.authenticate()

    def authenticate(self):
        """Send authentication request to central."""
        auth_message = {
            "type": "auth_request",
            "taxi_id": self.taxi_id
        }
        self.client_socket.send(json.dumps(auth_message).encode('utf-8'))
        response = json.loads(self.client_socket.recv(1024).decode('utf-8'))
        if response.get("status") == "OK":
            self.authenticated = True
            print("Authentication successful.")
        else:
            print("Authentication failed.")

    def listen_to_central(self):
        """Listen for commands from EC_Central."""
        while True:
            try:
                message = self.client_socket.recv(1024).decode('utf-8')
                if message:
                    self.handle_message(json.loads(message))
            except Exception as e:
                print(f"Error receiving message from EC_Central: {e}")
                break
#aqui falta la logica 
    def handle_message(self, message):
        """Handle incoming messages from EC_Central."""
        if message['type'] == 'service_request':
            self.producer.send('taxi_topic', {'taxi_id': self.taxi_id, 'info': 'received service request'})
            # Aqui falta la matraca cuando se haga el mapa tt

    def run(self):
        """Start the Digital Engine."""
        try:
            self.connect_to_central()
            if self.authenticated:
                thread = threading.Thread(target=self.listen_to_central)
                thread.start()
        except Exception as e:
            print(f"An error occurred: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Usage: python digital_engine.py <EC_Central_IP> <EC_Central_Port> <Kafka_IP_Port> <EC_S_IP> <EC_S_Port> <Taxi_ID>")
        sys.exit(1)

    DE = DigitalEngine(sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4], int(sys.argv[5]), sys.argv[6])
    DE.run()
