import socket
import sys
import threading
import time
import json
from kafka import KafkaConsumer, KafkaProducer

class DigitalEngine:
    def __init__(self, ec_central_ip, ec_central_port, kafka_ip_port, ec_de_port, taxi_id):
        self.ec_central_addr = (ec_central_ip, ec_central_port)
        self.kafka_ip_port = kafka_ip_port
        self.de_addr = (socket.gethostbyname(socket.gethostname()), int(ec_de_port))
        self.taxi_id = taxi_id
        self.status = "OK"
        self.position = [0, 0]
        self.authenticated = False
        self.producer = KafkaProducer(bootstrap_servers=kafka_ip_port, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        self.sensor_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def connect_to_central(self):
        try:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect(self.ec_central_addr)
            auth_message = {
                "type": "auth_request",
                "taxi_id": self.taxi_id,
                "status": self.status,
                "position": self.position
            }
            client_socket.send(json.dumps(auth_message).encode('utf-8'))
            response = json.loads(client_socket.recv(1024).decode('utf-8'))
            if response.get("status") == "OK":
                print(f"Taxi {self.taxi_id} autenticado correctamente.")
                self.authenticated = True
            else:
                print(f"Error en la autenticación del taxi {self.taxi_id}: {response.get('message')}")
                self.authenticated = False
        except Exception as e:
            print(f"Error al intentar autenticarse: {e}")
            self.authenticated = False
        finally:
            client_socket.close()

    def send_position_update(self):
        if self.authenticated:
            update_message = {
                "type": "position_update",
                "taxi_id": self.taxi_id,
                "position": self.position,
                "status": self.status
            }
            self.producer.send('taxi_updates', update_message)
            print(f"Enviada actualización de posición para el taxi {self.taxi_id}: {self.position}, Estado: {self.status}")

    def update_taxi_status_in_json(self):
        try:
            with open('../central/taxis_status.json', 'r') as file:
                taxis_data = json.load(file)
            taxi_id_str = str(self.taxi_id)
            if taxi_id_str in taxis_data:
                taxis_data[taxi_id_str]['status'] = self.status
                taxis_data[taxi_id_str]['position'] = self.position
            with open('../central/taxis_status.json', 'w') as file:
                json.dump(taxis_data, file, indent=4)
        except Exception as e:
            print(f"Error al actualizar el estado del taxi en JSON: {e}")

    def handle_sensors(self):
        self.sensor_socket.bind((self.de_addr[0], self.de_addr[1]))
        self.sensor_socket.listen(1)
        print("Data engine up and listening at ", self.de_addr[0], " ", self.de_addr[1])
        conn, addr = self.sensor_socket.accept()
        print("NUEVA CONEXION: ", addr)

        try:
            while True:
                status = conn.recv(4096).decode()
                if status == 'q':
                    print("Cerrando conexión...")
                    break
                elif status == "OK":
                    self.status = "OK"
                    print(f"Estado recibido: {self.status}")
                elif status == "KO":
                    self.status = "KO"
                    print(f"Estado recibido: {self.status}")
                
                # Actualizar el estado en el JSON
                self.update_taxi_status_in_json()
                
                time.sleep(1)
                self.send_position_update()  # Siempre enviar la actualización del estado
        except Exception as e:
            print(f"Error en la recepción de datos: {e}")
        finally:
            conn.close()
            print("Conexión cerrada")

    def run(self):
        """Start the Digital Engine."""
        try:
            # Primero, intentamos conectarnos a la central y autenticarnos
            self.connect_to_central()
            
            # Si la autenticación es exitosa, iniciamos la escucha del sensor
            if self.authenticated:
                print("Conexión y autenticación exitosa. Escuchando al sensor...")
                self.handle_sensors()  # Ahora, una vez autenticado, escucha al sensor directamente

        except Exception as e:
            print(f"An error occurred: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Usage: python digital_engine.py <EC_Central_IP> <EC_Central_Port> <Kafka_IP_Port> <EC_DE_Port> <Taxi_ID>")
        sys.exit(1)

    DE = DigitalEngine(sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4], int(sys.argv[5]))
    DE.run()
