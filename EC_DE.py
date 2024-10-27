import socket
import sys
import threading
import re
import time
import json
from kafka import KafkaConsumer, KafkaProducer

# INICIAL
TOPIC_TAXI_UPDATES = 'taxi_updates' #envia a central actualizaciones
TOPIC_ASIGNACION_TAXIS = 'asignacion-taxis' #consume de central para obtneer recados
TOPIC_TAXI_END_CLIENT = 'taxi-end-client' #envia al cliente el fin de servicio
TOPIC_TAXI_END_CENTRAL = 'taxi-end-central' #envia a central el fin de servicio

class DigitalEngine:
    def __init__(self, ec_central_ip, ec_central_port, kafka_ip_port, ec_de_port, taxi_id):
        self.ec_central_addr = (ec_central_ip, ec_central_port)
        self.kafka_ip_port = kafka_ip_port
        self.de_addr = (socket.gethostbyname(socket.gethostname()), int(ec_de_port))
        self.taxi_id = taxi_id
        self.status = "OK"
        self.position = [1, 1]
        self.client_position = [1, 1]
        self.goal_position = [1, 1]
        self.authenticated = False
        self.available = True
        self.recogido = False
        self.ordenado = False
        self.llegado = False
        self.cliente = ''
        self.producer = KafkaProducer(bootstrap_servers=kafka_ip_port, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        self.sensor_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.producer_end = KafkaProducer(bootstrap_servers=kafka_ip_port, value_serializer=lambda v: json.dumps(v).encode('utf-8'))       
        self.consumer = KafkaConsumer(
            TOPIC_ASIGNACION_TAXIS,
            bootstrap_servers=kafka_ip_port,
            group_id=f"taxi_{self.taxi_id}",
            auto_offset_reset='latest',
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )

    def send_to_kafka(self, topic, message):
        try:
            self.producer.send(topic, value=message)
            self.producer.flush()
            print(f"Mensaje enviado a Kafka: {message}")
        except Exception as e:
            print(f"Error al enviar mensaje a Kafka: {e}")

    def update_client_coordinates(self, x, y):
        if x < self.client_position[0]:
            x+= 1
        elif x > self.client_position[0]:
            x -= 1
        elif y < self.client_position[1]:
            y += 1
        elif y > self.client_position[1]:
            y -= 1
        print(f"{x, y}")
        return x, y

    def updateCoordinates(self):     
        if self.position[0] < self.goal_position[0]:
            self.position[0] += 1
        elif self.position[0] > self.goal_position[0]:
            self.position[0] -= 1
        elif self.position[1] < self.goal_position[1]:
            self.position[1] += 1
        elif self.position[1] > self.goal_position[1]:
            self.position[1] -= 1
        print(f"{self.position[0], self.position[1]}")

    def connect_to_central(self):
        try:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect(self.ec_central_addr)
            print(f"Establecida conexión en [{client_socket.getsockname()}]")
            
            # Enviar el ID del taxi al servidor central para verificación
            print("Envio al servidor: ", self.taxi_id)
            client_socket.send(str(self.taxi_id).encode('utf-8'))
            
            while True:
                replay = client_socket.recv(2048).decode('utf-8')
                patron = r"'x' ?: ?(\d+)[.,] ?'y' ?: ?(\d+)"
                resultado = re.search(patron, replay)

                if resultado:
                    # Extraer los valores de coordenadas
                    x = int(resultado.group(1))
                    y = int(resultado.group(2))

                    # Almacenar coordenadas
                    self.position = [x, y]
                    print(f"Mi coordenada es {self.position}")
                if replay == "El taxi no existe":
                    self.taxi_id = input("Introduce un ID valido: ")
                    client_socket.send(self.taxi_id.encode('utf-8'))  # Reenviar el mensaje con un ID válido
                else:
                    print(f"Taxi {self.taxi_id} autenticado correctamente.")
                    self.authenticated = True
                    break
        except Exception as e:
            print(f"Error al intentar autenticarse: {e}")
            self.authenticated = False
        finally:
            client_socket.close()

    def listen_asignacion_kafka(self):
        while True:
            msg = next(self.consumer)
            if msg is None:
                continue
            command = msg.value().decode('utf-8')
            print(f"Command received from Central: {command}")
            auxArr = command.split("#")
            if len(auxArr) == 7:
                Tid = int(auxArr[1])
                Gx = int(auxArr[2])
                Gy = int(auxArr[3])
                cliX = int(auxArr[4])
                cliY = int(auxArr[5])
                self.cliente = auxArr[6]
                if Tid == int(self.taxi_id):
                    self.goal_position = [Gx, Gy]
                    self.client_position = [cliX, cliY]
                    self.ordenado = True
                    self.llegado = False
                    print(f"RECOGER AL CLIENTE EN {self.client_position} PARA IR A {self.goal_position}")

    def handle_sensors(self):
        print(f"Intentando escuchar en la IP: {self.de_addr[0]}, Puerto: {self.de_addr[1]}")

        self.sensor_socket.bind((self.de_addr[0], self.de_addr[1]))
        self.sensor_socket.listen(1)
        print("Data engine up and listening at ", self.de_addr[0], " ", self.de_addr[1])
        conn, addr = self.sensor_socket.accept()
        print("NUEVA CONEXION: ", addr)

        def recibir_mensajes():
            while True:
                mensaje = conn.recv(1024).decode('utf-8')
                if mensaje == 'q':
                    print("Cerrando conexión...")
                    break
                if mensaje:
                    if mensaje == "KO":
                        self.status = "KO"
                    elif mensaje == "OK":
                        self.status = "OK"
                    print(f"Mensaje recibido del sensor: {mensaje}")
                    self.send_to_kafka(TOPIC_TAXI_UPDATES, f"{self.taxi_id}#{mensaje}#{self.position[0]}#{self.position[1]}")

        # Hilo para recibir mensajes del sensor
        threading.Thread(target=recibir_mensajes, daemon=True).start()

        # Hilo principal para mover el taxi
        try:
            while True:
                # Procesa la lógica de movimiento solo si el estado ha cambiado a "KO"
                if self.status == "KO":
                    # Actualizar coordenadas y enviar a Kafka
                    self.updateCoordinates()
                    self.send_to_kafka(TOPIC_TAXI_UPDATES, f"{self.taxi_id}#KO#{self.position[0]}#{self.position[1]}")

                time.sleep(1)  # Pausa para simular el proceso
        except socket.error as e:
            print(f"Error al recibir mensaje del Sensor: {e}")
        finally:
            conn.close()
            print("Conexión cerrada")


    def run(self):
        """Start the Digital Engine."""
        self.recogido = False
        try:
            # Iniciar los hilos de escucha antes de la autenticación
            sensor_thread = threading.Thread(target=self.handle_sensors, daemon=True)
            sensor_thread.start()
            
            kafka_thread = threading.Thread(target=self.listen_asignacion_kafka, daemon=True)
            kafka_thread.start()

            # Luego, intentamos conectarnos a la central y autenticarnos
            self.connect_to_central()
            
            # Si la autenticación es exitosa
            if self.authenticated:
                print("Conexión y autenticación exitosa.")
                while True:
                    time.sleep(5)  # Mantener el proceso principal en ejecución
            else:
                print("Fallo en la autenticación, revise la conexión con la central.")
        except KeyboardInterrupt:
            print("Cerrando conexiones...")
        finally:
            self.sensor_socket.close()
            self.producer.flush()
            self.consumer.close()


if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Usage: python digital_engine.py <EC_Central_IP> <EC_Central_Port> <Kafka_IP_Port> <EC_DE_Port> <Taxi_ID>")
        sys.exit(1)
        
    DE = DigitalEngine(sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4], int(sys.argv[5]))
    DE.run()