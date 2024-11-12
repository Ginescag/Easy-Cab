import socket
import sys
import threading
import re
import time
import json
from kafka import KafkaConsumer, KafkaProducer


TOPIC_TAXI_UPDATES = 'taxi_updates' 
TOPIC_ASIGNACION_TAXIS = 'asignacion-taxis' 
TOPIC_TAXI_END_CLIENT = 'taxi-end-client' 
TOPIC_TAXI_END_CENTRAL = 'taxi-end-central' 
FORMAT = 'utf-8'
HEADER = 64



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
        self.available = True
        self.inTaxi = False
        self.ordered = False
        self.arrived = False
        self.client_id = ''
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


    def send(self, msg, client):
        message = msg.encode(FORMAT)
        msg_length = len(message)
        send_length = str(msg_length).encode(FORMAT)
        send_length += b' ' * (HEADER - len(send_length))
        client.send(send_length)
        client.send(message)

    def send_to_kafka(self, topic, message):
        try:
            self.producer.send(topic, value=message)
            self.producer.flush()
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
        self.position = [x, y]
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
        print(f"Establecida conexión en [{client_socket.getsockname()}]")
        

        print("Envio al servidor: ", self.taxi_id)
        self.send(str(self.taxi_id), client_socket)
        
        while True:
            response = client_socket.recv(2048).decode('utf-8')
            pattern = r"'x' ?: ?(\d+)[.,] ?'y' ?: ?(\d+)"
            aux = re.search(pattern, response)

            if aux:
                self.position = [int(aux.group(1)), int(aux.group(2))]
                print(f"Mi coordenada es {self.position}")
            if response == "ERROR taxi doesnt exist":
                print("ERROR: id not on our database, try again ")
                sys.exit(1)
            else:
                print(f"Taxi {self.taxi_id} autenticado correctamente.")
                break
        client_socket.close()

    def listen_asignacion_kafka(self):
        for msg in self.consumer:
            if msg is None:
                continue
            command = msg.value
            print(f"Command received from Central: {command}")
            auxArr = command.split("#")
            if len(auxArr) == 7:
                Tid = int(auxArr[1])
                Gx = int(auxArr[2])
                Gy = int(auxArr[3])
                cliX = int(auxArr[4])
                cliY = int(auxArr[5])
                self.client_id = auxArr[6]
                if Tid == int(self.taxi_id):
                    self.goal_position = [Gx, Gy]
                    self.client_position = [cliX, cliY]
                    self.ordered = True
                    self.arrived = False
                    print(f"RECOGER AL CLIENTE EN {self.client_position} PARA IR A {self.goal_position}")

    def handle_sensors(self):
        ok = False
        while True:
            try:
                msg = conn.recv(1024).decode('utf-8')
                if msg:
                    if msg == "KO":
                        self.status = "KO"
                        self.send_to_kafka(TOPIC_TAXI_UPDATES,f"{self.taxi_id}#{msg}#{self.position[0]}#{self.position[1]}")
                        continue
                    if self.position == self.client_position and self.ordered:
                        self.inTaxi = True
                    
                    if self.inTaxi == False:
                        self.update_client_coordinates(self.position[0], self.position[1])
                        self.send_to_kafka(TOPIC_TAXI_UPDATES, f"{self.taxi_id}#{msg}#{self.position[0]}#{self.position[1]}")
                    else:
                        print(f"Ordenado == {self.ordered} y arrived =={self.arrived}")
                        if self.ordered and not self.arrived and self.position == self.goal_position:
                            self.send_to_kafka(TOPIC_TAXI_UPDATES, f"{self.taxi_id}#{msg}#{self.position[0]}#{self.position[1]}#destino")
                            print("Service completed")
                            self.arrived = True
                            aux = f"taxi#{self.taxi_id}#cliente#{self.client_id}#ha llegado a su destino"
                            self.producer_end.send(TOPIC_TAXI_END_CENTRAL, value=aux)
                            self.producer_end.send(TOPIC_TAXI_END_CLIENT, value=aux)

                            if ok == False:
                                ok = True
                                self.position[0] -= 1
                            self.inTaxi = False
                            self.ordered = False
                        
                        else:
                            self.updateCoordinates()
                            self.send_to_kafka(TOPIC_TAXI_UPDATES,f"{self.taxi_id}#{msg}#{self.position[0]}#{self.position[1]}#recogido")
            except socket.error as error:
                print(f"Error al recibir mensaje del Sensor: {error}") 
                break

if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Usage: python digital_engine.py <EC_Central_IP> <EC_Central_Port> <Kafka_IP_Port> <EC_DE_Port> <Taxi_ID>")
        sys.exit(1)
        
    DE = DigitalEngine(sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4], int(sys.argv[5]))

    DE.recogido = False
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect(DE.ec_central_addr)
    DE.connect_to_central()

    try:
        DE.sensor_socket.bind((DE.de_addr[0], DE.de_addr[1]))
        DE.sensor_socket.listen(1)
        print("Data engine up and listening at ", DE.de_addr[0], " ", DE.de_addr[1])
        conn, addr = DE.sensor_socket.accept()
        print("NUEVA CONEXION: ", addr)
    except socket.error as e:
        print(f"Error connecting to sensors: {e}")
        sys.exit(1)

    sensor_thread = threading.Thread(target=DE.handle_sensors)
    sensor_thread.start()

    kafka_thread = threading.Thread(target=DE.listen_asignacion_kafka)
    kafka_thread.start()

    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        print("Closing Conn...")
    finally:
        DE.sensor_socket.close()
        DE.producer.flush()
        DE.consumer.close()