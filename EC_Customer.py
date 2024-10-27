import socket
import json
import time
import sys
import threading
from kafka import KafkaProducer, KafkaConsumer

# Constantes de los tópicos de Kafka
TOPIC_SOLICITUDES_TAXIS = 'solicitudes-taxis' #produce una solicitud de taxi
TOPIC_RESPUESTAS_TAXIS = 'respuestas-taxis' #consume una respuesta de la central para los clientes
TOPIC_TAXI_END_CLIENT = 'taxi-end-client'   #consume un aviso de que se ha acabado el servicio del taxi

class ECCustomer:
    def __init__(self, kafka_ip_port, customer_id, destinations_file):
        self.kafka_ip_port = kafka_ip_port
        self.customer_id = customer_id
        self.destinations = self.load_destinations(destinations_file)
        self.current_destination_index = 0
        self.last_offset = -1
        self.last_offset_taxi_end = -1

        # Inicializar el productor y consumidor de Kafka
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_ip_port,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.consumer = KafkaConsumer(
            TOPIC_RESPUESTAS_TAXIS,
            bootstrap_servers=kafka_ip_port,
            group_id=f"grupo_{self.customer_id}",
            auto_offset_reset='latest',
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        self.consumer_taxi = KafkaConsumer(
            TOPIC_TAXI_END_CLIENT,
            bootstrap_servers=kafka_ip_port,
            group_id=f"grupo_{self.customer_id}",
            auto_offset_reset='latest',
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
    
    def handle_response(self):
        """Recibe la respuesta de la central para el cliente a través de Kafka."""
        for message in self.consumer:
            if message.offset > self.last_offset:
                mensaje = message.value
                clientID, confirmacion = mensaje.split(": ")
                if self.customer_id == clientID:
                    print(f"Procesando mensaje: {mensaje} con offset {message.offset}")

                    self.last_offset = message.offset
                    return mensaje
                else:
                    continue
            else:
                print(f"Ignorando mensaje con offset {message.offset}")

    def load_destinations(self, filename):
        """Carga la lista de destinos desde un archivo."""
        with open(filename, 'r') as file:
            data = json.load(file)
            return [request['Id'] for request in data['Requests']]

    def request_taxi(self, destination):
        """Envía una solicitud de taxi con origen y destino a través de Kafka."""
        message = f"{self.customer_id} solicita {destination}"
        self.producer.send(TOPIC_SOLICITUDES_TAXIS, value=message)
        self.producer.flush()

    def wait_arrival(self):
        """Espera la llegada del taxi para el cliente a través de Kafka."""
        print("ENTRO A ESCUCHAR")
        for message in self.consumer_taxi:
            if message.offset > self.last_offset_taxi_end:
                mensaje = message.value
                print(f'EL MENSAJE ES {mensaje}')
                self.last_offset_taxi_end = message.offset
                mensajes = mensaje.split('#')
                cliente_id = mensajes[3]

                if self.customer_id == cliente_id:
                    return "has llegado a tu destino"
                else:
                    continue
            else:
                continue

    def run(self):
        """Ejecuta la lógica principal del cliente."""
        try:
            # Iniciar un hilo para manejar las respuestas de Kafka
            threading.Thread(target=self.handle_response, daemon=True).start()

            # Enviar solicitudes de taxis para todos los destinos
            while self.current_destination_index < len(self.destinations):
                if self.request_taxi():
                    time.sleep(1)  # Esperar un segundo antes de enviar la siguiente solicitud

            print(f"Cliente {self.customer_id} ha completado todos los destinos.")
        except Exception as e:
            print(f"Error en el cliente: {e}")
        finally:
            self.producer.close()
            self.consumer.close()

if __name__ == "__main__":
    # Parámetros de ejemplo para la conexión y archivo de destinos
    if len(sys.argv) != 4:
        print("PARÁMETROS INCORRECTOS USAGE: EC_Customer <KAFKA_IP_PORT> <CUSTOMER_ID> <REQUESTS FILE>")
        sys.exit(1)
    
    kafka_ip_port = sys.argv[1]
    customer_id = sys.argv[2]
    destinations_file = sys.argv[3]

    # Crear un cliente y ejecutarlo
    customer = ECCustomer(kafka_ip_port, customer_id, destinations_file)
    for destination in customer.destinations:
        print()
        print(f"Enviando solicitud para ir al destino: {destination}")
        customer.request_taxi(destination)
        print()
        response = customer.handle_response()
        responseID, allOK = response.split(': ')
        print(f"Respuesta recibida para el servicio con destino {destination} : {allOK}")
        if allOK == 'OK':
        
           taxi_response = customer.wait_arrival()
           print()
           print(taxi_response)
        else:
            print('no se ha recibido confirmación')
        time.sleep(4)
