import socket
import json
import time
import sys
import threading
from kafka import KafkaProducer, KafkaConsumer

# Constantes de los tópicos de Kafka
TOPIC_SOLICITUDES_TAXIS = 'solicitudes-taxis'
TOPIC_RESPUESTAS_TAXIS = 'respuestas-taxis'
TOPIC_ASIGNACION_TAXIS = 'asignacion-taxis'

class ECCustomer:
    def __init__(self, kafka_ip_port, customer_id, destinations_file):
        self.kafka_ip_port = kafka_ip_port
        self.customer_id = customer_id
        self.destinations = self.load_destinations(destinations_file)
        self.current_destination_index = 0

        # Inicializar el productor y consumidor de Kafka
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_ip_port,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.consumer = KafkaConsumer(
            TOPIC_RESPUESTAS_TAXIS,
            bootstrap_servers=self.kafka_ip_port,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            group_id=f'customer_{self.customer_id}'
        )

    def load_destinations(self, filename):
        """Carga la lista de destinos desde un archivo."""
        with open(filename, 'r') as file:
            data = json.load(file)
            return [request['Id'] for request in data['Requests']]

    def request_taxi(self):
        """Envía una solicitud de taxi a través de Kafka."""
        if self.current_destination_index >= len(self.destinations):
            print("No hay más destinos para solicitar.")
            return False

        destination = self.destinations[self.current_destination_index]
        request = {
            "type": "customer_request",
            "customer_id": self.customer_id,
            "destination": destination
        }

        # Enviar solicitud a Kafka
        self.producer.send(TOPIC_SOLICITUDES_TAXIS, request)
        self.producer.flush()
        print(f"Solicitud enviada para el destino: {destination}")
        return True

    def handle_response(self):
        """Espera y maneja las respuestas de la central."""
        for message in self.consumer:
            response_data = message.value
            if response_data.get("customer_id") == self.customer_id:
                if response_data.get("status") == "OK":
                    print(f"Taxi asignado para el destino {response_data['destination']}. ID del taxi: {response_data['taxi_id']}")
                    self.wait_for_service_completion()
                    self.current_destination_index += 1
                else:
                    print(f"Solicitud de taxi rechazada para el destino {response_data['destination']}.")
                    time.sleep(4)  # Reintentar después de 4 segundos

    def wait_for_service_completion(self):
        """Simula la espera para la finalización del servicio."""
        print("Esperando a que el taxi complete el servicio...")
        # Para la simulación, esperaremos 4 segundos antes de proceder al siguiente destino
        time.sleep(4)
        print("Servicio completado.")

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
    if len(sys.argv) < 3:
        print("PARÁMETROS INCORRECTOS USAGE: EC_Customer <KAFKA_IP_PORT> <CUSTOMER_ID [0...99]>")
        sys.exit(1)
    
    kafka_ip_port = sys.argv[1]
    customer_id = sys.argv[2]
    destinations_file = "EC_Requests.json"

    # Crear un cliente y ejecutarlo
    customer = ECCustomer(kafka_ip_port, customer_id, destinations_file)
    customer.run()
    print(customer.destinations)
