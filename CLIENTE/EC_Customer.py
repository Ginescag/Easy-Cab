import socket
import json
import time
import sys
class ECCustomer:
    def __init__(self, broker_ip, broker_port, customer_id, destinations_file):
        self.broker_ip = broker_ip
        self.broker_port = broker_port
        self.customer_id = customer_id
        self.destinations = self.load_destinations(destinations_file)
        self.current_destination_index = 0

    def load_destinations(self, filename):
        """Carga la lista de destinos desde un archivo."""
        with open(filename, 'r') as file:
            return [line.strip() for line in file if line.strip()]

    def connect_to_central(self): #this is changing
        """Establece la conexión con la central."""
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.connect((self.broker_ip, self.broker_port))

    def request_taxi(self): #this is changing
        """Solicita un taxi a la central."""
        if self.current_destination_index >= len(self.destinations):
            print("No hay más destinos para solicitar.")
            return False

        destination = self.destinations[self.current_destination_index]
        request = {
            "type": "customer_request",
            "customer_id": self.customer_id,
            "destination": destination
        }

        # Enviar solicitud a la central
        self.client_socket.send(json.dumps(request).encode('utf-8'))

        # Esperar respuesta de la central
        response = self.client_socket.recv(1024).decode('utf-8')
        response_data = json.loads(response)

        if response_data.get("status") == "OK":
            print(f"Taxi asignado para el destino {destination}. ID del taxi: {response_data['taxi_id']}")
            return True
        else:
            print(f"Solicitud de taxi rechazada para el destino {destination}.")
            return False

    def wait_for_service_completion(self):
        """Simula la espera para la finalización del servicio."""
        print("Esperando a que el taxi complete el servicio...")
        # Para la simulación, esperaremos 4 segundos antes de proceder al siguiente destino
        time.sleep(4)
        print("Servicio completado.")

    def run(self):
        """Ejecuta la lógica principal del cliente."""
        try:
            self.connect_to_central()
            print(f"Cliente {self.customer_id} conectado a la central.")

            while self.current_destination_index < len(self.destinations):
                # Solicitar un taxi para el siguiente destino
                if self.request_taxi():
                    # Esperar a que el servicio se complete antes de proceder
                    self.wait_for_service_completion()
                    self.current_destination_index += 1
                else:
                    # Si no se pudo asignar un taxi, esperar antes de intentar nuevamente
                    print("Reintentando en 4 segundos...")
                    time.sleep(4)

            print(f"Cliente {self.customer_id} ha completado todos los destinos.")
        except Exception as e:
            print(f"Error en el cliente: {e}")
        finally:
            self.client_socket.close()

if __name__ == "__main__":
    # Parámetros de ejemplo para la conexión y archivo de destinos
    if len(sys.argv) < 4:
        print("PARÁMETROS INCORRECTOS USAGE: EC_Customer <BROKER_IP> <BROKER_PORT> <CUSTOMER_ID [0...99]>")
        sys.exit(1)
    
    broker_ip = sys.argv[1]
    broker_port = int(sys.argv[2])
    customer_id = int(sys.argv[3])
    destinations_file = "destinos.txt"

    # Crear un cliente y ejecutarlo
    customer = ECCustomer(broker_ip, broker_port, customer_id, destinations_file)
    customer.run()
    print(customer.destinations)
