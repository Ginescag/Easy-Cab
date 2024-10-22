import socket
import sys
import time
import threading

class Sensors:
    def __init__(self, ec_de_ip, ec_de_port):
        self.ec_de_addr = (ec_de_ip, int(ec_de_port))
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.running = True
        self.force_ko = False  # Indicador para forzar el envío de KO

    def connect_to_de(self):
        """Conecta con el Digital Engine."""
        self.client_socket.connect(self.ec_de_addr)
        print(f"Conectado a Digital Engine en {self.ec_de_addr}")

    def send_status(self, status):
        """Envía un mensaje de estado al Digital Engine."""
        try:
            self.client_socket.sendall(status.encode('utf-8'))
        except Exception as e:
            print(f"Error al enviar estado: {e}")
            self.running = False

    def simulate_sensors(self):
        """Simula los sensores enviando mensajes cada segundo."""
        while self.running:
            if self.force_ko:
                # Si se ha forzado un KO, lo enviamos y esperamos antes de continuar con OK
                self.send_status("KO")
            else:
                self.send_status("OK")
            time.sleep(1)

    def manual_trigger(self):
        """Permite al usuario alternar entre OK y KO manualmente."""
        print("Presione 'k' para KO, 'o' para OK, 'q' para salir.")
        while self.running:
            inp = input()
            if inp.lower() == 'k':
                self.force_ko = True  # Forzamos KO
            elif inp.lower() == 'o':
                self.force_ko = False  # Volvemos a OK
            elif inp.lower() == 'q':
                self.send_status('q')  # Enviar 'q' para cerrar
                self.running = False
                print("Cerrando aplicación de sensores...")

    def run(self):
        """Ejecuta los hilos para simulación y entrada de usuario."""
        try:
            self.connect_to_de()
            sensor_thread = threading.Thread(target=self.simulate_sensors)
            input_thread = threading.Thread(target=self.manual_trigger)
            sensor_thread.start()
            input_thread.start()
            sensor_thread.join()
            input_thread.join()
        except Exception as e:
            print(f"Error al iniciar los sensores: {e}")
        finally:
            self.client_socket.close()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python ec_s.py <IP_EC_DE> <Puerto_EC_DE>")
        sys.exit(1)
    
    ec_de_ip = sys.argv[1]
    ec_de_port = sys.argv[2]
    sensors = Sensors(ec_de_ip, ec_de_port)
    sensors.run()
