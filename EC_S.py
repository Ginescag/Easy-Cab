
import socket
import sys
import threading
import time

def wait_input(client_socket, estado):
    print("Presione 'k' para KO, 'o' para OK, 'q' para salir.")
    while estado['running']:
        user_input = input()
        if user_input.lower() == 'k':
            estado['value'] = "KO"
            print(f"Estado cambiado a {estado['value']}")
        elif user_input.lower() == 'o':
            estado['value'] = "OK"
            print(f"Estado cambiado a {estado['value']}")
        elif user_input.lower() == 'q':
            send_message(client_socket, 'q')  # Enviar 'q' para cerrar
            estado['running'] = False
            print("Cerrando aplicación de sensores...")

def send_message(client_socket, message):
    try:
        client_socket.sendall(message.encode('utf-8'))
        print(f"Mensaje enviado: {message}")
    except socket.error as e:
        print(f"Error al enviar mensaje: {e}")

def run():
    if len(sys.argv) != 3:
        print("Uso: python EC_S.py <IP_EC_DE> <PUERTO_EC_DE>")
        sys.exit(1)

    ip_de = sys.argv[1]
    puerto_de = int(sys.argv[2])

    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((ip_de, puerto_de))
        print(f"Conectado al EC_DE en {ip_de}:{puerto_de}")
    except socket.error as e:
        print(f"Error al conectarse a EC_DE: {e}")
        sys.exit(1)
    estado = {'value': 'OK', 'running': True}

    input_thread = threading.Thread(target=wait_input, args=(client_socket, estado))
    input_thread.start()

    try:
        while estado['running']:
            send_message(client_socket, estado['value'])  
            time.sleep(1)  
    except KeyboardInterrupt:
        print("Cerrando conexión...")
    finally:
        estado['running'] = False
        input_thread.join()
        client_socket.close()

if __name__ == "__main__":
    run()
