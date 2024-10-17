import socket
import time

# Configuración del servidor
HOST = 'localhost'  # Dirección del servidor (puede ser 127.0.0.1 o localhost)
PORT = 12345        # Puerto en el que se ejecutará el servidor

# Crear el socket del servidor
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind((HOST, PORT))
server_socket.listen(1)

print(f"Servidor escuchando en {HOST}:{PORT}...")

# Esperar una conexión del cliente
client_socket, client_address = server_socket.accept()
print(f"Conectado por {client_address}")

try:
    while True:
        # Recibir datos del cliente
        data = client_socket.recv(1024).decode('utf-8')
        

        # Imprimir el estado recibido
        print(f"Estado recibido: {data}")

        # Simular un procesamiento
        time.sleep(1)

finally:
    client_socket.close()
    server_socket.close()
    print("Conexión cerrada")
