import socket
import time

# Configuración del cliente
HOST = 'localhost'  # Dirección del servidor
PORT = 12345        # Puerto del servidor

# Crear el socket del cliente
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket.connect((HOST, PORT))

print("Conectado al servidor. Escribe 'k' para enviar KO, o cualquier otra tecla para continuar enviando OK.")

try:
    while True:
        # Enviar el estado "OK" por defecto
        estado = "OK"
        
        # Leer la entrada del usuario (sin bloquear el envío predeterminado)
        if input("Presiona 'k' para enviar KO: ") == 'k':
            estado = "KO"

        # Enviar el estado actual al servidor
        client_socket.sendall(estado.encode('utf-8'))

        # Esperar 1 segundo antes de la siguiente actualización
        time.sleep(1)

finally:
    client_socket.close()
    print("Conexión cerrada")
