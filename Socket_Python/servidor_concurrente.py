import socket
import threading
import sys

if len(sys.argv) < 2:
    print("USO: servidor_concurrente.py <PUERTO>")
    sys.exit(1)

HEADER = 64
PORT = int(sys.argv[1])
SERVER = socket.gethostbyname(socket.gethostname())
ADDR = (SERVER, PORT)
FORMAT = 'utf-8'
FIN = "FIN"
MULT = "MULT"
SUM = "SUM"
MAX_CONEXIONES = 2

def handle_client(conn, addr):
    print(f"[NUEVA CONEXION] {addr} connected.")
    
    connected = True
    while connected:
        try:
            # Recibe la longitud del mensaje
            msg_length = conn.recv(HEADER).decode(FORMAT)
            if msg_length:
                msg_length = int(msg_length)
                
                # Recibe la operación
                operation = conn.recv(msg_length).decode(FORMAT)
                print(f"Operacion recibida: {operation}")
                
                if operation == FIN:
                    connected = False
                    continue

                if operation == SUM or operation == MULT:
                    # Recibe el primer operando
                    conn.send("PRIMER OPERANDO: ".encode(FORMAT))
                    operand1 = conn.recv(2048).decode(FORMAT)
                    operand1 = int(operand1)
                    print(f"Primer operando recibido: {operand1}")

                    # Recibe el segundo operando
                    conn.send("SEGUNDO OPERANDO: ".encode(FORMAT))
                    operand2 = conn.recv(2048).decode(FORMAT)
                    operand2 = int(operand2)  # Asegúrate de que sea un entero
                    print(f"Segundo operando recibido: {operand2}")
                    # Realiza la operación
                    if operation == SUM:
                        result = operand1 + operand2
                    elif operation == MULT:
                        result = operand1 * operand2
                    
                    print(f"Resultado calculado: {result}")
                    # Envía el resultado de vuelta al cliente
                    conn.send(f"RESULTADO: {result}".encode(FORMAT))
                else:
                    conn.send("OPERACION NO VALIDA".encode(FORMAT))

        except Exception as e:
            print(f"Error al procesar el cliente {addr}: {e}")
            connected = False

    print(f"Conexion con {addr} cerrada.")
    conn.close()

def start():
    server.listen()
    print(f"[LISTENING] Servidor a la escucha en {SERVER}")
    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()
        print(f"[CONEXIONES ACTIVAS] {threading.active_count() - 1}")

######################### MAIN ##########################
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(ADDR)

print("[STARTING] Servidor inicializándose...")
start()
