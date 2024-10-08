import socket
import sys

HEADER = 64
PORT = 5050  # Asegúrate de que coincida con el puerto correcto
FORMAT = 'utf-8'
FIN = "FIN"
MULT = "MULT"
SUM = "SUM"

def send(msg):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    client.send(send_length)
    client.send(message)

########## MAIN ##########

print("****** BIENVENIDO A NUESTRO CLIENTE SOCKET ****")

if len(sys.argv) == 3:
    SERVER = sys.argv[1]
    PORT = int(sys.argv[2])
    ADDR = (SERVER, PORT)
    
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(ADDR)
    print(f"Conexión establecida con [{ADDR}]")

    while True:
        print("Elige una operación:")
        print("SUM")
        print("MULT")
        print("Escribe 'FIN' para salir")

        option = input("Selecciona una opción: ").strip().upper()

        if option == 'SUM':
            send(SUM)  # Envía la operación Sumar
        elif option == 'MULT':
            send(MULT)  # Envía la operación Multiplicar
        elif option == FIN:
            send(FIN)  # Envía FIN para cerrar la conexión
            break
        else:
            print("Opción no válida")
            continue

        # Recibe del servidor el mensaje que pide el primer operando
        primer_operando_msg = client.recv(2048).decode(FORMAT)
        print(primer_operando_msg)
        operand1 = input().strip()
        print(operand1)  
        send(operand1)

        # Recibe del servidor el mensaje que pide el segundo operando
        segundo_operando_msg = client.recv(2048).decode(FORMAT)
        print(segundo_operando_msg)
        operand2 = input().strip()  
        send(operand2)

        # Recibe el resultado
        result = client.recv(2048).decode(FORMAT)
        print(result)

    print("Conexión cerrada.")
    client.close()
else:
    print("Error: Debes proporcionar <ServerIP> <Puerto>")
