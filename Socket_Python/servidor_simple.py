import socket
import sys

if len(sys.argv) < 2:
    print("Uso: python servidor_simple.py <PUERTO>")
    sys.exit(1)

HOST = socket.gethostbyname(socket.gethostname())
PORT = int(sys.argv[1])

my_socket=socket.socket()
my_socket.bind((HOST, PORT))
my_socket.listen(5)

print ("Servidor creado y a la escucha en ", HOST, " ", PORT )
conexion, addr = my_socket.accept()

print ("Nueva conexion")
print (addr)

pet=conexion.recv(4096)
print ("Recibido: ", pet.decode())
    
conexion.send("HASTA LUEGO. CORTO CONEXION".encode('utf-8'))
print ("Cerrando Socket")
conexion.close()



