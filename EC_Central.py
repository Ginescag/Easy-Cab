import socket
import threading
import sys
import json
from kafka import KafkaProducer, KafkaConsumer
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.colors as mcolors
from matplotlib.animation import FuncAnimation

# Constantes de los tópicos de Kafka
TOPIC_SOLICITUDES_TAXIS = 'solicitudes-taxis' #consume solicitudes de clientes para que les recojan service
TOPIC_RESPUESTAS_TAXIS = 'respuestas-taxis' #produce una respuesta para los clientes central_replay
TOPIC_ASIGNACION_TAXIS = 'asignacion-taxis' #produce una respuesta para los taxis cuando se le asigna un cliente taxi commands
TOPIC_TAXI_UPDATES = 'taxi_updates' #consume para obtener el estado y posicion de los taxis taxi_status
TOPIC_TAXI_END_CENTRAL = 'taxi-end-central' #envia a central el fin de servicio taxi_end2

class ECCentral:
    def __init__(self, port, kafka_ip_port, taxi_bd):
        self.port = int(port)
        self.kafka_ip_port = kafka_ip_port
        self.taxi_bd = taxi_bd
        self.offset_taxi_end = -1
        
        self.kafka_consumer_taxi = KafkaConsumer(
            TOPIC_TAXI_UPDATES, 
            bootstrap_servers=self.kafka_ip_port,
            group_id='central_taxi_group',
            auto_offset_reset='latest',
            value_deserializer=lambda v: json.loads(v.decode('utf-8')))
        
        self.producer_taxicommands = KafkaProducer(
            bootstrap_servers=kafka_ip_port,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        self.producer_customer = KafkaProducer(
            bootstrap_servers=kafka_ip_port,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        self.consumer_customer = KafkaConsumer(
            TOPIC_SOLICITUDES_TAXIS,
            bootstrap_servers=kafka_ip_port,
            group_id='central_customer_group',
            auto_offset_reset='latest',
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )

        self.consumer_taxi_end = KafkaConsumer(
            TOPIC_TAXI_END_CENTRAL,
            bootstrap_servers=kafka_ip_port,
            group_id='taxi_end_group',
            auto_offset_reset='latest',
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )

        
  #------------------socketAUTH--------------------------------------------------------------------------  

    def save_taxis_to_json(filename, taxis):
        # Guarda el estado de los taxis en un archivo JSON
        with open(filename, 'w') as file:
            json.dump(taxis, file, indent=4)
   

    def load_file(self, filename):
        try:
            with open(filename, 'r') as file:
                return json.load(file)
        except FileNotFoundError:
            print(f"{filename} not found.")
            return []
        except json.JSONDecodeError:
            print(f"Error decoding {filename}.")
            return []
    

    def check_taxi_status(id_taxi, estado, taxis):
        for taxi in taxis:
            if taxi["id"] == id_taxi:
                taxi["estado"] = "verde" if estado == "OK" else "rojo"
                return True
        return False


    def check_id(taxis, id_taxi):
        for taxi in taxis:
            if taxi['id'] == id_taxi:
                return True
        return False
    

    def listen_taxi_updates(self):
        try:
            for msg in self.kafka_consumer_taxi:
                # Proceso del mensaje de estado recibido desde Kafka
                estado_mensaje = msg.value
                partes = estado_mensaje.split("#")
                taxis = self.load_file(self.taxi_bd)
                mapa = self.load_file('Mapa.json')

                if len(partes) == 4:  # si todavía no lo ha recogido
                    taxi_id = int(partes[0])
                    estado = partes[1]
                    coor_taxix = int(partes[2])
                    coor_taxiy = int(partes[3])

                    for taxi in taxis:
                        if taxi['id'] == taxi_id:
                            taxi['coordenada_origen'] = {'x': coor_taxix, 'y': coor_taxiy}
                            self.save_taxis_to_json(self.taxi_bd, taxis)

                    if self.check_taxi_status(taxi_id, estado, taxis):
                        self.save_taxis_to_json(self.taxi_bd, taxis)
                    else:
                        print(f"Taxi {taxi_id} no encontrado en el archivo.")
                else:  # si lo ha recogido
                    taxi_id = int(partes[0])
                    estado = partes[1]
                    coor_taxix = int(partes[2])
                    coor_taxiy = int(partes[3])
                    destorec = partes[4]  # destino o recogido

                    for taxi in taxis:
                        if taxi['id'] == taxi_id:
                            taxi['coordenada_origen'] = {'x': coor_taxix, 'y': coor_taxiy}

                            if destorec == "recogido":
                                taxi['recogido'] = True
                                taxi['cliente'] = {'x': coor_taxix, 'y': coor_taxiy}
                                idcliente = taxi['cliente']['id_cliente']
                                mapa[idcliente] = [coor_taxix, coor_taxiy]
                            else:
                                taxi['recogido'] = False

                            self.save_taxis_to_json(self.taxi_bd, taxis)
                            self.save_taxis_to_json('Mapa.json', mapa)
        except Exception as e:
            print(f"Error al escuchar actualizaciones de taxis: {e}")
    

    def listen_customer_services(self):
        try:
            for msg in self.consumer_customer:

                mensaje_cliente = msg.value.decode('utf-8')
                print(f"Mensaje recibido del cliente: {mensaje_cliente}")

                servicios = mensaje_cliente.split(" ")
                
                if len(servicios) != 3:
                    print("ERROR WRONG FORMAT.")
                    continue
                
                client, aux, destination = servicios
                
                taxi_available = self.check_taxi_availability(destination, client)

                if taxi_available:
                    response = f"{client}: OK"
                else:
                    response = f"{client}: KO"

                # Enviar la respuesta al customer
                self.producer_customer.send(TOPIC_RESPUESTAS_TAXIS, value=response.encode('utf-8'))
                self.producer_customer.flush()
                print(f"Response sent: {response}")
        except Exception as e:
            print(f"Error al escuchar servicios de clientes: {e}")
    

    def listen_taxi_end(self):
        try:
            for msg in self.consumer_taxi_end:
                if msg.offset > self.offset_taxi_end:
                    mensaje = msg.value
                    print(f"el mensaje es {mensaje}")
                    self.offset_taxi_end = msg.offset
                    
                    mensajes = mensaje.split('#')
                    taxi_id = int(mensajes[1])
                    cliente_id = mensajes[3]
                    taxis = self.load_file(self.taxi_bd)
                    mapa = self.load_file('Mapa.json')
                    
                    for taxi in taxis:
                        if taxi['id'] == taxi_id:
                            taxi['disponible'] = True
                            taxi['recogido'] = False
                            taxi['cliente']['id_cliente'] = ""
                            taxi['cliente']['x'] = None
                            taxi['cliente']['y'] = None
                            taxi['coordenada_destino']['x'] = None
                            taxi['coordenada_destino']['y'] = None
                            taxi['coordenada_destino']['id'] = ""
                            print(f"taxi {taxi_id} has arrived")

                            corx = taxi['coordenada_origen']['x']
                            cory = taxi['coordenada_origen']['y']
                            mapa[cliente_id] = [corx, cory]

                            self.save_taxis_to_json(self.taxi_bd, taxis)
                            self.save_taxis_to_json('Mapa.json', mapa)
        except Exception as e:
            print(f"Error al escuchar el fin del servicio del taxi: {e}")


    def send_coordinates(self, producer, taxi_id, kafka_topic):
        taxis = self.load_file(self.taxi_bd)
        
        for taxi in taxis:
            if taxi['id'] == taxi_id:
                coordenada_destino = taxi['coordenada_destino']
                cliente = taxi['cliente']
                mensaje = f"Taxi has to go to#{taxi_id}#{coordenada_destino['x']}#{coordenada_destino['y']}#{cliente['x']}#{cliente['y']}#{cliente['id_cliente']}"
                print(f"sent to EC_DE: {mensaje}")
                
                # Enviar mensaje a EC_DE a través de Kafka
                producer.send(kafka_topic, value=mensaje)
                producer.flush()
                return True
        return False


    def get_coordinates(self, destino, client):
        coordinates = self.load_file('Mapa.json')
        if destino in coordinates:
            destX, destY = coordinates[destino]
        if client in coordinates:
            clientXpos, clientYpos = coordinates[client]
            print(f"Client coordinates are {clientXpos}, {clientYpos}")
        return clientXpos, clientYpos, destX, destY
    

    def socket_taxi(self, conn, addr):
        print(f"[NEW CONN] {addr} connected.")
    
        while True:
            msg_length = conn.recv(64).decode('utf-8')
            if not msg_length:
                break
            msg_length = int(msg_length)
            msg = conn.recv(msg_length).decode('utf-8')
            taxis = self.load_file(self.taxi_bd)
            id_taxi = int(msg)
            exists = self.check_id(taxis, id_taxi)
            for taxi in taxis:
                if taxi["id"] == id_taxi:
                    coordinates = taxi["coordenada_origen"]
                    taxi['verificado'] = True 
                    
            print(f"my taxi ID is: {msg} and my coordinates are {coordinates}")
            
            self.save_taxis_to_json(self.taxi_bd, taxis)

            response = ""
            if exists:
                response += f"your coordinates are {coordinates}"
            else:
                response += "ERROR taxi doesnt exist"
            conn.send(response.encode('utf-8'))
        
        conn.close()


    def check_taxi_availability(self, destino, client):
        taxis = self.load_file(self.taxi_bd)

        for taxi in taxis:
            if taxi['disponible'] and taxi['verificado']:  # Verifica si el campo 'disponible' es True
                print(f"Taxi disponible: ID {taxi['id']}, Estado: {taxi['estado']}, Coordenada origen: {taxi['coordenada_origen']}")
                taxi['disponible'] = False
                taxi['coordenada_destino']['id'] = destino
                clientX, clientY, destX, destY = self.get_coordinates(destino, client)
                taxi['coordenada_destino']['x'] = destX
                taxi['coordenada_destino']['y'] = destY
                taxi['cliente']['x'] = clientX
                print(f"la coor x es {taxi['coordenada_destino']['x']}")
                taxi['cliente']['y'] = clientY
                taxi['cliente']['id_cliente'] = client
                print(f"el cliente está en {clientX},{clientY} y quiere ir a {destX},{destY}")
                self.save_taxis_to_json(self.taxi_bd, taxis)
                
                self.send_coordinates(self.producer_taxicommands, taxi['id'], TOPIC_ASIGNACION_TAXIS)

                return True  
        print("No available taxis.")
        return False
    
    
    def actualizar_mapa(self, frame, taxis, ubicaciones, ax, size):
        ax.clear()  # Limpiar el gráfico actual para redibujar

        # Crear una matriz para los colores de fondo de cada celda (números en lugar de nombres de colores)
        mapa_colores = np.zeros((size, size))

        # Definir un mapa de colores personalizado
        cmap = mcolors.ListedColormap(['white', 'yellow', 'blue', 'green', 'red'])
        bounds = [0, 1, 2, 3, 4, 5]  # Limites para cada color
        norm = mcolors.BoundaryNorm(bounds, cmap.N)
        ubicaciones = self.load_file('Mapa.json')
        # Colocar los clientes en el mapa (letras minúsculas, fondo amarillo -> valor 1)
        for cliente, pos in ubicaciones.items():
            if cliente.islower():  # Clientes
                mapa_colores[pos[1] - 1, pos[0] - 1] = 1 # Fondo amarillo para clientes

        # Colocar los destinos en el mapa (letras mayúsculas, fondo azul -> valor 2)
        for destino, pos in ubicaciones.items():
            if destino.isupper():  # Destinos
                mapa_colores[pos[1] - 1, pos[0] - 1] = 2  # Fondo azul para destinos

        # Simulación: actualizando posiciones de taxis para la animación
        # Puedes reemplazar esta lógica para hacer que se lea desde un archivo o una API
        taxisact = self.load_file('taxis.json')
        xtaxi1 = None
        ytaxi1 = None
        xtaxi2 = None
        ytaxi2 = None

        for ta in taxisact:
            if ta['id'] == 1:
                xtaxi1 = ta['coordenada_origen']['x']
                ytaxi1 = ta['coordenada_origen']['y']
            if ta['id'] == 2:
                xtaxi2 = ta['coordenada_origen']['x']
                ytaxi2 = ta['coordenada_origen']['y']

        for taxi in taxis:
            if taxi['id'] == 1:
                taxi['coordenada_origen']['x'] = xtaxi1
                taxi['coordenada_origen']['y'] = ytaxi1
            if taxi['id'] == 2:
                taxi['coordenada_origen']['x'] = xtaxi2
                taxi['coordenada_origen']['y'] = ytaxi2

        # Colocar los taxis en el mapa (estado verde -> valor 3, rojo -> valor 4)
        
        for taxi in taxisact:
            taxi_pos = taxi['coordenada_origen']
            estado = taxi['estado']
        #############################################
            if estado == "verde":  # Taxis disponibles (verde -> valor 3)
                mapa_colores[taxi_pos['y'] - 1, taxi_pos['x'] - 1] = 3
            else:  # Taxis ocupados (rojo -> valor 4)
                mapa_colores[taxi_pos['y'] - 1, taxi_pos['x'] - 1] = 4

    
        # Crear el mapa de colores con casillas alineadas
        ax.imshow(mapa_colores, cmap=cmap, norm=norm, extent=[0, size, 0, size], origin='lower')

        # Colocar el texto de las identificaciones después de colocar las casillas
        # Colocar identificaciones de clientes
    
        for cliente, pos in ubicaciones.items():
            if cliente.islower():
                ax.text(pos[0] - 0.5, pos[1] - 0.5, cliente, color='black', fontsize=12, ha='left', va='center')           

        # Colocar identificaciones de destinos
        for destino, pos in ubicaciones.items():
            if destino.isupper():
                ax.text(pos[0] - 0.5, pos[1] - 0.5, destino, color='black', fontsize=12, ha='center', va='center')

        # Colocar identificaciones de taxis
        for taxi in taxisact:
            taxi_pos = taxi['coordenada_origen']
            if taxi['recogido'] == True:
                taxi_id = str(taxi['id']) #+"-" + taxi['cliente']['id_cliente']
            else:
                taxi_id = taxi['id']
            ax.text(taxi_pos['x'] - 0.5, taxi_pos['y'] - 0.5, str(taxi_id), color='black', fontsize=12, ha='right', va='center')

        # Configurar los ejes para que vayan de 1 a 20
        ax.set_xticks(np.arange(0, size))
        ax.set_yticks(np.arange(0, size))
        ax.set_xticklabels(np.arange(1, size + 1))
        ax.set_yticklabels(np.arange(1, size + 1))

        # Configurar cuadrícula
        ax.grid(True, color='black', linestyle='-', linewidth=0.5)
        ax.set_xlim(0, size)
        ax.set_ylim(0, size)

        plt.gca().invert_yaxis()  # Invertir el eje Y para que (1,1) esté en la esquina inferior izquierda


    def iniciar_grafico(self, taxis, ubicaciones):
        fig, ax = plt.subplots()
        size = 20
        ani = FuncAnimation(fig, self.actualizar_mapa, fargs=(taxis, ubicaciones, ax, size), interval=1000, cache_frame_data=False)
        plt.show()



#------------------------------------------------------------------------------------------
    
    def start(self):
        server.listen()
        print(f"[LISTENING] Servidor a la escucha en el puerto {self.port}")

        while True:

            conn, addr = server.accept()
            print(f"[NEW CONNECTION] {addr}")
            

            thread = threading.Thread(target=self.socket_taxi, args=(conn, addr))
            thread.start()
            print(f"[CONEXIONES ACTIVAS] {threading.active_count() - 1}")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print('USAGE: EC_Central.py <CENTRAL_PORT> <KAFKA_IP_PORT> <taxis.json>')
        sys.exit(1)

    
    # Parámetros de ejemplo, deben ser ajustados según los argumentos de línea de comandos
    central = ECCentral(sys.argv[1], sys.argv[2], sys.argv[3])
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((socket.gethostbyname(socket.gethostname()), central.port))

    taxis = central.load_file('taxis.json')
    ubis = central.load_file('Mapa.json')


    kafka_thread_taxis = threading.Thread(target=central.listen_taxi_updates)
    kafka_thread_taxis.start()
    kafka_thread_customer = threading.Thread(target=central.listen_customer_services)
    kafka_thread_customer.start()
    thread_socket = threading.Thread(target=central.start)
    thread_socket.start()
    kafka_thread_end_taxi = threading.Thread(target=central.listen_taxi_end)
    kafka_thread_end_taxi.start()
        # Iniciar el gráfico en el hilo principal
        
    central.iniciar_grafico(taxis, ubis)