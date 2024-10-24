import tkinter as tk
import time
import json
import threading

# Constantes para el tamaño de la ventana y las celdas
WINDOW_SIZE = 600
GRID_SIZE = 20
CELL_SIZE = WINDOW_SIZE // GRID_SIZE

# Colores en hexadecimal para Tkinter
WHITE = "#FFFFFF"
BLUE = "#3C92DE"
YELLOW = "#FFFF00"
GREEN = "#59E01F"
RED = "#FF0000"
BLACK = "#000000"

class CityMap(tk.Canvas):
    def __init__(self, master, filename, width, height, grid_size, cell_size):
        super().__init__(master, width=width, height=height, bg=WHITE)
        self.grid_size = grid_size
        self.cell_size = cell_size
        self.city_map = [[[] for _ in range(self.grid_size)] for _ in range(self.grid_size)]  # Mapa de objetos (clientes, taxis)
        self.localidad_map = [['' for _ in range(self.grid_size)] for _ in range(self.grid_size)]  # Mapa separado para las localidades
        self.taxi_state = {}  # Diccionario para manejar el estado de cada taxi (VERDE o ROJO)
        self.filename = filename
        self.load_map()

        self.pack()
        self.draw_grid()

    def load_map(self):
        """Lee el archivo JSON y genera el mapa como una matriz 2D."""
        with open(self.filename, 'r') as file:
            data = json.load(file)

            # Cargar localidades
            for loc in data.get('locations', []):
                id_loc = loc['Id']
                x, y = map(int, loc['POS'].split(','))  # Extraer las coordenadas como enteros
                if id_loc.isupper():
                    self.localidad_map[x][y] = id_loc  # Guardar la localidad en el mapa fijo

            # Cargar clientes
            for customer in data.get('customers', []):  # Obtener los clientes si existen
                id_cust = customer['Id']
                x, y = map(int, customer['POS'].split(','))  # Extraer las coordenadas como enteros
                self.city_map[x][y].append(id_cust)  # Añadir el cliente al mapa de la ciudad

    def draw_grid(self):
        """Dibuja la cuadrícula y las celdas vacías."""
        for x in range(self.grid_size):
            for y in range(self.grid_size):
                self.create_rectangle(y * self.cell_size, x * self.cell_size, 
                                      (y + 1) * self.cell_size, (x + 1) * self.cell_size, 
                                      outline=BLACK)

    def object_exists(self, obj_id):
        """Verifica si un objeto con el ID ya existe en el mapa o en las localidades."""
        for row in self.city_map:
            for cell in row:
                if obj_id in cell:
                    return True
        for row in self.localidad_map:
            if obj_id in row:
                return True
        return False

    def add_object(self, obj_id, x, y):
        """Añade un nuevo objeto dinámicamente al mapa, si no existe."""
        if self.object_exists(obj_id):
            print(f"Error: El objeto con ID '{obj_id}' ya existe en el mapa o en las localidades.")
            return

        if 0 <= x < self.grid_size and 0 <= y < self.grid_size:
            self.city_map[x][y].append(obj_id)
            self.taxi_state[obj_id] = 'VERDE'  # Inicialmente, el taxi está en estado VERDE
        else:
            print("Posición fuera de los límites del mapa.")
        self.draw_map()

    def find_position_by_label(self, label, target_map):
        """Encuentra la posición en el mapa de una etiqueta dada."""
        for x in range(self.grid_size):
            for y in range(self.grid_size):
                if target_map[x][y] == label:
                    return (x, y)
        return None

    def move_object(self, obj_id, new_x, new_y):
        """Mueve un objeto en el mapa hacia la nueva posición si está en estado VERDE."""
        current_pos = None

        # Verificar si el taxi está en estado ROJO (parado)
        if self.taxi_state.get(obj_id) == 'ROJO':
            print(f"El taxi {obj_id} está en estado ROJO y no puede moverse.")
            return

        # Buscar el objeto actual en el mapa
        for x in range(self.grid_size):
            for y in range(self.grid_size):
                if obj_id in self.city_map[x][y]:
                    current_pos = (x, y)
                    break

        if current_pos:
            curr_x, curr_y = current_pos
            while (curr_x, curr_y) != (new_x, new_y):
                if curr_x < new_x:
                    curr_x += 1
                elif curr_x > new_x:
                    curr_x -= 1

                if curr_y < new_y:
                    curr_y += 1
                elif curr_y > new_y:
                    curr_y -= 1

                # Mover el taxi sin sobrescribir otros objetos en la misma celda
                self.city_map[current_pos[0]][current_pos[1]].remove(obj_id)  # Eliminar el taxi de la posición anterior
                self.city_map[curr_x][curr_y].append(obj_id)  # Añadir el taxi a la nueva posición
                self.draw_map()  # Redibujar el mapa completo
                time.sleep(0.25)  # Pausa de movimiento
                current_pos = (curr_x, curr_y)
        else:
            print(f"Objeto {obj_id} no encontrado en el mapa.")

    def draw_map(self):
        """Dibuja el mapa y los objetos sobre el canvas."""
        self.delete("all")  # Limpiar el canvas antes de redibujar
        self.draw_grid()

        for x in range(self.grid_size):
            for y in range(self.grid_size):
                if self.localidad_map[x][y]:  # Si es una localidad
                    color = BLUE
                    cell_value = self.localidad_map[x][y]
                elif self.city_map[x][y]:  # Si es un objeto (cliente, taxi)
                    cell_value = ''.join(self.city_map[x][y])
                    color = YELLOW if any(map(str.islower, self.city_map[x][y])) else GREEN
                else:
                    color = WHITE
                    cell_value = ''

                # Dibujar la celda
                self.create_rectangle(y * self.cell_size, x * self.cell_size, 
                                      (y + 1) * self.cell_size, (x + 1) * self.cell_size, 
                                      outline=BLACK, fill=color)

                # Dibujar el texto en el centro de la celda
                if cell_value:
                    self.create_text(y * self.cell_size + self.cell_size // 2, 
                                     x * self.cell_size + self.cell_size // 2, 
                                     text=cell_value, fill=BLACK)

    def move_object_to_localidad(self, obj_id, label):
        """Mueve un objeto hacia una localidad (casilla azul)."""
        position = self.find_position_by_label(label, self.localidad_map)
        if position:
            threading.Thread(target=self.move_object, args=(obj_id, *position)).start()
        else:
            print(f"No se encontró la localidad '{label}' en el mapa.")

    def move_object_to_cliente(self, obj_id, label):
        """Mueve un objeto hacia un cliente (casilla amarilla)."""
        for x in range(self.grid_size):
            for y in range(self.grid_size):
                if label in self.city_map[x][y]:
                    threading.Thread(target=self.move_object, args=(obj_id, x, y)).start()
                    return
        print(f"No se encontró el cliente '{label}' en el mapa.")


# Crear la ventana principal de Tkinter
root = tk.Tk()
root.title("Mapa EasyCab")

# Crear y ejecutar el CityMap en el Canvas
city = CityMap(root, "EC_locations.json", WINDOW_SIZE, WINDOW_SIZE, GRID_SIZE, CELL_SIZE)

# Añadir objetos (taxis y clientes)
city.add_object('1', 1, 1)  # Añadir un taxi
city.add_object('2', 1, 1)  # Añadir otro taxi en la misma casilla

# Mover taxis
city.move_object_to_localidad('1', 'C')  # Mover el taxi '1' a la localidad 'C'
city.move_object_to_cliente('2', 'f')  # Mover el taxi '2' al cliente 'f'
# Iniciar el loop principal de Tkinter
root.mainloop()
