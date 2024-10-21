import pygame
import time

# Constantes para el tamaño de la ventana y las celdas
WINDOW_SIZE = 600
GRID_SIZE = 20
CELL_SIZE = WINDOW_SIZE // GRID_SIZE

# Colores
WHITE = (255, 255, 255)
BLUE = (60, 146, 222)
YELLOW = (255, 255, 0)
GREEN = (89, 224, 31)
RED = (255, 0, 0)
BLACK = (0, 0, 0)

class CityMap:
    def __init__(self, filename):
        """Inicializa el mapa cargando los datos del archivo."""
        self.city_map = [['' for _ in range(GRID_SIZE)] for _ in range(GRID_SIZE)]
        self.filename = filename
        self.load_map()

        # Inicializar pygame
        pygame.init()
        self.screen = pygame.display.set_mode((WINDOW_SIZE, WINDOW_SIZE))
        pygame.display.set_caption("Mapa EasyCab")
        self.font = pygame.font.SysFont(None, 24)  # Fuente para el texto en las celdas

    def load_map(self):
        """Lee el archivo y genera el mapa como una matriz 2D."""
        with open(self.filename, 'r') as file:
            for line in file:
                elements = line.strip().split()
                if len(elements) == 3:
                    id_loc, x, y = elements[0], int(elements[1]), int(elements[2])
                    self.city_map[x][y] = id_loc

    def object_exists(self, obj_id):
        """Verifica si un objeto con el ID ya existe en el mapa."""
        for row in self.city_map:
            if obj_id in row:
                return True
        return False

    def add_object(self, obj_id, x, y):
        """Añade un nuevo objeto dinámicamente al mapa, si no existe."""
        if self.object_exists(obj_id):
            print(f"Error: El objeto con ID '{obj_id}' ya existe en el mapa.")
            return

        if 0 <= x < GRID_SIZE and 0 <= y < GRID_SIZE:
            self.city_map[x][y] = obj_id
        else:
            print("Posición fuera de los límites del mapa.")

    def find_position_by_label(self, label):
        """Encuentra la posición en el mapa de una etiqueta dada."""
        for x in range(GRID_SIZE):
            for y in range(GRID_SIZE):
                if self.city_map[x][y] == label:
                    return (x, y)
        return None

    def move_object(self, obj_id, new_x, new_y):
        """Mueve un objeto en el mapa hacia la nueva posición."""
        current_pos = None
        
        # Buscar el objeto actual en el mapa
        for x in range(GRID_SIZE):
            for y in range(GRID_SIZE):
                if self.city_map[x][y] == obj_id:
                    current_pos = (x, y)
                    break
            if current_pos:
                break
        
        if current_pos:
            # Mover el objeto paso a paso hacia la nueva posición
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

                # Guardar el valor original de la celda
                current_content = self.city_map[curr_x][curr_y]

                # Si es una localidad, no la sobrescribimos pero mostramos el taxi temporalmente
                if current_content.isupper():  # Si es una localidad
                    self.city_map[current_pos[0]][current_pos[1]] = ''  # Limpiar la posición anterior del taxi
                    self.draw_map()  # Dibujar con el taxi sobre la localidad temporalmente
                    pygame.draw.rect(self.screen, GREEN, (curr_y * CELL_SIZE, curr_x * CELL_SIZE, CELL_SIZE, CELL_SIZE))  # Dibujar el taxi encima
                    pygame.display.flip()
                else:
                    # Si no hay localidad, mover el taxi normalmente
                    self.city_map[current_pos[0]][current_pos[1]] = ''  # Limpiar la posición anterior del taxi
                    self.city_map[curr_x][curr_y] = obj_id  # Mover el taxi a la nueva posición
                    self.draw_map()  # Redibujar el mapa para actualizar la ventana
                
                time.sleep(1)  # Esperar un segundo para cada paso
                current_pos = (curr_x, curr_y)

            # Si el taxi llega a una localidad, lo dibujamos pero no lo removemos del mapa
            if self.city_map[new_x][new_y].isupper():
                print(f"Taxi {obj_id} está sobre una localidad: {self.city_map[new_x][new_y]}")
                pygame.draw.rect(self.screen, GREEN, (new_y * CELL_SIZE, new_x * CELL_SIZE, CELL_SIZE, CELL_SIZE))
                pygame.display.flip()
            else:
                self.city_map[new_x][new_y] = obj_id  # Mover el taxi si no hay localidad

        else:
            print(f"Objeto {obj_id} no encontrado en el mapa.")

    def move_object_to_label(self, obj_id, label):
        """Mueve un objeto hacia la posición donde se encuentra la etiqueta dada."""
        position = self.find_position_by_label(label)
        if position:
            new_x, new_y = position
            self.move_object(obj_id, new_x, new_y)
        else:
            print(f"No se encontró la etiqueta '{label}' en el mapa.")

    def draw_map(self):
        """Dibuja el mapa en la ventana usando pygame."""
        self.screen.fill(WHITE)

        for x in range(GRID_SIZE):
            for y in range(GRID_SIZE):
                cell_value = self.city_map[x][y]

                if cell_value == '':
                    color = WHITE
                elif cell_value.isupper():
                    color = BLUE  # Localizaciones (letras mayúsculas)
                elif cell_value.islower():
                    color = YELLOW  # Clientes (letras minúsculas)
                else:
                    color = GREEN if cell_value.isdigit() else RED  # Taxis (número verde o rojo)

                # Dibujar la celda
                pygame.draw.rect(self.screen, color, (y * CELL_SIZE, x * CELL_SIZE, CELL_SIZE, CELL_SIZE))
                pygame.draw.rect(self.screen, BLACK, (y * CELL_SIZE, x * CELL_SIZE, CELL_SIZE, CELL_SIZE), 1)

                # Dibujar el valor de la celda en el centro
                if cell_value:
                    text = self.font.render(cell_value, True, BLACK)
                    text_rect = text.get_rect(center=(y * CELL_SIZE + CELL_SIZE // 2, x * CELL_SIZE + CELL_SIZE // 2))
                    self.screen.blit(text, text_rect)

        pygame.display.flip()

    def run(self):
        """Inicia el loop de pygame."""
        running = True
        while running:
            for event in pygame.event.get():
                if event.type == pygame.QUIT:
                    running = False

            # Dibuja el mapa en cada iteración del bucle
            self.draw_map()

        pygame.quit()

# Crear el objeto CityMap y ejecutar
city = CityMap("mapa.txt")

# Ejemplo de cómo añadir objetos dinámicamente

city.add_object('1', 10, 10)    # Añadir un taxi en la posición (10, 10)
city.add_object('A', 5, 5)      # Añadir una localización en la posición (5, 5)

# Ejemplo de cómo mover un objeto hacia una etiqueta específica
city.move_object_to_label('1', 'C')  # Mover el taxi '1' a la posición de 'C'
city.move_object_to_label('1', 'a')
city.run()
