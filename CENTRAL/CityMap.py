import pygame 

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
city.run()
