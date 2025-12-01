'''
Administra la asignacion y liberacion de bloqes locales
Simula una tabla de bloques
'''

class ADMBloques:

    def __init__(self, total_de_bloques):

        self.total_de_bloques = total_de_bloques
        self.tabla_de_bloques = {}

        for id_bloque in range(total_de_bloques):
            self.tabla_de_bloques[id_bloque] = "LIBRE"


    def asignar_bloques(self):
        for id_bloque, estado in self.tabla_de_bloques.items():
            if estado == "LIBRE":
                self.tabla_de_bloques[id_bloque] = "OCUPAO"
                return id_bloque
        return None
        
    def liberar_bloque(self, id_bloque):
        if id_bloque in self.tabla_de_bloques:
            self.tabla_de_bloques = "LIBRE"

    
    def obten_lista_bloques_libres(self):
        return [id_bloque for id_bloque, estado in self.tabla_de_bloques.items() if estado == "LIBRE"]


    def obten_lista_bloques_ocupados(self):
        return [id_bloque for id_bloque, estado in self.tabla_de_bloques.items() if estado == "OCUPAO"]
    

    def obten_tabla_bloques(self):
        return self.tabla_de_bloques