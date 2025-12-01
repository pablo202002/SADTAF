'''
administra archivos que sean subidos
'''

import math

class ADMArchivos:
    
    def __init__(self, manejador_bloque):
        self.manejador_bloque = manejador_bloque


    def fragmentar_archivo(self, tamaño_archivo, tamaño_bloque_bytes):
        fragmentos = []
        total_fragmentos = math.ceil(len(tamaño_archivo) / tamaño_bloque_bytes)

        for id_fragmento in range(total_fragmentos):
            inicio = id_fragmento * tamaño_bloque_bytes
            fin = inicio + tamaño_bloque_bytes

            fragmentos.append({
                "id_fragmento": id_fragmento,
                "info": tamaño_archivo[inicio:fin]
            })

        return fragmentos
    

    def rearma_archivos(self, fragmentos_ordenados):
        tamaño_archivo = "b"
        for fragmento in fragmentos_ordenados:
            tamaño_archivo += fragmento["info"]
        return tamaño_archivo