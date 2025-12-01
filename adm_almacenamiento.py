'''
maneja almacenamiento real de nodos
'''

import os

class ADMAlmacenamiento:
    
    def __init__(self, ruta_SS, tamaño_bloques):
        self.ruta_SS = ruta_SS
        self.tamaño_bloques = tamaño_bloques

        os.makedirs(self.ruta_SS, exist_ok=True)

    def escribe_bloque(self, id_bloque, info):
        ruta = self._ruta_bloque(id_bloque)
        with open(ruta, "wb") as f:
            f.write(info)

    def lee_bloque(self, id_bloque):
        ruta = self._ruta_bloque(id_bloque)
        with open(ruta, "rb") as f:
            return f.read()
        
    def eliminar_bloque(self, id_bloque):
        ruta = self._ruta_bloque(id_bloque)
        if os.path.exists(ruta):
            os.remove(ruta)

    def _ruta_bloque(self, id_bloque):
        return os.path.join(self.ruta_SS, f"bloque_{id_bloque}.bin")
    