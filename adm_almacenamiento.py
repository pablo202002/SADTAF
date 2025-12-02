'''
maneja almacenamiento real de nodos
'''

import os

class ADMAlmacenamiento:
    def __init__(self, ruta_ss, tamaño_bloque):
        self.ruta_ss = ruta_ss
        self.tamaño_bloque = tamaño_bloque
        os.makedirs(ruta_ss, exist_ok=True)

    def _ruta_bloque(self, id_bloque):
        return os.path.join(self.ruta_ss, f"bloque_{id_bloque}.bin")

    def escribe_bloque(self, id_bloque, data):
        if len(data) > self.tamaño_bloque:
            raise ValueError("Bloque demasiado grande")

        with open(self._ruta_bloque(id_bloque), "wb") as f:
            f.write(data)

    def lee_bloque(self, id_bloque):
        ruta = self._ruta_bloque(id_bloque)
        if not os.path.exists(ruta):
            return None
        with open(ruta, "rb") as f:
            return f.read()

    def eliminar_bloque(self, id_bloque):
        ruta = self._ruta_bloque(id_bloque)
        if os.path.exists(ruta):
            os.remove(ruta)
