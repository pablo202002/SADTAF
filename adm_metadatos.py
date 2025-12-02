'''
administra metadatos de archivos
'''

class ADMMetadatos:
    def __init__(self):
        # nombre_archivo -> lista de fragmentos
        self.tabla_archivos = {}

        # (id_nodo, id_bloque) -> info bloque
        self.tabla_bloques = {}

    # =========================
    # ARCHIVOS
    # =========================

    def agrega_archivo(self, nombre, entradas):

        # ✅ CORREGIDO: usar tabla_archivos
        self.tabla_archivos[nombre] = entradas

        # ✅ registrar bloques como OCUPADOS
        for frag in entradas:
            llave = (frag["id_nodo"], frag["id_bloque"])
            self.tabla_bloques[llave] = {
                "estado": "OCUPADO",
                "nombre_archivo": nombre,
                "id_fragmento": frag["id_fragmento"]
            }

    def obten_archivo(self, nombre_archivo):
        return self.tabla_archivos.get(nombre_archivo)

    def lista_archivos(self):
        return list(self.tabla_archivos.keys())

    def eliminar_archivo(self, nombre_archivo):
        fragmentos = self.tabla_archivos.pop(nombre_archivo, [])

        # ✅ liberar bloques asociados
        for fragmento in fragmentos:
            llave = (fragmento["id_nodo"], fragmento["id_bloque"])
            if llave in self.tabla_bloques:
                self.tabla_bloques[llave]["estado"] = "LIBRE"
                self.tabla_bloques[llave]["nombre_archivo"] = None
                self.tabla_bloques[llave]["id_fragmento"] = None

        return fragmentos

    # =========================
    # BLOQUES
    # =========================

    def liberar_bloque(self, id_nodo, id_bloque):
        self.tabla_bloques[(id_nodo, id_bloque)] = {
            "estado": "LIBRE",
            "nombre_archivo": None,
            "id_fragmento": None
        }

    def obten_tabla_bloques(self):
        return self.tabla_bloques

    # =========================
    # GUI / CONSULTAS
    # =========================

    def obten_atributos_archivos(self, nombre_archivo):
        fragmentos = self.tabla_archivos.get(nombre_archivo)

        if not fragmentos:
            return []

        atributos = []
        for fragmento in fragmentos:
            atributos.append({
                "id_fragmento": fragmento["id_fragmento"],
                "id_nodo": fragmento["id_nodo"],
                "id_bloque": fragmento["id_bloque"]
            })

        return atributos
