'''
administra metadatos de archivos
'''

class ADMMetadatos:
    def __init__(self):
        self.tabla_archivos = {}
        self.tabla_bloques = {}

    def agrega_archivo(self, nombre_archivo, entradas_fragmentos):
        self.tabla_archivos[nombre_archivo] = entradas_fragmentos

        for fragmento in entradas_fragmentos:
            llave = (fragmento["id_nodo"], fragmento["id_bloque"])
            self.tabla_bloques[llave] = {
                "estado": "OCUPAO",
                "nombre_archivo": nombre_archivo,
                "id_fragmento":fragmento["id_fragmento"]
            }

    def obten_archivo(self, nombre_archivo):
        return self.tabla_archivos.get(nombre_archivo)
    
    def eliminar_archivo(self, nombre_archivo):
        fragmentos = self.tabla_archivos.pop(nombre_archivo, [])

        for fragmento in fragmentos:
            llave = (fragmento["id_nodo"], fragmento["id_bloque"])
            if llave in self.tabla_bloques:
                del self.tabla_bloques[llave]

        return fragmentos
    
    def lista_archivos(self):
        return list(self.tabla_archivos.keys())
    
    def liberar_bloque(self, id_nodo, id_bloque):
        self.tabla_bloques[(id_nodo, id_bloque)] = {
            "estado": "LIBRE",
            "nombre_archivo": None,
            "id_fragmento": None
        }

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
    
    def obten_tabal_bloques(self):
        return self.tabla_bloques
    