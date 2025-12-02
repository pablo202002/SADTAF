# Contenido de adm_bloques.py (CORREGIDO)
import os # 👈 IMPORTACIÓN NECESARIA

class ADMBloques:

    def __init__(self, total_bloques, ruta_ss): # 👈 ACEPTA ruta_ss
        self.total_bloques = total_bloques
        self.libres = set(range(total_bloques))
        self.ruta_ss = ruta_ss # 👈 GUARDAR RUTA

        self._recuperar_bloques_existentes() # 👈 LLAMAR A LA RECUPERACIÓN

    # 🆕 MÉTODO PARA RECUPERAR BLOQUES EN EL DISCO
    def _recuperar_bloques_existentes(self):
        bloques_ocupados = set()

        # Iterar sobre todos los archivos en el shared space
        if os.path.isdir(self.ruta_ss): # Asegurar que la ruta exista
            for nombre_archivo in os.listdir(self.ruta_ss):
                if nombre_archivo.startswith("bloque_") and nombre_archivo.endswith(".bin"):

                    try:
                        # Extraer el ID del bloque del nombre del archivo "bloque_ID.bin"
                        parte_id = nombre_archivo[len("bloque_"): -len(".bin")]
                        id_bloque = int(parte_id)

                        if 0 <= id_bloque < self.total_bloques:
                            bloques_ocupados.add(id_bloque)

                    except ValueError:
                        # Ignorar archivos que no siguen el patrón "bloque_ID.bin"
                        continue

        # Actualizar los bloques libres eliminando los que están ocupados en el disco
        self.libres = self.libres - bloques_ocupados

        print(f"[ADM_BLOQUES] Recuperación completada. {len(bloques_ocupados)} bloques ocupados de {self.total_bloques} posibles.")


    def asigna_bloques(self):
        if not self.libres:
            return None

        id_bloque = self.libres.pop()

        #✅ INVARIANTE CRÍTICO
        if not (0 <= id_bloque < self.total_bloques):
            raise RuntimeError("ID_BLOQUE FUERA DE RANGO")

        return id_bloque

    def liberar_bloque(self, id_bloque):
        if not (0 <= id_bloque < self.total_bloques):
            raise RuntimeError("Liberación inválida de bloque")

        self.libres.add(id_bloque)

    def obten_lista_bloques_libres(self):
        return list(self.libres)