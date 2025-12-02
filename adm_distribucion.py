"""
Decide dónde se almacena cada fragmento.
El nodo dueño del disco es el ÚNICO que asigna sus bloques.
"""

import random
import base64


class ADMDistribucion:

    def __init__(self, id_nodo, manejador_nodo, manejador_bloque,
                 manejador_almacenamiento, comunicacion):

        self.id_nodo = id_nodo
        self.manejador_nodo = manejador_nodo
        self.manejador_bloque = manejador_bloque
        self.manejador_almacenamiento = manejador_almacenamiento
        self.comunicacion = comunicacion


    # ============================
    # COLOCACIÓN DE FRAGMENTOS
    # ============================
    def colocar_fragmento(self, fragmento):
        # elegir nodo destino
        id_nodo = self._elige_nodo_para_fragmento()
        data_bytes = fragmento["info"]

        # 🟩 CASO 1: almacenamiento LOCAL
        if id_nodo == self.id_nodo:
            id_bloque = self.manejador_bloque.asigna_bloques()
            if id_bloque is None:
                raise Exception("Nodo local sin bloques libres")

            self.manejador_almacenamiento.escribe_bloque(id_bloque, data_bytes)

        # 🟦 CASO 2: almacenamiento REMOTO
        else:
            nodo = self.manejador_nodo.cluster_nodos[id_nodo]

            id_bloque = self.comunicacion.envia_y_recibe_json(
                nodo["host"],
                nodo["puerto"],
                {"tipo": "ASIGNAR_BLOQUE"}
            )

            # 🔒 blindaje CRÍTICO
            if not isinstance(id_bloque, int):
                raise Exception(f"ID de bloque inválido recibido: {id_bloque}")

            mensaje = {
                "tipo": "ALMACENAR_BLOQUE",
                "id_bloque": id_bloque,
                "data": base64.b64encode(data_bytes).decode("ascii")
            }

            self.comunicacion.envia_mensaje(
                nodo["host"],
                nodo["puerto"],
                mensaje
            )

        return {
            "id_fragmento": fragmento["id_fragmento"],
            "id_nodo": id_nodo,
            "id_bloque": id_bloque
        }


    # ============================
    # ELECCIÓN DE NODO
    # ============================
    def _elige_nodo_para_fragmento(self):
        nodos_activos = self.manejador_nodo.obten_nodos_activos()

        if not nodos_activos:
            raise Exception("No hay nodos activos para almacenar")

        return random.choice(nodos_activos)


    # ============================
    # ESCRITURA REMOTA
    # ============================
    def _escribe_bloque_remoto(self, id_nodo, id_bloque, data_bytes):
        info_nodo = self.manejador_nodo.cluster_nodos[id_nodo]

        mensaje = {
            "tipo": "ALMACENAR_BLOQUE",
            "id_bloque": id_bloque,
            "data": base64.b64encode(data_bytes).decode("ascii")
        }

        respuesta = self.comunicacion.envia_y_recibe_json(
            info_nodo["host"],
            info_nodo["puerto"],
            mensaje
        )

        if not respuesta or not respuesta.get("ok"):
            raise Exception(
                f"No se pudo escribir bloque {id_bloque} en nodo {id_nodo}"
            )


    # ============================
    # LECTURA DE BLOQUES
    # ============================
    def _lee_bloque_desde_nodo(self, id_nodo, id_bloque):

        info_nodo = self.manejador_nodo.cluster_nodos.get(id_nodo)
        if not info_nodo:
            return None

        mensaje = {
            "tipo": "LEER_BLOQUE",
            "id_bloque": id_bloque
        }

        respuesta = self.comunicacion.envia_y_recibe_json(
            info_nodo["host"],
            info_nodo["puerto"],
            mensaje
        )

        if not isinstance(respuesta, str):
            return None

        try:
            return base64.b64decode(respuesta)
        except Exception:
            return None


    # ============================
    # ELIMINACIÓN
    # ============================
    def eliminar_bloque(self, id_nodo, id_bloque):

        if id_nodo == self.id_nodo:
            self.manejador_bloque.liberar_bloque(id_bloque)
            self.manejador_almacenamiento.eliminar_bloque(id_bloque)
            return True

        info_nodo = self.manejador_nodo.cluster_nodos.get(id_nodo)
        if not info_nodo:
            return False

        mensaje = {
            "tipo": "ELIMINAR_BLOQUE",
            "id_bloque": id_bloque
        }

        try:
            self.comunicacion.envia_mensaje(
                info_nodo["host"],
                info_nodo["puerto"],
                mensaje
            )
            return True
        except Exception:
            return False
