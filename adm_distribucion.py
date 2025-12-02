# adm_distribucion (parche)
import random
import base64

class ADMDistribucion:

    def __init__(self, id_node, manejador_nodo, manejador_bloque, manejador_almacenamiento, comunicacion):
        self.id_nodo = id_node
        self.manejador_nodo = manejador_nodo
        self.manejador_bloque = manejador_bloque
        self.manejador_almacenamiento = manejador_almacenamiento
        self.comunicacion = comunicacion

    def colocar_fragmento(self, fragmento):
        nodo_objetivo = self._elige_nodo_para_fragmento()

        if nodo_objetivo == self.id_nodo:
            id_bloque = self.manejador_bloque.asignar_bloques()
            # escribe bytes localmente
            self.manejador_almacenamiento.escribe_bloque(id_bloque, fragmento["info"])
        else:
            # almacena remotamente en formato base64 y espera confirmación
            id_bloque = self._almacena_bloque_remoto(nodo_objetivo, fragmento["info"])

        return {
            "id_nodo": nodo_objetivo,
            "id_bloque": id_bloque
        }

    def _elige_nodo_para_fragmento(self):
        # elegir entre nodos activos (incluye local)
        nodos_activos = self.manejador_nodo.obten_nodos_activos()
        if not nodos_activos:
            raise Exception("No hay nodos activos para almacenar")
        return random.choice(nodos_activos)

    def _almacena_bloque_remoto(self, id_nodo, info_bytes):
        # Generar id de bloque remoto (el nodo receptor debe aceptarlo o ignorarlo)
        id_bloque = random.randint(0, 10_000_000)

        mensaje = {
            "tipo": "ALMACENAR_BLOQUE",
            "id_bloque": id_bloque,
            # enviar en base64 ASCII
            "data": base64.b64encode(info_bytes).decode("ascii")
        }

        info = self.comunicacion.cluster_nodos.get(id_nodo)
        if not info:
            raise Exception(f"Nodo remoto {id_nodo} no encontrado en cluster")

        # usamos envia_y_recibe_json para intentar recibir confirmación
        try:
            respuesta = self.comunicacion.envia_y_recibe_json(info["host"], info["puerto"], mensaje)
            # si queremos, validar respuesta == {"ok": True}
        except Exception:
            # no bloquear si falla la comunicación; el id_bloque retornado queda como referencia
            pass

        return id_bloque

    def _lee_bloque_desde_nodo(self, id_nodo, id_bloque):
        info_nodo = self.manejador_nodo.cluster_nodos.get(id_nodo)
        if not info_nodo:
            print(f"[DISTRIBUCION] Nodo {id_nodo} no encontrado")
            return None

        mensaje = {
            "tipo": "LEER_BLOQUE",
            "id_bloque": id_bloque
        }

        try:
            respuesta = self.comunicacion.envia_y_recibe_json(
                info_nodo["host"],
                info_nodo["puerto"],
                mensaje
            )

            if not respuesta:
                print(f"[DISTRIBUCION] Nodo {id_nodo} respondió vacío para bloque {id_bloque}")
                return None

            # respuesta debe ser string base64
            if not isinstance(respuesta, str):
                print(f"[DISTRIBUCION] Nodo {id_nodo} devolvió no-string para bloque {id_bloque}")
                return None

            # valida base64
            try:
                base64.b64decode(respuesta)
            except Exception:
                print(f"[DISTRIBUCION] Respuesta NO válida Base64 desde nodo {id_nodo}")
                return None

            return respuesta

        except Exception as e:
            print(f"[DISTRIBUCION] Error consultando nodo {id_nodo}: {e}")
            return None

    def eliminar_bloque(self, id_nodo, id_bloque):
        if id_nodo == self.id_nodo:
            self.manejador_bloque.liberar_bloque(id_bloque)
            self.manejador_almacenamiento.eliminar_bloque(id_bloque)
            return True

        mensaje = {
            "tipo": "ELIMINAR_BLOQUE",
            "id_bloque": id_bloque
        }

        info = self.comunicacion.cluster_nodos.get(id_nodo)
        if not info:
            print(f"[DISTRIBUCION] Nodo remoto {id_nodo} no encontrado")
            return False

        try:
            self.comunicacion.envia_mensaje(info["host"], info["puerto"], mensaje)
            return True
        except Exception:
            print(f"[DISTRIBUCION] No se pudo eliminar bloque remoto {id_bloque} en nodo {id_nodo}")
            return False
