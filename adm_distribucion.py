'''
DEcide donde se alamacena cada fragmento, administra lectura/escritura de bloques
'''

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
        nodo_objetivo = self._elige_nodos_para_fragmentos()

        if nodo_objetivo == self.id_nodo:
            id_bloque = self.manejador_bloque.asignar_bloques()
            self.manejador_almacenamiento.escribe_bloque(id_bloque, fragmento["info"])
        else:
            id_bloque = self._almacena_bloque_remoto(nodo_objetivo, fragmento["info"])

        return {
            "id_nodo": nodo_objetivo,
            "id_bloque": id_bloque
        }        
            

    def _elige_nodo_para_fragmento(self):
        nodos_activos = self.manejador_nodo.obten_nodos_activos()

        if not nodos_activos:
            raise Exception("No hay nodos activos para alamacenar")
        
        return random.choice(nodos_activos)


    def _almacena_bloque_remoto(self, id_nodo, info):
        id_bloque = random.randint(0,10_000_000)
        
        mensaje = {
            "tipo": "ALMACENAR_BLOQUE",
            "id_bloque": id_bloque,
            "info": info.decode("latin1")
        }
 
        info = self.comunicacion.cluster_nodos[id_nodo]
        self.comunicacion.envia_mensaje(info["host"], info["puerto"], mensaje)

        return id_bloque


    def _lee_bloque_desde_nodo(self, id_nodo, id_bloque):

        # Obtener datos del nodo desde el manejador de nodos
        info_nodo = self.manejador_nodo.cluster_nodos.get(id_nodo)

        if not info_nodo:
            print(f"[DISTRIBUCION] Nodo {id_nodo} no encontrado")
            return None

        mensaje = {
            "tipo": "LEER_BLOQUE",
            "id_bloque": id_bloque
        }

        try:
            # Usar tu sistema de comunicación P2P real
            respuesta = self.comunicacion.envia_y_recibe_json(
                info_nodo["host"],
                info_nodo["puerto"],
                mensaje
            )

            if not respuesta:
                print(f"[DISTRIBUCION] Nodo {id_nodo} respondió vacío para bloque {id_bloque}")
                return None

            # Validar que sea cadena base64
            if not isinstance(respuesta, str):
                print(f"[DISTRIBUCION] Nodo {id_nodo} devolvió algo que no es string Base64")
                return None

            import base64
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
            return
        
        mensaje = {
            "tipo": "ELIMINAR_BLOQUE",
            "id_bloque": id_bloque 
        }

        info = self.comunicacion.cluster_nodos[id_nodo]
        self.comunicacion.envia_mensaje(info["host"], info["puerto"], mensaje)
        
        print(f"[DISTRIBUCION] No se pudo eliminar bloque remoto {id_bloque} en nodo {id_nodo}")
        return False