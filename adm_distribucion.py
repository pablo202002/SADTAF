'''
DEcide donde se alamacena cada fragmento, administra lectura/escritura de bloques
'''

import random

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
 
        self.comunicacion.enviar(id_nodo, mensaje)

        return id_bloque


    def _lee_bloque_desde_nodo(self, id_nodo, id_bloque):
        if id_nodo == self.id_nodo:
            info = self.manejador_almacenamiento.lee_bloque(id_bloque)
            return info
        

        mensaje = {
            "tipo": "LEER_BLOQUE",
            "id_bloque": id_bloque
        }

        respuesta = self.comunicacion.envia_recibe_json(
            self.manejador_nodo.cluster_nodos[id_nodo]["host"],
            self.manejador_nodo.cluster_nodos[id_nodo]["puerto"],
            mensaje
        )

        return respuesta


    def eliminar_bloque(self, id_nodo, id_bloque):
        if id_nodo == self.id_nodo:
            self.manejador_bloque.liberar_bloque(id_bloque)
            self.manejador_almacenamiento.eliminar_bloque(id_bloque)
            return
        
        mensaje = {
            "tipo": "ELIMINAR_BLOQUE",
            "id_bloque": id_bloque 
        }

        self.comunicacion.enviar(id_nodo, mensaje)