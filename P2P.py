'''
La red p2p, permite la comunicacion entre nodos
'''

import socket
import threading
from auxiliar import envia_json, recibe_json

class P2P:

    def __init__(self, host, puerto, mensajero):
        self.host = host
        self.puerto = puerto
        self.mensajero = mensajero
        self.socket_server = None
        self.ejecutando = False

        self.cluster_nodos = None
        self.id_nodo = None


    def configura_cluster(self, id_nodo, cluster_nodos):
        self.id_nodo = id_nodo
        self.cluster_nodos = cluster_nodos


    def empieza_server(self):
        self.socket_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket_server.bind((self.host, self.puerto))
        self.socket_server.listen()
        self.ejecutando = True

        threading.Thread(target=self._acepta_ciclo, daemon=True).start()
    

    def _acepta_ciclo(self):
        while self.ejecutando:
            conx, _ = self.socket_server.accept()
            threading.Thread(target=self._maneja_conexion, args=(conx,), daemon=True).start()

    
    def _maneja_conexion(self, conx):
        try:
            mensaje = recibe_json(conx)
            self.mensajero(mensaje)
        finally:
            conx.close()


    def envia_mensaje(self, host, puerto, mensaje):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, puerto))
            envia_json(s, mensaje)


    def envia_eleccion(self, id_nodo_objetivo):
        nodo = self.cluster_nodos[id_nodo_objetivo]

        mensaje = {
            "tipo": "ELECCION_LIDER",
            "emisor": self.id_nodo
        }

        self.envia_mensaje(nodo["host"], nodo["puerto"], mensaje)

        return True
    

    def envia_ok(self, id_nodo_objetivo):
        nodo = self.cluster_nodos[id_nodo_objetivo]

        mensaje = {
            "tipo": "ELECCION_OK",
            "emisor": self.id_nodo
        } 

        self.envia_mensaje(nodo["host"], nodo["puerto"], mensaje)

        return True
    
    def envia_recibe_json(self, host, puerto, mensaje, tam_buffer=4096, timeout=5):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(timeout)
            s.connect((host, puerto))
            envia_json(s, mensaje)
            respuesta = recibe_json(s, tam_buffer)
            return respuesta

    

    def envia_anuncio_lider(self, id_nodo_objetivo, id_lider):
        nodo = self.cluster_nodos[id_nodo_objetivo]

        mensaje = {
            "tipo": "ANUNCIO_LIDER",
            "emisor": id_lider
        } 

        self.envia_mensaje(nodo["host"], nodo["puerto"], mensaje)

        return True
    

    def paro(self):
        self.ejecutando = False
        if self.socket_server:
            self.socket_server.close()
            