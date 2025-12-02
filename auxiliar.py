'''
En este archivo hay funciones auxiliares varias.
'''

import json
import os
import socket
import hashlib

def carga_configuracion(ruta_config):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    ruta_completa = os.path.join(base_dir, ruta_config)
    
    with open(ruta_completa, "r") as f:
        return json.load(f)
    
#Funciones auxiliares para red

def obten_IP_local():
    hostname = socket.gethostname()

    return socket.gethostbyname(hostname)


def envia_json(conexion, mensaje_dict):
    """
    Envía un JSON por socket, terminando siempre con un '\n'
    para evitar mensajes incompletos.
    """
    data = (json.dumps(mensaje_dict) + "\n").encode("utf-8")
    conexion.sendall(data)



def recibe_json(conexion, tam_buffer=4096):
    buffer = ""

    while True:
        chunk = conexion.recv(tam_buffer)
        if not chunk:
            if buffer.strip():
                return json.loads(buffer)
            raise Exception("Conexión cerrada sin datos")

        buffer += chunk.decode("utf-8")

        if "\n" in buffer:
            linea = buffer.split("\n")[0]
            return json.loads(linea)



#Funciones auxiliares para achivos y data

def verifica_integridad(data_bytes):
    
    return hashlib.sha256(data_bytes).hexdigest()


def carpeta_SS_default(ruta):
    os.makedirs(ruta, exist_ok=True)