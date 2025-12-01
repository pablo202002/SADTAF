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

    return socket.gethostname(hostname)


def envia_json(conexion_socket, data):
    mensaje = json.dumps(data).encode("utf-8")
    conexion_socket.sendall(mensaje)


def recibe_json(conexion_socket, tam_buffer=4096):
    data = conexion_socket.recv(tam_buffer)
    
    return json.loads(data.decode(("utf-8")))


#Funciones auxiliares para achivos y data

def verifica_integridad(data_bytes):
    
    return hashlib.sha256(data_bytes).hexdigest()


def carpeta_SS_default(ruta):
    os.makedirs(ruta, exist_ok=True)