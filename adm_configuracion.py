'''
Administra configuracion de nodo usando el json
'''

class ADMConfiguracion:

    def __init__(self, info_JSON):
        self.info_JSON = info_JSON


    
    def obten_id_nodo(self):
        return self.info_JSON["nodo"]["id"]
    
    def obten_host_nodo(self):
        return self.info_JSON["nodo"]["host"]
    
    def obten_puerto_nodo(self):
        return self.info_JSON["nodo"]["puerto"]
    


    def obten_ruta_SS(self):
        return self.info_JSON["almacenamiento"]["ruta_SS"]
    
    def obten_tamanio_SS(self):
        return self.info_JSON["almacenamiento"]["tamanio_SS_Mb"]

    def obten_tamanio_bloque(self):
        return self.info_JSON["almacenamiento"]["tamanio_bloque_bytes"]
    

    
    def obten_cluster_nodos(self):
        return self.info_JSON["cluster"]["nodos"]
    
    def obten_intervalo_pulso(self):
        return self.info_JSON["cluster"]["intervalo_pulso"]

    def obten_tiempo_espera_pulso(self):
        return self.info_JSON["cluster"]["tiempo_espera_pulso"]
    


    def obten_prioridad_eleccion(self):
        return self.info_JSON["eleccion"]["prioridad"]
    
    def obten_metodo_eleccion(self):
        return self.info_JSON["eleccion"]["metodo"]