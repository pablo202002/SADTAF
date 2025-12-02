'''
Mantiene estado de nodos de clsuter
usa pulso para marcar activo/caido
'''
import time

class ADMNodos:

    def __init__(self, id_nodo_local, cluster_nodos):
        self.id_nodo_local = id_nodo_local
        self.cluster_nodos = cluster_nodos

        self.ultimo_pulso = {}
        self.nodos_activos = set()
        self.tiempo_inicio = time.time()

        for id_nodo in cluster_nodos:
            self.ultimo_pulso[id_nodo] = None

        #tiempo_actual = time.time()
        #for id_nodo in cluster_nodos:
        #    self.ultimo_pulso[id_nodo] =tiempo_actual
        #    self.nodos_activos.add(id_nodo)

        self.nodos_activos.add(self.id_nodo_local)
        self.ultimo_pulso[self.id_nodo_local] = time.time()


#adm pulsos
    def pulso_recibido(self, id_nodo):
        if id_nodo not in self.cluster_nodos:
            return
        
        if id_nodo not in self.nodos_activos:
            print(f"[] Nodo '{id_nodo}' se ha conectado al cluster")

        self.ultimo_pulso[id_nodo] = time.time()
        self.nodos_activos.add(id_nodo)

    def detectar_fallo(self, tiempo_espera_S):
        ahora = time.time()
        nodos_caidos = []
        
        for id_nodo, ultimo in self.ultimo_pulso.items():
            if id_nodo == self.id_nodo_local:
                continue
            if ultimo is None:
                continue

            if (ahora - ultimo) > tiempo_espera_S:
                if id_nodo in self.nodos_activos:
                    self.nodos_activos.remove(id_nodo)
                    nodos_caidos.append(id_nodo)
                    print(f"[NODO] Nodo {id_nodo} 'se ha marcado como caido' ")

        return nodos_caidos
    

#info nodos
    def obten_nodos_activos(self):
        self.nodos_activos.add(self.id_nodo_local)
        return list(self.nodos_activos)
    
    def esta_nodo_activo(self, id_nodo):
        if id_nodo == self.id_nodo_local:
            return True
        
        return id_nodo in self.nodos_activos
    
    def eliminar_nodo(self, id_nodo):
        if id_nodo == self.id_nodo_local:
            return
        
        self.nodos_activos.discard(id_nodo)
        self.ultimo_pulso.pop(id_nodo)

    def agregar_nodo(self, id_nodo):
        if id_nodo not in self.cluster_nodos:
            return
        
        self.nodos_activos.add(id_nodo)
        self.ultimo_pulso[id_nodo] = time.time()