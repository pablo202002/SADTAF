'''
Administra operaciones para seleccionar al nodo lider
'''

class ADMLider:
    
    def __init__(self, id_nodo, prioridad, cluster_nodos, comunicacion):
        self.id_nodo = id_nodo
        self.prioridad = prioridad
        self.cluster_nodos = cluster_nodos
        self.p2p = comunicacion
        self.id_lider = None
        self.es_lider_flag = False


    def iniciar(self):
        if len(self.cluster_nodos) == 1:
            self.id_lider = self.id_nodo
            self.es_lider = True
            print(f"{self.id_nodo} Yo soy el lida")
            return
        
        self.iniciar_eleccion()

    
    def iniciar_eleccion(self):
        nodos_prioritarios = self._nodos_prioritarios()

        if not nodos_prioritarios:
            self._convertir_lider()
            return
        
        respuestas = 0

        for id_nodo in nodos_prioritarios:
            if id_nodo == self.id_nodo:
                continue

            if self.p2p.envia_eleccion(id_nodo):
                respuestas += 1

        if respuestas == 0:
            self._convertir_lider()


    def _convertir_lider(self):
        self.id_lider = self.id_nodo
        self.es_lider_flag = True

        print(f"{self.id_nodo} Ahora Yo soy lider")

        for id_nodo in self.cluster_nodos:
            if id_nodo != self.id_nodo:
                self.p2p.envia_anuncio_lider(id_nodo, self.id_nodo)


    def cuando_mensaje_eleccion(self, id_emisor):
        prioridad_emisor = self.cluster_nodos[id_emisor]["prioridad"]

        if prioridad_emisor < self.prioridad:
            self.p2p.envia_ok(id_emisor)
            self.iniciar_eleccion(self)

    def _nodos_prioritarios(self):
        orden = []

        for id_nodo, info in self.cluster_nodos.items():
            if id_nodo != self.cluster_nodos and info["prioridad"] > self.prioridad:
                orden.append(id_nodo)

        return orden
    
    def es_lider(self):
        return self.es_lider_flag

    
        