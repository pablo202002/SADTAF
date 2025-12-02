'''
envia y monitorea pulsos entre los nodos del cluster
'''

import threading
import time

class ADMPulso:
    def __init__(self, id_nodo, manejador_nodo, comunicacion, intervalos_s=2, tiempo_espera_s=5):
        self.id_nodo = id_nodo
        self.manejador_nodo = manejador_nodo
        self.comunicacion = comunicacion
        self.intervalo = intervalos_s
        self.tiempo_espera = tiempo_espera_s

        self.ejecutando = False


    def inicio(self):
        if self.ejecutando:
            return
            
        self.ejecutando = True

        threading.Thread(target=self._enviar_ciclo, daemon=True).start()

        threading.Thread(target=self._monitor_ciclo, daemon=True).start()

    
    def paro(self):
        self.ejecutando = False


    def _enviar_ciclo(self):
        while self.ejecutando:
            for id_nodo, info_nodo in self.manejador_nodo.cluster_nodos.items():
                if id_nodo == self.id_nodo:
                    continue

                try:
                    print(f"[PULSO] Enviando pulso desde {self.id_nodo} a {id_nodo}")
                    self.comunicacion.envia_mensaje(
                        info_nodo["host"], 
                        info_nodo["puerto"], 
                        {
                            "tipo": "PULSO",
                            "id_nodo": self.id_nodo
                        }

                    )
                except Exception:
                    pass

            time.sleep(self.intervalo)

    
    def _monitor_ciclo(self):
        while self.ejecutando:
            nodos_fallidos = self.manejador_nodo.detectar_fallo(self.tiempo_espera)

            if nodos_fallidos:
                print(f"[pulso] se cayeron nodos, levantalos we")

            time.sleep(self.intervalo)
            #El programa duerme mas que yo XDDDD