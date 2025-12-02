'''
Clase MAIN
'''

import os
import base64

from adm_almacenamiento import ADMAlmacenamiento
from adm_configuracion import ADMConfiguracion
from adm_distribucion import ADMDistribucion
from auxiliar import carga_configuracion
from adm_metadatos import ADMMetadatos
from adm_archivos import ADMArchivos
from adm_bloques import ADMBloques
from adm_nodos import ADMNodos
from adm_pulso import ADMPulso
from adm_lider import ADMLider
from P2P import P2P

class SAD:
    
    def __init__(self, configuracion:ADMConfiguracion):
        self.configuracion = configuracion

        #info de nodo
        self.id_nodo = configuracion.obten_id_nodo()
        self.host = configuracion.obten_host_nodo()
        self.puerto = configuracion.obten_puerto_nodo()


        #confugura almacenammiento
        tamaño_bloque = configuracion.obten_tamanio_bloque()
        ruta_SS = configuracion.obten_ruta_SS()
        tamaño_SS = configuracion.obten_tamanio_SS()

        bloques_totales = (tamaño_SS * 1024 * 1024) // tamaño_bloque

        self.adm_bloque = ADMBloques(bloques_totales)
        self.adm_almacenamiento = ADMAlmacenamiento(ruta_SS, tamaño_bloque)
        self.adm_archivo = ADMArchivos(self.adm_bloque)


        #metadatos
        self.adm_metadatos = ADMMetadatos()

        #info del cluster
        self.adm_nodos = ADMNodos(self.id_nodo, configuracion.obten_cluster_nodos())


        '''
        RED P2P
        NO MOVERLE A ESTO, JAJA PINCHE CHINGADERA
        '''

        self.P2P = P2P(self.host, self.puerto, self._maneja_mensaje)

        self.P2P.configura_cluster(self.id_nodo, configuracion.obten_cluster_nodos())


        #eleccion de lider
        self.adm_lider = ADMLider(id_nodo=self.id_nodo,
                                   prioridad=configuracion.obten_prioridad_eleccion(),
                                   cluster_nodos=configuracion.obten_cluster_nodos(),
                                   comunicacion=self.P2P)
        
        #distribucion
        self.adm_distribucion = ADMDistribucion(self.id_nodo,
                                                self.adm_nodos,
                                                self.adm_bloque,
                                                self.adm_almacenamiento,
                                                self.P2P)
        
        #pulsos
        self.adm_pulso = ADMPulso (self.id_nodo, 
                                     self.adm_nodos,
                                     self.P2P,
                                     configuracion.obten_intervalo_pulso(),
                                     configuracion.obten_tiempo_espera_pulso())
        

    def iniciar(self):
        self.P2P.empieza_server()
        self.adm_pulso.inicio()
        self.adm_lider.iniciar()

        print(f"Nodo SAD {self.id_nodo} iniciandose en: {self.host}:{self.puerto}")


    #OPERACIONES DE GUI
    def subir_archivo(self, ruta_archivo):
        nombre_archivo = os.path.basename(ruta_archivo)

        with open(ruta_archivo, "rb") as f:
            info = f.read()

        tamaño_bloque = self.configuracion.obten_tamanio_bloque()
        bloques_libres = self.adm_bloque.obten_lista_bloques_libres()
        memoria_restante_bytes = len(bloques_libres) * tamaño_bloque

        if len(info) > memoria_restante_bytes:
            raise Exception(f"Archivo demasiado grande. Memoria restante: {memoria_restante_bytes} bytes")

        fragmentos = self.adm_archivo.fragmentar_archivo(info, tamaño_bloque)
        entradas_fragmentos = []

        for fragmento in fragmentos:
            posicion = self.adm_distribucion.colocar_fragmento(fragmento)

            entradas_fragmentos.append({
                "id_fragmento": fragmento["id_fragmento"],
                "id_nodo": posicion["id_nodo"],
                "id_bloque": posicion["id_bloque"]
            })

        self.adm_metadatos.agrega_archivo(nombre_archivo, entradas_fragmentos)
        # luego de agregar localmente:
        anuncio = {
            "tipo": "ANUNCIO_METADATO",
            "nombre_archivo": nombre_archivo,
            "entradas": entradas_fragmentos
        }

        # anunciar a todos los nodos del cluster (excluyendo este)
        for id_nodo, info in self.adm_nodos.cluster_nodos.items():
            if id_nodo == self.id_nodo:
                continue
            try:
                # usamos envia_mensaje (fire-and-forget) para no bloquear
                self.P2P.envia_mensaje(info["host"], info["puerto"], anuncio)
            except Exception:
                pass


    def descargar_archivo(self, nombre_archivo, ruta_guardado):

        metadato_fragmento = self.adm_metadatos.obten_archivo(nombre_archivo)
        if not metadato_fragmento:
            raise Exception(f"No existe el archivo '{nombre_archivo}' en el sistema distribuido")
        
        fragmentos_bytes = []

        for fragmento in sorted(metadato_fragmento, key=lambda x: x["id_fragmento"]):

            # Recibir base64 del nodo remoto
            info_b64 = self.adm_distribucion._lee_bloque_desde_nodo(
                fragmento["id_nodo"], 
                fragmento["id_bloque"]
            )

            if info_b64 is None:
                raise Exception(f"No se pudo leer el fragmento {fragmento['id_fragmento']} del nodo {fragmento['id_nodo']}")

            # Decodificar base64 → bytes verdaderos
            try:
                info = base64.b64decode(info_b64)
            except Exception as e:
                raise Exception(f"Error al decodificar Base64 desde nodo {fragmento['id_nodo']}: {e}")

            fragmentos_bytes.append(info)

        # Unir todos los fragmentos
        archivo_completo = b"".join(fragmentos_bytes)

        # Guardar archivo reensamblado
        with open(ruta_guardado, "wb") as f:
            f.write(archivo_completo)

        print(f"Archivo '{nombre_archivo}' descargado correctamente en: {ruta_guardado}")

    def eliminar_archivo(self, nombre_archivo):
        fragmentos = self.adm_metadatos.eliminar_archivo(nombre_archivo)

        for fragmento in fragmentos:
            self.adm_distribucion.eliminar_bloque(fragmento["id_nodo"], fragmento["id_bloque"])

    
    def lista_archivos(self):
        return self.adm_metadatos.lista_archivos()
    
    def obten_atributos_archivo(self, nombre_archivo):
        return self.adm_metadatos.obten_atributos_archivos(nombre_archivo)
    
    def obten_tabla_bloques(self):
        return self.adm_metadatos.obten_tabla_bloques()

    def _maneja_mensaje(self, mensaje):
        tipo = mensaje.get("tipo")

        if tipo == "PULSO":
            self.adm_nodos.pulso_recibido(mensaje["id_nodo"])
            return {"ok": True}

        elif tipo == "ALMACENAR_BLOQUE":
            data = base64.b64decode(mensaje["data"])
            self.adm_almacenamiento.escribe_bloque(mensaje["id_bloque"], data)
            return {"ok": True}

        elif tipo == "LEER_BLOQUE":
            data = self.adm_almacenamiento.lee_bloque(mensaje["id_bloque"])
            if data is None:
                return {"error": "bloque inexistente"}
            return base64.b64encode(data).decode("ascii")

        elif tipo == "ELIMINAR_BLOQUE":
            self.adm_bloque.liberar_bloque(mensaje["id_bloque"])
            self.adm_almacenamiento.eliminar_bloque(mensaje["id_bloque"])
            return {"ok": True}

        elif tipo == "ASIGNAR_BLOQUE":
            id_bloque = self.adm_bloque.asigna_bloques()

            if id_bloque is None:
                return None

            return id_bloque


        elif tipo == "ANUNCIO_METADATO":
            self.adm_metadatos.agrega_archivo(
                mensaje["nombre_archivo"],
                mensaje["entradas"]
            )
            return {"ok": True}


        elif tipo == "SOLICITAR_TABLA_BLOQUES":
            tabla = {}

            total = self.adm_bloque.total_bloques
            libres = set(self.adm_bloque.obten_lista_bloques_libres())

            for id_bloque in range(total):
                clave = (self.id_nodo, id_bloque)

                if id_bloque in libres:
                    tabla[id_bloque] = {
                        "estado": "LIBRE",
                        "nombre_archivo": None,
                        "id_fragmento": None
                    }
                else:
                    info = self.adm_metadatos.tabla_bloques.get(clave)
                    tabla[id_bloque] = info if info else {
                        "estado": "OCUPADO",
                        "nombre_archivo": "?",
                        "id_fragmento": "?"
                    }

            return tabla


        # ✅ ESTE ES EL FIX CLAVE
        return {"ok": False, "error": "Tipo de mensaje no reconocido"}



    def obten_tabla_bloques_completa_cluster(self):
        tabla_completa = {}

        # =========================
        # 1. BLOQUES LOCALES OCUPADOS (metadatos)
        # =========================
        for (id_nodo, id_bloque), info in self.adm_metadatos.tabla_bloques.items():
            tabla_completa[(id_nodo, id_bloque)] = info.copy()

        # =========================
        # 2. BLOQUES LOCALES LIBRES
        # =========================
        for id_bloque in self.adm_bloque.obten_lista_bloques_libres():
            clave = (self.id_nodo, id_bloque)
            if clave not in tabla_completa:
                tabla_completa[clave] = {
                    "estado": "LIBRE",
                    "nombre_archivo": None,
                    "id_fragmento": None
                }

        # =========================
        # 3. BLOQUES DE NODOS REMOTOS
        # =========================
        for id_nodo, info_nodo in self.adm_nodos.cluster_nodos.items():
            if id_nodo == self.id_nodo:
                continue

            mensaje = {"tipo": "SOLICITAR_TABLA_BLOQUES"}

            try:
                respuesta = self.P2P.envia_y_recibe_json(
                    info_nodo["host"],
                    info_nodo["puerto"],
                    mensaje
                )

                # ✅ respuesta DEBE ser dict {id_bloque: info}
                if not isinstance(respuesta, dict):
                    continue

                for id_bloque, info in respuesta.items():
                    clave = (id_nodo, id_bloque)
                    tabla_completa[clave] = {
                        "estado": info.get("estado", "LIBRE"),
                        "nombre_archivo": info.get("nombre_archivo"),
                        "id_fragmento": info.get("id_fragmento")
                    }

            except Exception:
                continue

        return tabla_completa



    def es_lider(self):
        return self.adm_lider.es_lider()
    
if __name__ == "__main__":
    from GUI import GUI

    info_configuracion = carga_configuracion("config.json")
    configuracion = ADMConfiguracion(info_configuracion)

    sad = SAD(configuracion)
    sad.iniciar()

    gui = GUI(sad)
    gui.run()
    
                                     
        





