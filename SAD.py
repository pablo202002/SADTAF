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

# Contenido de SAD.py (CORREGIDO)

class SAD:

    def __init__(self, configuracion:ADMConfiguracion):
        self.configuracion = configuracion

        # 1. Información del nodo: ¡ASIGNAR ESTO PRIMERO!
        self.id_nodo = configuracion.obten_id_nodo()
        self.host = configuracion.obten_host_nodo()
        self.puerto = configuracion.obten_puerto_nodo()


        # 2. Configura almacenamiento (usa la configuración, no self.id_nodo)
        tamaño_bloque = configuracion.obten_tamanio_bloque()
        ruta_SS = configuracion.obten_ruta_SS()
        tamaño_SS = configuracion.obten_tamanio_SS()

        bloques_totales = (tamaño_SS * 1024 * 1024) // tamaño_bloque

        # NOTA: Asumiendo que ya aplicaste la corrección de ADMBloques para la ruta_SS
        self.adm_bloque = ADMBloques(bloques_totales, ruta_SS)
        self.adm_almacenamiento = ADMAlmacenamiento(ruta_SS, tamaño_bloque)
        self.adm_archivo = ADMArchivos(self.adm_bloque)


        # 3. Metadatos
        self.adm_metadatos = ADMMetadatos()

        # 4. Info del cluster: ¡Ahora self.id_nodo ya existe!
        self.adm_nodos = ADMNodos(self.id_nodo, configuracion.obten_cluster_nodos())


        # ... (El resto del constructor va aquí, manteniendo el orden para ADMLider, ADMPulso, etc.)
        self.P2P = P2P(self.host, self.puerto, self._maneja_mensaje)

        self.P2P.configura_cluster(self.id_nodo, configuracion.obten_cluster_nodos())


        # eleccion de lider
        self.adm_lider = ADMLider(id_nodo=self.id_nodo,
                                   prioridad=configuracion.obten_prioridad_eleccion(),
                                   cluster_nodos=configuracion.obten_cluster_nodos(),
                                   comunicacion=self.P2P)

        # distribucion
        self.adm_distribucion = ADMDistribucion(self.id_nodo,
                                                self.adm_nodos,
                                                self.adm_bloque,
                                                self.adm_almacenamiento,
                                                self.P2P)

        # pulsos
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

        num_copias = self.configuracion.info_JSON.get("copias", {}).get("numbero_de_copias", 1)
        nodos_activos = self.adm_nodos.obten_nodos_activos()

        for fragmento in fragmentos:
            nodos_usados = set()
            for _ in range(num_copias):
                # Elegir nodo disponible distinto
                nodos_disponibles = [n for n in nodos_activos if n not in nodos_usados]
                if not nodos_disponibles:
                    break  # no quedan nodos para copias adicionales
                id_nodo = self.adm_distribucion._elige_nodo_para_fragmento_custom(nodos_disponibles)
                nodos_usados.add(id_nodo)

                data_bytes = fragmento["info"]

                if id_nodo == self.id_nodo:
                    id_bloque = self.adm_bloque.asigna_bloques()
                    if id_bloque is None:
                        raise Exception("Nodo local sin bloques libres")
                    self.adm_almacenamiento.escribe_bloque(id_bloque, data_bytes)
                else:
                    nodo = self.adm_nodos.cluster_nodos[id_nodo]
                    id_bloque = self.P2P.envia_y_recibe_json(
                        nodo["host"],
                        nodo["puerto"],
                        {"tipo": "ASIGNAR_BLOQUE"}
                    )
                    mensaje = {
                        "tipo": "ALMACENAR_BLOQUE",
                        "id_bloque": id_bloque,
                        "data": base64.b64encode(data_bytes).decode("ascii")
                    }
                    self.P2P.envia_mensaje(nodo["host"], nodo["puerto"], mensaje)

                entradas_fragmentos.append({
                    "id_fragmento": fragmento["id_fragmento"],
                    "id_nodo": id_nodo,
                    "id_bloque": id_bloque
                })

        # Guardar metadatos
        self.adm_metadatos.agrega_archivo(nombre_archivo, entradas_fragmentos)

        # Anunciar a otros nodos
        anuncio = {
            "tipo": "ANUNCIO_METADATO",
            "nombre_archivo": nombre_archivo,
            "entradas": entradas_fragmentos
        }

        for id_nodo, info in self.adm_nodos.cluster_nodos.items():
            if id_nodo == self.id_nodo:
                continue
            try:
                self.P2P.envia_mensaje(info["host"], info["puerto"], anuncio)
            except Exception:
                pass



    def descargar_archivo(self, nombre_archivo, ruta_guardado):
        metadato_fragmento = self.adm_metadatos.obten_archivo(nombre_archivo)
        if not metadato_fragmento:
            raise Exception(f"No existe el archivo '{nombre_archivo}' en el sistema distribuido")
        
        fragmentos_bytes = []

        # Agrupar fragmentos por id_fragmento (para varias copias)
        from collections import defaultdict
        fragmentos_por_id = defaultdict(list)
        for frag in metadato_fragmento:
            fragmentos_por_id[frag["id_fragmento"]].append(frag)

        for id_frag, copias in sorted(fragmentos_por_id.items()):
            data = None

            # Intentar cada copia hasta que alguna funcione
            for frag in copias:
                id_nodo = frag["id_nodo"]
                id_bloque = frag["id_bloque"]

                if not self.adm_nodos.esta_nodo_activo(id_nodo):
                    continue  # nodo caído, intentar otra copia

                respuesta = self.adm_distribucion._lee_bloque_desde_nodo(id_nodo, id_bloque)

                if respuesta is not None:
                    # Normalizar bytes
                    if isinstance(respuesta, bytes):
                        data = respuesta
                    else:
                        try:
                            data = base64.b64decode(respuesta)
                        except Exception:
                            continue
                    break  # ya encontramos una copia válida

            if data is None:
                raise Exception(f"No se pudo leer el fragmento {id_frag} de ningún nodo activo")

            fragmentos_bytes.append(data)

        # Unir todos los fragmentos
        archivo_completo = b"".join(fragmentos_bytes)

        # Guardar archivo reensamblado
        with open(ruta_guardado, "wb") as f:
            f.write(archivo_completo)

        print(f"Archivo '{nombre_archivo}' descargado correctamente en: {ruta_guardado}")


    def eliminar_archivo(self, nombre_archivo):
        # 1. eliminar metadato local y obtener todas las copias
        fragmentos = self.adm_metadatos.eliminar_archivo(nombre_archivo)

        # 2. eliminar bloques físicos de cada copia en nodos activos
        from collections import defaultdict
        fragmentos_por_id = defaultdict(list)
        for frag in fragmentos:
            fragmentos_por_id[frag["id_fragmento"]].append(frag)

        for id_frag, copias in fragmentos_por_id.items():
            for frag in copias:
                id_nodo = frag["id_nodo"]
                id_bloque = frag["id_bloque"]

                # solo intentar si el nodo está activo
                if self.adm_nodos.esta_nodo_activo(id_nodo):
                    self.adm_distribucion.eliminar_bloque(id_nodo, id_bloque)

        # 3. anunciar eliminación al cluster
        mensaje = {
            "tipo": "ELIMINAR_METADATO",
            "nombre_archivo": nombre_archivo
        }

        for id_nodo, info in self.adm_nodos.cluster_nodos.items():
            if id_nodo == self.id_nodo:
                continue
            try:
                self.P2P.envia_mensaje(info["host"], info["puerto"], mensaje)
            except Exception:
                pass
    
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
        
        elif tipo == "ELIMINAR_METADATO":
            nombre = mensaje["nombre_archivo"]
            self.adm_metadatos.eliminar_archivo(nombre)
            return {"ok": True}



        elif tipo == "SOLICITAR_TABLA_BLOQUES":
            # Construir tabla completa del nodo local: todos los bloques [0 .. total-1]
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
                    tabla[id_bloque] = info.copy() if info else {
                        "estado": "OCUPADO",
                        "nombre_archivo": None,
                        "id_fragmento": None
                    }

            # Devolver dict con keys numéricas
            return tabla



        # ✅ ESTE ES EL FIX CLAVE
        return {"ok": False, "error": "Tipo de mensaje no reconocido"}



    def obten_tabla_bloques_completa_cluster(self):
        tabla_completa = {}

        # =========================
        # 1. BLOQUES LOCALES (TODOS)
        # =========================
        total_local = self.adm_bloque.total_bloques
        libres_locales = set(self.adm_bloque.obten_lista_bloques_libres())

        for id_bloque in range(total_local):
            clave = (self.id_nodo, id_bloque)

            if id_bloque in libres_locales:
                tabla_completa[clave] = {
                    "estado": "LIBRE",
                    "nombre_archivo": None,
                    "id_fragmento": None
                }
            else:
                info = self.adm_metadatos.tabla_bloques.get(clave)
                tabla_completa[clave] = info.copy() if info else {
                    "estado": "OCUPADO",
                    "nombre_archivo": None,
                    "id_fragmento": None
                }

        # =========================
        # 2. BLOQUES DE NODOS REMOTOS (TODOS)
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

                # ✅ respuesta debe ser dict { id_bloque (int o str): info }
                if not isinstance(respuesta, dict):
                    continue

                for id_bloque_raw, info in respuesta.items():
                    # 🔒 blindaje fuerte
                    try:
                        id_bloque = int(id_bloque_raw)
                    except (ValueError, TypeError):
                        continue

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
    
                                     
        





