class ADMBloques:

    def __init__(self, total_bloques):
        self.total_bloques = total_bloques
        self.libres = set(range(total_bloques))

    def asigna_bloques(self):
        if not self.libres:
            return None

        id_bloque = self.libres.pop()

        #✅ INVARIANTE CRÍTICO
        if not (0 <= id_bloque < self.total_bloques):
            raise RuntimeError("ID_BLOQUE FUERA DE RANGO")

        return id_bloque

    def liberar_bloque(self, id_bloque):
        if not (0 <= id_bloque < self.total_bloques):
            raise RuntimeError("Liberación inválida de bloque")

        self.libres.add(id_bloque)

    def obten_lista_bloques_libres(self):
        return list(self.libres)

