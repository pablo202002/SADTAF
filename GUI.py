import tkinter as tk
from tkinter import filedialog, messagebox
from tkinter import ttk, Toplevel

import os


class GUI:
    """
    Interfaz gráfica del sistema de archivos distribuido.
    Usa la interfaz pública del objeto SAD recibido.
    """

    def __init__(self, SAD):
        self.SAD = SAD

        self.root = tk.Tk()
        self.root.title("Distributed File System")

        self._build_ui()

    # =========================
    # UI SETUP
    # =========================

    def _build_ui(self):
        frame = tk.Frame(self.root, padx=10, pady=10)
        frame.pack(fill="both", expand=False)

        # Lista de archivos
        self.lista_archivos = tk.Listbox(frame, width=40)
        self.lista_archivos.pack(fill="x")

        refresh_btn = tk.Button(frame, text="Actualizar archivos", command=self.recarga_archivos)
        refresh_btn.pack(pady=5, fill="x")

        # Botones
        tk.Button(frame, text="1) Subir archivo", command=self.upload_file).pack(fill="x")
        tk.Button(frame, text="2) Descargar archivo", command=self.download_file).pack(fill="x")
        tk.Button(frame, text="3) Eliminar archivo", command=self.delete_file).pack(fill="x")
        tk.Button(frame, text="4) Atributos", command=self.show_attributes).pack(fill="x")
        tk.Button(frame, text="5) Tabla de bloques", command=self.show_block_table).pack(fill="x")

    # =========================
    # GUI ACTIONS
    # =========================

    def recarga_archivos(self):
        self.lista_archivos.delete(0, tk.END)
        files = []
        try:
            files = self.SAD.lista_archivos()
        except Exception as e:
            print("Error al listar archivos:", e)

        for f in files:
            self.lista_archivos.insert(tk.END, f)

    def get_selected_file(self):
        selection = self.lista_archivos.curselection()
        if not selection:
            return None
        return self.lista_archivos.get(selection[0])

    # =========================
    # OPERATIONS
    # =========================

    def upload_file(self):
        path = filedialog.askopenfilename()
        if path:
            try:
                self.SAD.subir_archivo(path)
                self.recarga_archivos()
                messagebox.showinfo("OK", "Archivo subido correctamente")
            except Exception as e:
                messagebox.showerror("Error", str(e))

    def download_file(self):
        filename = self.get_selected_file()
        if not filename:
            messagebox.showwarning("Aviso", "Selecciona un archivo")
            return

        save_path = filedialog.asksaveasfilename(
            initialfile=filename,
            defaultextension=os.path.splitext(filename)[1],  # fuerza la extensión original
            filetypes=[("Todos los archivos", "*.*")]
        )
        if save_path:
            try:
                self.SAD.descargar_archivo(filename, save_path)
                messagebox.showinfo("OK", "Archivo descargado")
            except Exception as e:
                messagebox.showerror("Error", str(e))

    def delete_file(self):
        filename = self.get_selected_file()
        if not filename:
            messagebox.showwarning("Aviso", "Selecciona un archivo")
            return

        confirm = messagebox.askyesno("Confirmar", f"¿Eliminar {filename}?")
        if confirm:
            try:
                self.SAD.eliminar_archivo(filename)
                self.recarga_archivos()
                messagebox.showinfo("OK", "Archivo eliminado")
            except Exception as e:
                messagebox.showerror("Error", str(e))

    def show_attributes(self):
        filename = self.get_selected_file()
        if not filename:
            messagebox.showwarning("Aviso", "Selecciona un archivo")
            return

        try:
            attrs = self.SAD.obten_atributos_archivo(filename)
        except Exception as e:
            messagebox.showerror("Error", str(e))
            return

        # Crear ventana nueva
        window = tk.Toplevel(self.root)
        window.title(f"Atributos del archivo: {filename}")
        window.geometry("420x300")

        # Frame para tree + scroll
        frame = tk.Frame(window, padx=5, pady=5)
        frame.pack(fill="both", expand=True)

        tree = ttk.Treeview(frame, columns=("Fragmento", "Nodo", "Bloque"), show="headings")
        tree.heading("Fragmento", text="Fragmento")
        tree.heading("Nodo", text="Nodo")
        tree.heading("Bloque", text="Bloque")
        tree.column("Fragmento", width=100, anchor="center")
        tree.column("Nodo", width=120, anchor="center")
        tree.column("Bloque", width=120, anchor="center")

        # Scroll vertical
        scrollbar = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=scrollbar.set)

        # layout
        tree.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        # Insertar datos
        for a in attrs:
            # blindaje defensivo
            frag = a.get("id_fragmento", "-")
            nodo = a.get("id_nodo", "-")
            bloque = a.get("id_bloque", "-")
            tree.insert("", "end", values=(frag, nodo, bloque))

    def show_block_table(self):
        try:
            block_table = self.SAD.obten_tabla_bloques_completa_cluster()
        except Exception as e:
            messagebox.showerror("Error", str(e))
            return

        # Crear ventana nueva
        ventana = Toplevel(self.root)
        ventana.title("Tabla de Bloques")
        ventana.geometry("700x450")

        # Frame contenedor para tree + scrollbar
        frame_tabla = tk.Frame(ventana, padx=6, pady=6)
        frame_tabla.pack(fill="both", expand=True)

        # Crear Treeview con columnas definidas
        cols = ("Nodo", "Bloque", "Estado", "Archivo", "Fragmento")
        tree = ttk.Treeview(frame_tabla, columns=cols, show="headings")

        # configurar columnas y encabezados
        tree.heading("Nodo", text="Nodo")
        tree.heading("Bloque", text="Bloque")
        tree.heading("Estado", text="Estado")
        tree.heading("Archivo", text="Archivo")
        tree.heading("Fragmento", text="Fragmento")

        tree.column("Nodo", width=120, anchor="center")
        tree.column("Bloque", width=80, anchor="center")
        tree.column("Estado", width=100, anchor="center")
        tree.column("Archivo", width=220, anchor="w")
        tree.column("Fragmento", width=80, anchor="center")

        # Scrollbar vertical
        scrollbar = ttk.Scrollbar(frame_tabla, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=scrollbar.set)

        # layout: tree a la izquierda, scrollbar a la derecha
        tree.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        # Normalizar e insertar datos ordenados
        items = []
        for (id_nodo, id_bloque), info in block_table.items():
            # convertir id_bloque a int si se puede, ignorar si no
            try:
                id_bloque_num = int(id_bloque)
            except Exception:
                # ignorar entradas inválidas
                continue

            # blindaje info como dict
            if not isinstance(info, dict):
                info = {}

            items.append((str(id_nodo), id_bloque_num, info))

        # ordenar por nodo (alfabético) y bloque (numérico)
        items.sort(key=lambda x: (x[0], x[1]))

        for id_nodo, id_bloque, info in items:
            estado = info.get("estado", "LIBRE")
            nombre = info.get("nombre_archivo") if info.get("nombre_archivo") is not None else ""
            fragmento = info.get("id_fragmento")
            fragmento = "" if fragmento is None else fragmento

            tree.insert("", tk.END, values=(
                id_nodo,
                id_bloque,
                estado,
                nombre,
                fragmento
            ))

    # =========================
    # MAIN LOOP
    # =========================

    def run(self):
        self.recarga_archivos()
        self.root.mainloop()
