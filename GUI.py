import tkinter as tk
from tkinter import filedialog, messagebox
from tkinter import ttk, Toplevel


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
        frame.pack()

        # Lista de archivos
        self.lista_archivos = tk.Listbox(frame, width=40)
        self.lista_archivos.pack()

        refresh_btn = tk.Button(frame, text="Actualizar archivos", command=self.recarga_archivos)
        refresh_btn.pack(pady=5)

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

        save_path = filedialog.asksaveasfilename(initialfile=filename)
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

        tree = ttk.Treeview(window, columns=("Fragmento", "Nodo", "Bloque"), show="headings")
        tree.heading("Fragmento", text="Fragmento")
        tree.heading("Nodo", text="Nodo")
        tree.heading("Bloque", text="Bloque")
        tree.column("Fragmento", width=100, anchor="center")
        tree.column("Nodo", width=100, anchor="center")
        tree.column("Bloque", width=100, anchor="center")
        tree.pack(fill="both", expand=True)

        # Insertar datos
        for a in attrs:
            tree.insert("", "end", values=(a["id_fragmento"], a["id_nodo"], a["id_bloque"]))

        # Scroll vertical
        scrollbar = ttk.Scrollbar(window, orient="vertical", command=tree.yview)
        tree.configure(yscroll=scrollbar.set)
        scrollbar.pack(side="right", fill="y")


    def show_block_table(self):
        try:
            block_table = self.SAD.obten_tabla_bloques_completa_cluster()
        except Exception as e:
            tk.messagebox.showerror("Error", str(e))
            return
        
        # Crear ventana nueva
        ventana = Toplevel(self.root)
        ventana.title("Tabla de Bloques")
        ventana.geometry("600x400")

        # Crear Treeview
        tree = ttk.Treeview(ventana)
        tree.pack(expand=True, fill="both")

        # Definir columnas
        tree["columns"] = ("Nodo", "Bloque", "Estado", "Archivo", "Fragmento")
        tree.heading("#0", text="")  # columna fantasma
        tree.column("#0", width=0, stretch=False)

        for col in tree["columns"]:
            tree.heading(col, text=col)
            tree.column(col, width=100, anchor="center")

        # Insertar datos
        for (id_nodo, id_bloque), info in sorted(block_table.items()):
            tree.insert("", tk.END, values=(
                id_nodo,
                id_bloque,
                info.get("estado", "LIBRE"),
                info.get("nombre_archivo", ""),
                info.get("id_fragmento", "")
            ))

        # Agregar scroll vertical
        scrollbar = ttk.Scrollbar(ventana, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=scrollbar.set)           
        scrollbar.pack(side="right", fill="y")


    # =========================
    # MAIN LOOP
    # =========================

    def run(self):
        self.recarga_archivos()
        self.root.mainloop()
