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

        # permitir redimensionado y tamaño mínimo razonable
        try:
            self.root.minsize(480, 320)
        except Exception:
            pass

        self._build_ui()

    # =========================
    # UI SETUP
    # =========================

    def _build_ui(self):
        # Estilos y título superior
        style = ttk.Style(self.root)
        try:
            style.theme_use('clam')
        except Exception:
            pass
        style.configure('Title.TLabel', background='#0b5a8a', foreground='white', font=('Helvetica', 11, 'bold'))

        title = ttk.Label(self.root, text='SADTF', style='Title.TLabel', anchor='center')
        title.pack(fill='x')

        frame = tk.Frame(self.root, padx=10, pady=10)
        frame.pack(fill="both", expand=True)

        # Título / label de lista de archivos
        files_label = tk.Label(frame, text="Archivos en el sistema:", anchor="w")
        files_label.pack(fill="x")

        # Contenedor para lista de archivos + scrollbar
        files_frame = tk.Frame(frame)
        files_frame.pack(fill="both", expand=True)

        self.lista_archivos = tk.Listbox(files_frame, width=40, height=12)
        scroll_y = ttk.Scrollbar(files_frame, orient="vertical", command=self.lista_archivos.yview)
        self.lista_archivos.configure(yscrollcommand=scroll_y.set)

        self.lista_archivos.pack(side="left", fill="both", expand=True)
        scroll_y.pack(side="right", fill="y")

        # Botón de recarga (estilo ttk para apariencia consistente)
        refresh_btn = ttk.Button(frame, text="Actualizar archivos", command=self.recarga_archivos)
        refresh_btn.pack(pady=6, fill="x")

        # Panel de botones de operaciones (alineados horizontalmente para mejor estética)
        buttons_frame = tk.Frame(frame)
        buttons_frame.pack(fill="x", pady=(8, 0))

        left_btns = tk.Frame(buttons_frame)
        left_btns.pack(side='left', fill='x', expand=True)
        right_btns = tk.Frame(buttons_frame)
        right_btns.pack(side='right')

        ttk.Button(left_btns, text="Cargar", command=self.upload_file).pack(side='left', padx=4)
        ttk.Button(left_btns, text="Atributos de archivo", command=self.show_attributes).pack(side='left', padx=4)
        ttk.Button(left_btns, text="Tabla", command=self.show_block_table).pack(side='left', padx=4)

        ttk.Button(right_btns, text="Descargar", command=self.download_file).pack(side='left', padx=4)
        ttk.Button(right_btns, text="Eliminar", command=self.delete_file).pack(side='left', padx=4)

        # Barra de estado simple que muestra número de archivos y mensajes cortos
        self.status_var = tk.StringVar(value="Listo")
        status_label = ttk.Label(self.root, textvariable=self.status_var, relief="sunken", anchor="w")
        status_label.pack(fill="x", side="bottom")

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
        # Actualizar barra de estado con el número de archivos
        try:
            count = len(files)
            self.status_var.set(f"{count} archivo" + ("s" if count != 1 else ""))
        except Exception:
            # en caso de que status_var no exista o falle, simplemente ignorar
            pass

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
        ventana.geometry("900x500")

        # Contenedor principal
        frame_tabla = ttk.Frame(ventana, padding=6)
        frame_tabla.pack(fill="both", expand=True)

        # Barra superior: búsqueda + expand/collapse
        top_frame = ttk.Frame(frame_tabla)
        top_frame.pack(fill='x', pady=(0,6))

        search_var = tk.StringVar()
        ttk.Label(top_frame, text='Buscar archivo:').pack(side='left', padx=(0,6))
        search_entry = ttk.Entry(top_frame, textvariable=search_var)
        search_entry.pack(side='left')

        def do_search():
            q = search_var.get().lower().strip()
            if not q:
                return
            for parent in tree.get_children(''):
                name = tree.item(parent, 'text').lower()
                if q in name:
                    tree.see(parent)
                    tree.selection_set(parent)
                    tree.item(parent, open=True)

        ttk.Button(top_frame, text='Buscar', command=do_search).pack(side='left', padx=6)

        btns_frame = ttk.Frame(top_frame)
        btns_frame.pack(side='right')

        def expand_all():
            for p in tree.get_children(''):
                tree.item(p, open=True)

        def collapse_all():
            for p in tree.get_children(''):
                tree.item(p, open=False)

        ttk.Button(btns_frame, text='Expandir todo', command=expand_all).pack(side='left', padx=4)
        ttk.Button(btns_frame, text='Colapsar todo', command=collapse_all).pack(side='left', padx=4)

        # Treeview con jerarquía: padre = archivo, hijos = réplicas
        cols = ("Nodo", "Bloque", "Estado", "Fragmento")
        tree = ttk.Treeview(frame_tabla, columns=cols, show='tree headings')
        tree.heading('#0', text='Archivo')
        tree.column('#0', width=300, anchor='w')

        tree.heading('Nodo', text='Nodo')
        tree.heading('Bloque', text='Bloque')
        tree.heading('Estado', text='Estado')
        tree.heading('Fragmento', text='Fragmento')

        tree.column('Nodo', width=140, anchor='center')
        tree.column('Bloque', width=80, anchor='center')
        tree.column('Estado', width=120, anchor='center')
        tree.column('Fragmento', width=100, anchor='center')

        vsb = ttk.Scrollbar(frame_tabla, orient='vertical', command=tree.yview)
        hsb = ttk.Scrollbar(frame_tabla, orient='horizontal', command=tree.xview)
        tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        tree.pack(fill='both', expand=True, side='left')
        vsb.pack(fill='y', side='right')
        hsb.pack(fill='x', side='bottom')

        # Agrupar réplicas por nombre de archivo
        replicas_by_file = {}
        free_entries = []
        for (id_nodo, id_bloque), info in block_table.items():
            try:
                id_bloque_num = int(id_bloque)
            except Exception:
                continue

            if not isinstance(info, dict):
                info = {}

            nombre = info.get('nombre_archivo')
            estado = info.get('estado', 'LIBRE')
            fragmento = info.get('id_fragmento') or ''
            entry = (str(id_nodo), id_bloque_num, estado, fragmento)

            if nombre:
                replicas_by_file.setdefault(nombre, []).append(entry)
            else:
                free_entries.append(entry)

        # Insertar grupos y réplicas como hijos
        for nombre in sorted(replicas_by_file.keys(), key=lambda s: s.lower()):
            parent = tree.insert('', 'end', text=nombre, values=("", "", "", ""))
            replicas = sorted(replicas_by_file[nombre], key=lambda x: (x[0], x[1]))
            for node, block, estado, fragmento in replicas:
                tree.insert(parent, 'end', text='', values=(node, block, estado, fragmento))

        if free_entries:
            p_free = tree.insert('', 'end', text='Bloques libres', values=("", "", "", ""))
            for node, block, estado, fragmento in sorted(free_entries, key=lambda x: (x[0], x[1])):
                tree.insert(p_free, 'end', text='', values=(node, block, estado, fragmento))

    # =========================
    # MAIN LOOP
    # =========================

    def run(self):
        self.recarga_archivos()
        self.root.mainloop()
