import tkinter as tk

from frames.menu_frame import MenuFrame
from frames.estoque_frame import EstoqueFrame
from frames.caixa_frame import CaixaFrame
from frames.historico_frame import HistoricoFrame

class PDVApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Sistema PDV")
        self.geometry("900x560")

        self.historico = []
        self.frame_atual = None

        # Barra superior
        barra = tk.Frame(self, height=48)
        barra.pack(fill="x")

        self.btn_voltar = tk.Button(barra, text="⬅ Voltar", width=10, command=self.voltar)
        self.btn_voltar.pack(side="left", padx=8, pady=8)

        self.btn_menu = tk.Button(barra, text="🏠 Menu", width=10, command=self.ir_menu)
        self.btn_menu.pack(side="left", pady=8)

        self.lbl_titulo = tk.Label(barra, text="Menu Principal", font=("Arial", 14))
        self.lbl_titulo.pack(side="left", padx=12)

        tk.Frame(self, height=1, bg="#cccccc").pack(fill="x")

        # Container
        container = tk.Frame(self)
        container.pack(fill="both", expand=True)
        container.grid_rowconfigure(0, weight=1)
        container.grid_columnconfigure(0, weight=1)

        self.frames = {}
        for F in (MenuFrame, EstoqueFrame, CaixaFrame, HistoricoFrame):
            frame = F(container, controller=self)
            self.frames[F.__name__] = frame
            frame.grid(row=0, column=0, sticky="nsew")

        self.titulos = {
            "MenuFrame": "Menu Principal",
            "EstoqueFrame": "Estoque",
            "CaixaFrame": "Caixa (PDV)",
            "HistoricoFrame": "Histórico de Vendas",
        }

        self.bind("<Escape>", lambda e: self.voltar())
        self.show("MenuFrame", registrar=False)

    def show(self, name: str, registrar=True):
        if registrar and self.frame_atual is not None:
            self.historico.append(self.frame_atual)

        self.frame_atual = name
        self.frames[name].tkraise()
        self.lbl_titulo.config(text=self.titulos.get(name, "Sistema PDV"))
        self.btn_voltar.config(state=("normal" if self.historico else "disabled"))

    def voltar(self):
        if not self.historico:
            return
        anterior = self.historico.pop()
        self.show(anterior, registrar=False)

    def ir_menu(self):
        self.historico.clear()
        self.show("MenuFrame", registrar=False)

if __name__ == "__main__":
    PDVApp().mainloop()
