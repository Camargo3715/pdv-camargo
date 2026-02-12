import tkinter as tk
from tkinter import ttk


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Sistema PDV (Frames)")
        self.geometry("900x600")
        self.minsize(800, 520)

        print("ARQUIVO DE FRAMES CARREGADO")

        # ===== Navegação =====
        self.historico = []      # stack de telas (nomes)
        self.tela_atual = None   # nome atual

        # ===== Layout principal =====
        self._criar_barra_superior()
        self._criar_container()

        # ===== Telas =====
        self.telas = {
            "menu": TelaMenu(self.container, app=self),
            "estoque": TelaEstoque(self.container, app=self),
            "caixa": TelaCaixa(self.container, app=self),
            "historico": TelaHistorico(self.container, app=self),
        }

        for tela in self.telas.values():
            tela.place(relwidth=1, relheight=1)

        # Atalhos (Esc = voltar)
        self.bind("<Escape>", lambda e: self.voltar())

        # Abre no menu
        self.show("menu", registrar_historico=False)

    def _criar_barra_superior(self):
        # Fonte "segura" que costuma renderizar a seta ←
        self._fonte_barra = ("Segoe UI", 10)

        self.barra = ttk.Frame(self, padding=(10, 8))
        self.barra.pack(fill="x")

        # Botão Voltar (seta garantida)
        self.btn_voltar = ttk.Button(self.barra, text="← Voltar", command=self.voltar)
        self.btn_voltar.pack(side="left")

        # Botão Menu
        self.btn_menu = ttk.Button(self.barra, text="Menu", command=self.ir_menu)
        self.btn_menu.pack(side="left", padx=(8, 0))

        # Título da tela
        self.lbl_titulo = ttk.Label(self.barra, text="Menu Principal", font=("Segoe UI", 12, "bold"))
        self.lbl_titulo.pack(side="left", padx=(14, 0))

        # Linha divisória
        ttk.Separator(self).pack(fill="x")

    def _criar_container(self):
        self.container = ttk.Frame(self)
        self.container.pack(fill="both", expand=True)

    def show(self, nome_tela: str, registrar_historico=True):
        """Mostra uma tela (frame) pelo nome e atualiza título/botões."""
        if nome_tela not in self.telas:
            raise ValueError(f"Tela '{nome_tela}' não existe.")

        if registrar_historico and self.tela_atual is not None:
            self.historico.append(self.tela_atual)

        self.tela_atual = nome_tela
        tela = self.telas[nome_tela]
        tela.tkraise()

        self.lbl_titulo.config(text=tela.titulo)
        self._atualizar_botoes()

        if hasattr(tela, "on_show"):
            tela.on_show()

    def voltar(self):
        if not self.historico:
            return
        anterior = self.historico.pop()
        self.show(anterior, registrar_historico=False)

    def ir_menu(self):
        self.historico.clear()
        self.show("menu", registrar_historico=False)

    def _atualizar_botoes(self):
        self.btn_voltar.config(state=("normal" if self.historico else "disabled"))


# ===== Base de tela =====
class TelaBase(ttk.Frame):
    titulo = "Sistema PDV"

    def __init__(self, parent, app: App):
        super().__init__(parent)
        self.app = app


# ===== Telas =====
class TelaMenu(TelaBase):
    titulo = "Menu Principal"

    def __init__(self, parent, app: App):
        super().__init__(parent, app)

        box = ttk.Frame(self, padding=20)
        box.pack(fill="both", expand=True)

        ttk.Label(box, text="Menu Principal", font=("Segoe UI", 22, "bold")).pack(pady=20)

        ttk.Button(
            box, text="Estoque", width=28,
            command=lambda: self.app.show("estoque")
        ).pack(pady=8)

        ttk.Button(
            box, text="Caixa (PDV)", width=28,
            command=lambda: self.app.show("caixa")
        ).pack(pady=8)

        ttk.Button(
            box, text="Histórico", width=28,
            command=lambda: self.app.show("historico")
        ).pack(pady=8)


class TelaEstoque(TelaBase):
    titulo = "Estoque"

    def __init__(self, parent, app: App):
        super().__init__(parent, app)

        box = ttk.Frame(self, padding=20)
        box.pack(fill="both", expand=True)

        ttk.Label(box, text="Tela de Estoque", font=("Segoe UI", 18, "bold")).pack(pady=10)
        ttk.Label(box, text="(Aqui você vai colocar o estoque real)").pack()


class TelaCaixa(TelaBase):
    titulo = "Caixa (PDV)"

    def __init__(self, parent, app: App):
        super().__init__(parent, app)

        box = ttk.Frame(self, padding=20)
        box.pack(fill="both", expand=True)

        ttk.Label(box, text="Tela do Caixa (PDV)", font=("Segoe UI", 18, "bold")).pack(pady=10)
        ttk.Label(box, text="(Aqui você vai colocar o caixa real)").pack(pady=(0, 10))

        # Área onde você vai montar o caixa de verdade
        self.area = ttk.Frame(box)
        self.area.pack(fill="both", expand=True)

        self.build()

    def build(self):
        demo = ttk.Labelframe(self.area, text="Área do Caixa Real", padding=10)
        demo.pack(fill="x", pady=10)
        ttk.Label(demo, text="Pronto para receber seu layout do caixa.").pack(anchor="w")


class TelaHistorico(TelaBase):
    titulo = "Histórico de Vendas"

    def __init__(self, parent, app: App):
        super().__init__(parent, app)

        box = ttk.Frame(self, padding=20)
        box.pack(fill="both", expand=True)

        ttk.Label(box, text="Tela de Histórico", font=("Segoe UI", 18, "bold")).pack(pady=10)
        ttk.Label(box, text="(Aqui você vai colocar o histórico real)").pack()


if __name__ == "__main__":
    App().mainloop()
