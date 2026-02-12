import tkinter as tk

class MenuFrame(tk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent)

        tk.Label(self, text="MENU PRINCIPAL", font=("Arial", 22, "bold")).pack(pady=18)

        box = tk.Frame(self)
        box.pack(pady=10)

        tk.Button(
            box, text="🧾 Caixa (PDV)", width=26, height=2,
            command=lambda: controller.show("CaixaFrame")
        ).pack(pady=6)

        tk.Button(
            box, text="📦 Estoque", width=26, height=2,
            command=lambda: controller.show("EstoqueFrame")
        ).pack(pady=6)

        tk.Button(
            box, text="📈 Histórico de Vendas", width=26, height=2,
            command=lambda: controller.show("HistoricoFrame")
        ).pack(pady=6)
