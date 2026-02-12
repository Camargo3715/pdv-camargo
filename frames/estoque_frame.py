import tkinter as tk

class EstoqueFrame(tk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent)

        tk.Label(self, text="ESTOQUE", font=("Arial", 22)).pack(pady=20)
        tk.Label(self, text="(Tela de estoque vai aqui)").pack()
