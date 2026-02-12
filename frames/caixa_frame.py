import tkinter as tk

class CaixaFrame(tk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent)

        tk.Label(self, text="CAIXA (PDV)", font=("Arial", 22)).pack(pady=20)
        tk.Label(self, text="(Tela do caixa vai aqui)").pack()
