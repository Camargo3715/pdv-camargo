import tkinter as tk

class HistoricoFrame(tk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent)

        tk.Label(self, text="HISTÓRICO", font=("Arial", 22)).pack(pady=20)
        tk.Label(self, text="(Tela de histórico vai aqui)").pack()
