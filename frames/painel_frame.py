import tkinter as tk
from tkinter import ttk
from datetime import datetime, timedelta

try:
    from tkcalendar import Calendar
except Exception:
    Calendar = None


class PainelProprietarioFrame(tk.Frame):
    """
    Painel do Proprietário:
    - Calendário para escolher data
    - Lista de vendas do período (ex: dia selecionado)
    - Mini calendário do mês: total vendido por dia + total do mês
    """

    def __init__(
        self,
        parent,
        controller,
        listar_vendas_por_periodo,
        total_por_periodo,
        totais_por_dia_do_mes=None,  # ✅ opcional (mas seu pdv.py já manda)
    ):
        # ===== Tema (se vier do controller, usa; senão, usa padrão) =====
        theme = getattr(controller, "theme", {}) if controller else {}
        self.BG_APP = theme.get("BG_APP", "#F2F2F2")
        self.CARD_BG = theme.get("CARD_BG", "white")
        self.BAR_BG = theme.get("BAR_BG", "#1F2937")
        self.BAR_FG = theme.get("BAR_FG", "white")

        super().__init__(parent, bg=self.BG_APP)
        self.controller = controller
        self.listar_vendas_por_periodo = listar_vendas_por_periodo
        self.total_por_periodo = total_por_periodo
        self.totais_por_dia_do_mes = totais_por_dia_do_mes

        # ===== Cabeçalho =====
        tk.Label(
            self,
            text="Painel do Proprietário",
            font=("Arial", 18, "bold"),
            bg=self.BG_APP
        ).pack(anchor="w", padx=12, pady=(12, 4))

        tk.Label(
            self,
            text="Selecione uma data no calendário para ver as vendas do dia. À direita: total por dia e total do mês.",
            font=("Arial", 10),
            bg=self.BG_APP
        ).pack(anchor="w", padx=12, pady=(0, 10))

        if Calendar is None:
            tk.Label(
                self,
                text="⚠ Para usar o calendário, instale: pip install tkcalendar",
                fg="red",
                bg=self.BG_APP,
                font=("Arial", 11, "bold")
            ).pack(padx=12, pady=10)
            return

        # ===== Layout: esquerda (calendar + vendas do dia), direita (totais do mês) =====
        root = tk.Frame(self, bg=self.BG_APP)
        root.pack(fill="both", expand=True, padx=12, pady=12)

        root.grid_columnconfigure(0, weight=2)
        root.grid_columnconfigure(1, weight=1)
        root.grid_rowconfigure(0, weight=1)

        left = tk.Frame(root, bg=self.BG_APP)
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 10))
        left.grid_rowconfigure(2, weight=1)
        left.grid_columnconfigure(0, weight=1)

        right = tk.Frame(root, bg=self.BG_APP)
        right.grid(row=0, column=1, sticky="nsew")
        right.grid_rowconfigure(1, weight=1)
        right.grid_columnconfigure(0, weight=1)

        # ===== Card calendário =====
        card_cal = tk.Frame(left, bg=self.CARD_BG, bd=1, relief="solid")
        card_cal.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        card_cal.grid_columnconfigure(0, weight=1)

        tk.Label(
            card_cal,
            text="Calendário",
            font=("Arial", 12, "bold"),
            bg=self.CARD_BG
        ).grid(row=0, column=0, sticky="w", padx=10, pady=(8, 6))

        self.cal = Calendar(
            card_cal,
            selectmode="day",
            date_pattern="dd/mm/yyyy",
        )
        self.cal.grid(row=1, column=0, sticky="w", padx=10, pady=(0, 10))

        # ===== Resumo do dia selecionado =====
        card_dia = tk.Frame(left, bg=self.CARD_BG, bd=1, relief="solid")
        card_dia.grid(row=1, column=0, sticky="ew", pady=(0, 10))
        card_dia.grid_columnconfigure(0, weight=1)

        self.lbl_dia = tk.Label(
            card_dia,
            text="Total do dia: R$ 0,00",
            font=("Arial", 12, "bold"),
            bg=self.CARD_BG
        )
        self.lbl_dia.grid(row=0, column=0, sticky="w", padx=10, pady=10)

        # ===== Tabela de vendas do dia =====
        card_tbl = tk.Frame(left, bg=self.CARD_BG, bd=1, relief="solid")
        card_tbl.grid(row=2, column=0, sticky="nsew")
        card_tbl.grid_rowconfigure(1, weight=1)
        card_tbl.grid_columnconfigure(0, weight=1)

        tk.Label(
            card_tbl,
            text="Vendas do dia",
            font=("Arial", 12, "bold"),
            bg=self.CARD_BG
        ).grid(row=0, column=0, sticky="w", padx=10, pady=(8, 6))

        cols = ("DataHora", "Cupom", "Itens", "Total")
        self.tbl = ttk.Treeview(card_tbl, columns=cols, show="headings", height=14)
        for c in cols:
            self.tbl.heading(c, text=c)
            self.tbl.column(c, anchor="center", width=120)
        self.tbl.column("DataHora", width=150)
        self.tbl.grid(row=1, column=0, sticky="nsew", padx=10, pady=(0, 10))

        scroll = ttk.Scrollbar(card_tbl, orient="vertical", command=self.tbl.yview)
        scroll.grid(row=1, column=1, sticky="ns", pady=(0, 10))
        self.tbl.configure(yscrollcommand=scroll.set)

        # ===== Direita: Totais por dia do mês =====
        card_mes = tk.Frame(right, bg=self.CARD_BG, bd=1, relief="solid")
        card_mes.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        card_mes.grid_columnconfigure(0, weight=1)

        tk.Label(
            card_mes,
            text="Totais do mês (por dia)",
            font=("Arial", 12, "bold"),
            bg=self.CARD_BG
        ).grid(row=0, column=0, sticky="w", padx=10, pady=(8, 6))

        self.lbl_total_mes = tk.Label(
            card_mes,
            text="Total do mês: R$ 0,00",
            font=("Arial", 12, "bold"),
            bg=self.CARD_BG
        )
        self.lbl_total_mes.grid(row=1, column=0, sticky="w", padx=10, pady=(0, 10))

        card_lista = tk.Frame(right, bg=self.CARD_BG, bd=1, relief="solid")
        card_lista.grid(row=1, column=0, sticky="nsew")
        card_lista.grid_rowconfigure(1, weight=1)
        card_lista.grid_columnconfigure(0, weight=1)

        tk.Label(
            card_lista,
            text="Dia → Total",
            font=("Arial", 12, "bold"),
            bg=self.CARD_BG
        ).grid(row=0, column=0, sticky="w", padx=10, pady=(8, 6))

        cols2 = ("Dia", "Total")
        self.tbl_mes = ttk.Treeview(card_lista, columns=cols2, show="headings", height=18)
        self.tbl_mes.heading("Dia", text="Dia")
        self.tbl_mes.heading("Total", text="Total (R$)")
        self.tbl_mes.column("Dia", width=90, anchor="center")
        self.tbl_mes.column("Total", width=140, anchor="e")
        self.tbl_mes.grid(row=1, column=0, sticky="nsew", padx=10, pady=(0, 10))

        scroll2 = ttk.Scrollbar(card_lista, orient="vertical", command=self.tbl_mes.yview)
        scroll2.grid(row=1, column=1, sticky="ns", pady=(0, 10))
        self.tbl_mes.configure(yscrollcommand=scroll2.set)

        # ===== Eventos =====
        self.cal.bind("<<CalendarSelected>>", lambda e: self._carregar_dia_selecionado())
        # quando muda o mês (tkcalendar suporta esse evento)
        self.cal.bind("<<CalendarMonthChanged>>", lambda e: self._carregar_totais_mes())

        # Primeira carga
        self._carregar_totais_mes()
        self._carregar_dia_selecionado()

    # ===== Helpers =====
    @staticmethod
    def _brl(v: float) -> str:
        return f"{v:.2f}".replace(".", ",")

    def on_show(self):
        # quando abre a tela, atualiza tudo
        self._carregar_totais_mes()
        self._carregar_dia_selecionado()

    def _carregar_totais_mes(self):
        # se a função não foi passada, não quebra
        if not callable(self.totais_por_dia_do_mes):
            self.lbl_total_mes.config(text="Total do mês: (função não configurada)")
            return

        # mês/ano atual do calendário
        # tkcalendar Calendar tem .datetime (nem sempre), então pegamos por seleção
        try:
            dt_sel = datetime.strptime(self.cal.get_date(), "%d/%m/%Y")
        except Exception:
            dt_sel = datetime.now()

        ano, mes = dt_sel.year, dt_sel.month
        mapa, total_mes = self.totais_por_dia_do_mes(ano, mes)

        self.lbl_total_mes.config(text=f"Total do mês: R$ {self._brl(total_mes)}")

        # preencher tabela
        self.tbl_mes.delete(*self.tbl_mes.get_children())

        # mostrar em ordem (1..31)
        for dia in range(1, 32):
            try:
                d = datetime(ano, mes, dia)
            except Exception:
                continue
            key = d.strftime("%Y-%m-%d")
            total_dia = float(mapa.get(key, 0.0))
            self.tbl_mes.insert("", "end", values=(d.strftime("%d/%m"), self._brl(total_dia)))

    def _carregar_dia_selecionado(self):
        # data selecionada no calendário
        try:
            dt = datetime.strptime(self.cal.get_date(), "%d/%m/%Y")
        except Exception:
            dt = datetime.now()

        ini = datetime(dt.year, dt.month, dt.day, 0, 0, 0)
        fim = datetime(dt.year, dt.month, dt.day, 23, 59, 59)

        # carregar vendas agregadas por “cupom”
        rows = self.listar_vendas_por_periodo(ini, fim)

        self.tbl.delete(*self.tbl.get_children())
        for datahora_fmt, cupom, itens, total in rows:
            self.tbl.insert("", "end", values=(datahora_fmt, cupom, itens, self._brl(float(total))))

        total_dia = float(self.total_por_periodo(ini, fim))
        self.lbl_dia.config(text=f"Total do dia: R$ {self._brl(total_dia)}")
