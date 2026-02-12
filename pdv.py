import tkinter as tk
from tkinter import messagebox, ttk, filedialog
from datetime import datetime, timedelta
from frames.painel_frame import PainelProprietarioFrame

import os
import json
import sqlite3
import tempfile


# =========================
# TEMA / PERSONALIZAÇÃO
# =========================
APP_TITLE = "Sistema de Caixa — Camargo Celulares"

THEME = {
    "BG_APP": "#F2F2F2",    # fundo geral
    "CARD_BG": "white",     # cards/caixas
    "BAR_BG": "#1F2937",    # barra superior
    "BAR_FG": "white",      # texto da barra
    "BTN_BG": "white",      # botões barra
}


# =========================================================
# CONFIG (config_pdv.json)
# =========================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "config_pdv.json")

DEFAULT_CONFIG = {
    "cupom_largura": 40,
    "loja": {
        "nome": "MERCADO EXEMPLO LTDA",
        "cnpj": "00.000.000/0001-00",
        "ie": "ISENTO",
        "endereco": "Rua Exemplo, 123 - Centro",
        "cidade": "Sua Cidade - UF",
        "telefone": "(11) 99999-9999",
        "mensagem": "OBRIGADO! VOLTE SEMPRE :)",
        "mostrar_cupom_nao_fiscal": True
    }
}


def garantir_config():
    if not os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(DEFAULT_CONFIG, f, ensure_ascii=False, indent=2)


def carregar_config_pdv():
    garantir_config()
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


# =========================================================
# BANCO (SQLite)
# =========================================================
DB_PATH = os.path.join(BASE_DIR, "pdv.db")


def conectar():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def _coluna_existe(conn, tabela: str, coluna: str) -> bool:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({tabela})")
    cols = [r[1] for r in cur.fetchall()]
    return coluna in cols


def inicializar_banco():
    with conectar() as conn:
        cur = conn.cursor()

        # Produtos
        cur.execute("""
        CREATE TABLE IF NOT EXISTS produtos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            codigo TEXT UNIQUE,
            nome TEXT NOT NULL,
            preco_custo REAL NOT NULL DEFAULT 0,
            preco_venda REAL NOT NULL,
            quantidade INTEGER NOT NULL DEFAULT 0
        )
        """)

        # (LEGADO) vendas por item — mantém se já existir, mas o sistema passa a usar as novas tabelas
        cur.execute("""
        CREATE TABLE IF NOT EXISTS vendas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            datahora TEXT NOT NULL,
            codigo TEXT,
            produto TEXT NOT NULL,
            preco_unit REAL NOT NULL,
            qtd INTEGER NOT NULL,
            total REAL NOT NULL
        )
        """)

        # Sessões de caixa
        cur.execute("""
        CREATE TABLE IF NOT EXISTS caixa_sessoes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            aberto_em TEXT NOT NULL,
            fechado_em TEXT,
            status TEXT NOT NULL DEFAULT 'ABERTO',
            saldo_inicial REAL NOT NULL DEFAULT 0,
            saldo_final_sistema REAL NOT NULL DEFAULT 0,
            saldo_final_informado REAL NOT NULL DEFAULT 0,
            diferenca REAL NOT NULL DEFAULT 0,
            operador TEXT,
            obs_abertura TEXT,
            obs_fechamento TEXT
        )
        """)

        # Cabeçalho da venda (um cupom por venda)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS vendas_cabecalho (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            datahora TEXT NOT NULL,
            sessao_id INTEGER,
            subtotal REAL NOT NULL DEFAULT 0,
            desconto REAL NOT NULL DEFAULT 0,
            total REAL NOT NULL DEFAULT 0,
            forma_pagamento TEXT NOT NULL,
            recebido REAL NOT NULL DEFAULT 0,
            troco REAL NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'FINALIZADA',
            FOREIGN KEY (sessao_id) REFERENCES caixa_sessoes(id)
        )
        """)

        # Itens da venda
        cur.execute("""
        CREATE TABLE IF NOT EXISTS vendas_itens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            venda_id INTEGER NOT NULL,
            codigo TEXT,
            produto TEXT NOT NULL,
            preco_unit REAL NOT NULL,
            preco_custo REAL NOT NULL DEFAULT 0,
            qtd INTEGER NOT NULL,
            total_item REAL NOT NULL,
            FOREIGN KEY (venda_id) REFERENCES vendas_cabecalho(id) ON DELETE CASCADE
        )
        """)

        # Índices úteis
        cur.execute("CREATE INDEX IF NOT EXISTS idx_vendas_cabecalho_datahora ON vendas_cabecalho(datahora)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_vendas_cabecalho_sessao ON vendas_cabecalho(sessao_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_vendas_itens_venda ON vendas_itens(venda_id)")

        conn.commit()


# =========================
# CONSULTAS PRODUTOS
# =========================
def listar_produtos():
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT codigo, nome, preco_custo, preco_venda, quantidade
            FROM produtos
            ORDER BY nome
        """)
        return cur.fetchall()


def sincronizar_produtos(itens, parse_float):
    """
    Recebe lista no formato do seu sistema:
    [Codigo, Produto, PrecoCusto_txt, PrecoVenda_txt, Quantidade_txt]
    e faz UPSERT no banco.
    """
    with conectar() as conn:
        cur = conn.cursor()
        for row in itens:
            if len(row) != 5:
                continue
            codigo, nome, custo_txt, venda_txt, qtd_txt = row
            codigo = str(codigo).strip()
            nome = str(nome).strip()
            if not codigo or not nome:
                continue

            custo = float(parse_float(custo_txt))
            venda = float(parse_float(venda_txt))
            qtd = int(str(qtd_txt).strip())

            cur.execute("""
                INSERT INTO produtos (codigo, nome, preco_custo, preco_venda, quantidade)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(codigo) DO UPDATE SET
                    nome=excluded.nome,
                    preco_custo=excluded.preco_custo,
                    preco_venda=excluded.preco_venda,
                    quantidade=excluded.quantidade
            """, (codigo, nome, custo, venda, qtd))
        conn.commit()


# =========================
# CAIXA (ABERTURA/FECHAMENTO)
# =========================
def get_sessao_aberta():
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, aberto_em, saldo_inicial, operador
            FROM caixa_sessoes
            WHERE status='ABERTO'
            ORDER BY id DESC
            LIMIT 1
        """)
        return cur.fetchone()  # (id, aberto_em, saldo_inicial, operador) ou None


def abrir_caixa_db(saldo_inicial: float, operador: str = "", obs: str = "") -> int:
    # Garante que não existe caixa aberto
    atual = get_sessao_aberta()
    if atual:
        raise RuntimeError(f"Já existe um caixa ABERTO (Sessão #{atual[0]}). Feche antes de abrir outro.")

    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO caixa_sessoes (aberto_em, status, saldo_inicial, operador, obs_abertura)
            VALUES (?, 'ABERTO', ?, ?, ?)
        """, (agora, float(saldo_inicial or 0.0), (operador or "").strip(), (obs or "").strip()))
        conn.commit()
        return int(cur.lastrowid)


def relatorio_pagamentos_sessao(sessao_id: int):
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT forma_pagamento, COALESCE(SUM(total), 0)
            FROM vendas_cabecalho
            WHERE sessao_id = ? AND status='FINALIZADA'
            GROUP BY forma_pagamento
            ORDER BY forma_pagamento
        """, (int(sessao_id),))
        return [(str(fp), float(t or 0.0)) for fp, t in cur.fetchall()]


def totais_sessao(sessao_id: int):
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                COALESCE(SUM(total), 0)
            FROM vendas_cabecalho
            WHERE sessao_id = ? AND status='FINALIZADA'
        """, (int(sessao_id),))
        total_vendas = float(cur.fetchone()[0] or 0.0)

        cur.execute("""
            SELECT saldo_inicial, aberto_em
            FROM caixa_sessoes
            WHERE id = ?
        """, (int(sessao_id),))
        row = cur.fetchone()
        saldo_inicial = float(row[0] or 0.0) if row else 0.0
        aberto_em = row[1] if row else ""
        saldo_final_sistema = saldo_inicial + total_vendas
        return saldo_inicial, total_vendas, saldo_final_sistema, aberto_em


def fechar_caixa_db(sessao_id: int, saldo_informado: float, obs: str = ""):
    saldo_inicial, total_vendas, saldo_final_sistema, _aberto_em = totais_sessao(sessao_id)
    saldo_informado = float(saldo_informado or 0.0)
    diferenca = saldo_informado - saldo_final_sistema
    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with conectar() as conn:
        cur = conn.cursor()
        cur.execute("""
            UPDATE caixa_sessoes
            SET
                fechado_em = ?,
                status = 'FECHADO',
                saldo_final_sistema = ?,
                saldo_final_informado = ?,
                diferenca = ?,
                obs_fechamento = ?
            WHERE id = ? AND status='ABERTO'
        """, (agora, float(saldo_final_sistema), float(saldo_informado), float(diferenca), (obs or "").strip(), int(sessao_id)))
        if cur.rowcount == 0:
            raise RuntimeError("Não foi possível fechar: sessão não está ABERTA (ou não existe).")
        conn.commit()

    return {
        "saldo_inicial": saldo_inicial,
        "total_vendas": total_vendas,
        "saldo_final_sistema": saldo_final_sistema,
        "saldo_informado": saldo_informado,
        "diferenca": diferenca,
        "fechado_em": agora
    }


# =========================
# VENDAS (NOVO MODELO)
# =========================
def registrar_venda_completa_db(
    sessao_id: int,
    itens: list,
    forma_pagamento: str,
    subtotal: float,
    desconto: float,
    total: float,
    recebido: float,
    troco: float,
    status: str = "FINALIZADA",
) -> int:
    """
    itens: lista de dicts:
      {"codigo": str, "produto": str, "preco_unit": float, "preco_custo": float, "qtd": int, "total_item": float}
    """
    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO vendas_cabecalho
                (datahora, sessao_id, subtotal, desconto, total, forma_pagamento, recebido, troco, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            agora, int(sessao_id), float(subtotal), float(desconto), float(total),
            str(forma_pagamento), float(recebido), float(troco), str(status)
        ))
        venda_id = int(cur.lastrowid)

        for it in itens:
            cur.execute("""
                INSERT INTO vendas_itens
                    (venda_id, codigo, produto, preco_unit, preco_custo, qtd, total_item)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                venda_id,
                str(it.get("codigo") or ""),
                str(it.get("produto") or ""),
                float(it.get("preco_unit") or 0.0),
                float(it.get("preco_custo") or 0.0),
                int(it.get("qtd") or 0),
                float(it.get("total_item") or 0.0),
            ))

        conn.commit()
        return venda_id


def listar_vendas():
    # Histórico por item (join novo modelo)
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                c.datahora,
                i.codigo,
                i.produto,
                i.preco_unit,
                i.qtd,
                i.total_item
            FROM vendas_itens i
            JOIN vendas_cabecalho c ON c.id = i.venda_id
            WHERE c.status='FINALIZADA'
            ORDER BY c.id DESC, i.id ASC
        """)
        return cur.fetchall()


# ✅ PARA O PAINEL (calendário) - Vendas agregadas por “cupom/id”
def listar_vendas_por_periodo(data_ini: datetime, data_fim: datetime):
    """
    Retorna lista no formato do Painel:
    (datahora_fmt, cupom(id), itens, total)
    """
    ini = data_ini.strftime("%Y-%m-%d %H:%M:%S")
    fim = data_fim.strftime("%Y-%m-%d %H:%M:%S")

    with conectar() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                c.datahora,
                c.id as cupom,
                COALESCE(SUM(i.qtd), 0) as itens,
                c.total as total
            FROM vendas_cabecalho c
            LEFT JOIN vendas_itens i ON i.venda_id = c.id
            WHERE c.status='FINALIZADA'
              AND datetime(c.datahora) BETWEEN datetime(?) AND datetime(?)
            GROUP BY c.id, c.datahora, c.total
            ORDER BY datetime(c.datahora) DESC
        """, (ini, fim))
        rows = cur.fetchall()

    saida = []
    for datahora, cupom, itens, total in rows:
        try:
            dt = datetime.fromisoformat(str(datahora))
            datahora_fmt = dt.strftime("%d/%m/%Y %H:%M")
        except Exception:
            datahora_fmt = str(datahora)
        saida.append((datahora_fmt, str(cupom), int(itens or 0), float(total or 0.0)))
    return saida


def total_por_periodo(data_ini: datetime, data_fim: datetime) -> float:
    ini = data_ini.strftime("%Y-%m-%d %H:%M:%S")
    fim = data_fim.strftime("%Y-%m-%d %H:%M:%S")
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT COALESCE(SUM(total), 0)
            FROM vendas_cabecalho
            WHERE status='FINALIZADA'
              AND datetime(datahora) BETWEEN datetime(?) AND datetime(?)
        """, (ini, fim))
        return float(cur.fetchone()[0] or 0.0)


# ✅ NOVO: totais por dia + total do mês (para o mini calendário)
def totais_por_dia_do_mes(ano: int, mes: int):
    """
    Retorna:
      - mapa: dict {"YYYY-MM-DD": total_do_dia}
      - total_mes: float
    """
    primeiro = datetime(ano, mes, 1, 0, 0, 0)

    if mes == 12:
        prox = datetime(ano + 1, 1, 1, 0, 0, 0)
    else:
        prox = datetime(ano, mes + 1, 1, 0, 0, 0)

    ini = primeiro.strftime("%Y-%m-%d %H:%M:%S")
    fim = (prox - timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")

    with conectar() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT date(datahora) as dia, COALESCE(SUM(total), 0)
            FROM vendas_cabecalho
            WHERE status='FINALIZADA'
              AND datetime(datahora) BETWEEN datetime(?) AND datetime(?)
            GROUP BY date(datahora)
        """, (ini, fim))
        rows = cur.fetchall()

    mapa = {str(dia): float(total or 0.0) for dia, total in rows}
    total_mes = sum(mapa.values())
    return mapa, total_mes


# =========================================================
# FUNÇÕES ÚTEIS (BRL/parse)
# =========================================================
def to_float(txt):
    s = str(txt).strip().replace(" ", "")
    if not s:
        return 0.0
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    return float(s)


def brl(v: float) -> str:
    return f"{v:.2f}".replace(".", ",")


def carregar_na_tabela(tabela, linhas):
    tabela.delete(*tabela.get_children())
    for row in linhas:
        tabela.insert("", tk.END, values=tuple(row))


# =========================================================
# CAIXA (UI)
# =========================================================
def montar_caixa(parent, ler_estoque, escrever_estoque, registrar_venda_completa, carregar_na_tabela, get_sessao_aberta_cb):
    caixa = tk.Frame(parent, bg=THEME["BG_APP"])
    caixa.pack(fill="both", expand=True)

    root = tk.Frame(caixa, bg=THEME["BG_APP"])
    root.pack(fill="both", expand=True, padx=10, pady=10)

    root.grid_columnconfigure(0, weight=4)
    root.grid_columnconfigure(1, weight=1)
    root.grid_rowconfigure(0, weight=1)

    left = tk.Frame(root, bg=THEME["CARD_BG"])
    left.grid(row=0, column=0, sticky="nsew", padx=(0, 10))
    left.grid_rowconfigure(3, weight=1)

    right = tk.Frame(root, bg=THEME["CARD_BG"])
    right.grid(row=0, column=1, sticky="nsew")
    right.grid_rowconfigure(2, weight=1)
    right.grid_columnconfigure(0, weight=1)

    carrinho = {}
    estoque_cache = {}

    CUPOM_LARGURA = 40
    DADOS_LOJA = {
        "nome": "MERCADO EXEMPLO LTDA",
        "cnpj": "00.000.000/0001-00",
        "ie": "ISENTO",
        "endereco": "Rua Exemplo, 123 - Centro",
        "cidade": "Sua Cidade - UF",
        "telefone": "(11) 99999-9999",
        "mensagem": "OBRIGADO! VOLTE SEMPRE :)",
        "mostrar_cupom_nao_fiscal": True,
    }

    def carregar_cfg_loja():
        nonlocal CUPOM_LARGURA, DADOS_LOJA
        try:
            cfg = carregar_config_pdv()
            if isinstance(cfg, dict):
                CUPOM_LARGURA = int(cfg.get("cupom_largura", CUPOM_LARGURA))
                loja = cfg.get("loja", {})
                if isinstance(loja, dict):
                    for k, v in loja.items():
                        DADOS_LOJA[k] = v
        except Exception:
            pass

    def parse_preco(txt):
        s = str(txt).strip().replace(" ", "")
        if not s:
            return 0.0
        if "," in s:
            s = s.replace(".", "").replace(",", ".")
        return float(s)

    def recarregar_estoque_cache():
        nonlocal estoque_cache
        itens = ler_estoque()
        estoque_cache = {i[0].strip(): i for i in itens}
        return itens

    def buscar_produto_por_codigo(codigo):
        return estoque_cache.get(codigo.strip())

    def total_carrinho():
        return sum(item["preco"] * item["qtd"] for item in carrinho.values())

    # ====== status do caixa (sessão) ======
    lbl_status_caixa = tk.Label(left, text="", font=("Arial", 10, "bold"), bg=THEME["CARD_BG"])
    lbl_status_caixa.grid(row=0, column=0, sticky="e", pady=(0, 8), padx=10)

    def atualizar_status_caixa_ui():
        sess = get_sessao_aberta_cb()
        if not sess:
            lbl_status_caixa.config(text="CAIXA: FECHADO", fg="#B91C1C")
        else:
            sid, aberto_em, saldo_ini, operador = sess
            op = f" • {operador}" if operador else ""
            lbl_status_caixa.config(text=f"CAIXA: ABERTO (Sessão #{sid}{op})", fg="#065F46")

    def atualizar_pagamento_ui(event=None):
        total = total_carrinho()

        try:
            desc = parse_preco(entry_desc.get())
        except Exception:
            desc = 0.0
        if desc < 0:
            desc = 0.0

        total_liq = max(0.0, total - desc)

        if cb_pag.get() == "Dinheiro":
            entry_rec.config(state="normal")
        else:
            entry_rec.config(state="disabled")
            entry_rec.delete(0, tk.END)
            entry_rec.insert(0, "0")

        try:
            rec = parse_preco(entry_rec.get())
        except Exception:
            rec = 0.0

        troco = max(0.0, rec - total_liq) if cb_pag.get() == "Dinheiro" else 0.0

        lbl_total_val.config(text=f"R$ {brl(total)}")
        lbl_desc_val.config(text=f"R$ {brl(desc)}")
        lbl_total_liq_val.config(text=f"R$ {brl(total_liq)}")
        lbl_troco_val.config(text=f"R$ {brl(troco)}")

    def recarregar_carrinho():
        tabela_car.delete(*tabela_car.get_children())
        for cod, item in carrinho.items():
            subtotal = item["preco"] * item["qtd"]
            tabela_car.insert(
                "",
                tk.END,
                values=(cod, item["produto"], brl(item["preco"]), item["qtd"], brl(subtotal), "🗑️"),
            )
        atualizar_pagamento_ui()

    # ====== CUPOM ======
    def centralizar(txt: str, largura: int):
        txt = (txt or "").strip()
        if len(txt) >= largura:
            return txt[:largura]
        return txt.center(largura)

    def linha_sep(char="-"):
        return char * CUPOM_LARGURA

    def gerar_numero_venda(venda_id: int):
        # número “humano” do cupom
        return f"{datetime.now().strftime('%Y%m%d')}-{venda_id:06d}"

    def fmt_item_linha1(nome):
        nome = (nome or "").strip()
        if len(nome) <= CUPOM_LARGURA:
            return nome
        return nome[:CUPOM_LARGURA]

    def fmt_item_linha2(qtd, unit, total):
        left_txt = f"{qtd} x {brl(unit)}"
        right_txt = brl(total)
        espacos = max(1, CUPOM_LARGURA - len(left_txt) - len(right_txt))
        return f"{left_txt}{' ' * espacos}{right_txt}"

    def linha_valor(rotulo: str, valor: float):
        rotulo = (rotulo or "").strip()
        val = brl(valor)
        espacos = max(1, CUPOM_LARGURA - len(rotulo) - len(val))
        return f"{rotulo}{' ' * espacos}{val}"

    def gerar_texto_cupom(numero_venda, pagamento, desconto, recebido, troco):
        agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        subtotal = total_carrinho()
        total_pagar = max(0.0, subtotal - desconto)

        carregar_cfg_loja()

        linhas = []
        if DADOS_LOJA.get("mostrar_cupom_nao_fiscal", True):
            linhas.append(centralizar("CUPOM NAO FISCAL", CUPOM_LARGURA))

        linhas.append(centralizar(DADOS_LOJA.get("nome", ""), CUPOM_LARGURA))
        linhas.append(centralizar(f"CNPJ: {DADOS_LOJA.get('cnpj', '')}", CUPOM_LARGURA))
        linhas.append(centralizar(f"IE: {DADOS_LOJA.get('ie', '')}", CUPOM_LARGURA))
        linhas.append(centralizar(DADOS_LOJA.get("endereco", ""), CUPOM_LARGURA))
        linhas.append(centralizar(DADOS_LOJA.get("cidade", ""), CUPOM_LARGURA))
        linhas.append(centralizar(f"Fone: {DADOS_LOJA.get('telefone', '')}", CUPOM_LARGURA))

        linhas.append(linha_sep("="))
        linhas.append(f"DATA: {agora}")
        linhas.append(f"VENDA: {numero_venda}")
        linhas.append(linha_sep("-"))
        linhas.append("ITENS")
        linhas.append(linha_sep("-"))

        for cod, item in carrinho.items():
            nome = f"{item['produto']} ({cod})"
            qtd = item["qtd"]
            unit = item["preco"]
            total_item = unit * qtd
            linhas.append(fmt_item_linha1(nome))
            linhas.append(fmt_item_linha2(qtd, unit, total_item))

        linhas.append(linha_sep("-"))
        linhas.append(linha_valor("SUBTOTAL", subtotal))
        if desconto > 0:
            linhas.append(linha_valor("DESCONTO", desconto))
        linhas.append(linha_valor("TOTAL", total_pagar))
        linhas.append(linha_sep("-"))
        linhas.append(f"PAGAMENTO: {pagamento}")

        if pagamento == "Dinheiro":
            linhas.append(linha_valor("RECEBIDO", recebido))
            linhas.append(linha_valor("TROCO", troco))

        linhas.append(linha_sep("="))
        linhas.append(centralizar(DADOS_LOJA.get("mensagem", ""), CUPOM_LARGURA))
        linhas.append(linha_sep("="))

        return "\n".join(linhas)

    # ====== Preview / impressão ======
    def imprimir_comprovante_windows(texto: str):
        if os.name != "nt":
            raise RuntimeError("Impressão direta (print) está configurada apenas para Windows.")
        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt", mode="w", encoding="utf-8") as f:
            f.write(texto)
            caminho = f.name
        os.startfile(caminho, "print")

    def salvar_comprovante_dialog(texto: str, sugestao_nome="cupom.txt"):
        caminho = filedialog.asksaveasfilename(
            defaultextension=".txt",
            initialfile=sugestao_nome,
            filetypes=[("Arquivo de texto", "*.txt")],
        )
        if not caminho:
            return
        with open(caminho, "w", encoding="utf-8") as f:
            f.write(texto)
        messagebox.showinfo("Salvo", f"Cupom salvo em:\n{caminho}")

    def abrir_preview_comprovante(parent_widget, texto: str, numero_venda=None):
        win = tk.Toplevel(parent_widget)
        win.title("Pré-visualização do cupom")
        win.transient(parent_widget)
        win.grab_set()
        win.geometry("560x640")
        win.minsize(520, 520)

        topo = ttk.Frame(win, padding=10)
        topo.pack(fill="x")
        ttk.Label(topo, text="Cupom (estilo mercado)", font=("Segoe UI", 12, "bold")).pack(anchor="w")
        if numero_venda is not None:
            ttk.Label(topo, text=f"Venda Nº: {numero_venda}", font=("Segoe UI", 9)).pack(anchor="w", pady=(2, 0))

        centro = ttk.Frame(win, padding=(10, 0, 10, 10))
        centro.pack(fill="both", expand=True)

        txt = tk.Text(centro, wrap="none")
        txt.pack(side="left", fill="both", expand=True)
        txt.configure(font=("Consolas", 10))
        txt.insert("1.0", texto)
        txt.config(state="disabled")

        scroll_y = ttk.Scrollbar(centro, orient="vertical", command=txt.yview)
        scroll_y.pack(side="right", fill="y")
        txt.configure(yscrollcommand=scroll_y.set)

        rodape = ttk.Frame(win, padding=10)
        rodape.pack(fill="x")

        def on_imprimir():
            try:
                imprimir_comprovante_windows(texto)
            except Exception as e:
                messagebox.showerror("Erro ao imprimir", str(e))

        def on_salvar():
            nome = f"cupom_{numero_venda}.txt" if numero_venda else "cupom.txt"
            salvar_comprovante_dialog(texto, sugestao_nome=nome)

        ttk.Button(rodape, text="Salvar TXT", command=on_salvar).pack(side="left")
        ttk.Button(rodape, text="Imprimir", command=on_imprimir).pack(side="left", padx=8)
        ttk.Button(rodape, text="Fechar", command=win.destroy).pack(side="right")

        win.bind("<Escape>", lambda e: win.destroy())
        win.focus_set()

    # ===================== Ações do carrinho =====================
    def adicionar_ao_carrinho():
        codigo = entry_codigo.get().strip()
        qtd_txt = entry_qtd.get().strip()

        if not codigo:
            messagebox.showerror("Erro", "Informe o código do produto.")
            entry_codigo.focus()
            return

        qtd = 1
        if qtd_txt:
            try:
                qtd = int(qtd_txt)
                if qtd <= 0:
                    raise ValueError
            except ValueError:
                messagebox.showerror("Erro", "Quantidade deve ser um inteiro > 0.")
                entry_qtd.focus()
                return

        recarregar_estoque_cache()
        produto = buscar_produto_por_codigo(codigo)
        if not produto:
            messagebox.showerror("Erro", "Produto não encontrado pelo código.")
            entry_codigo.focus()
            return

        cod, nome, custo_txt, venda_txt, estoque_txt = produto
        estoque_atual = int(estoque_txt)
        preco_venda = parse_preco(venda_txt)
        custo = parse_preco(custo_txt)

        qtd_no_carrinho = carrinho.get(cod, {}).get("qtd", 0)
        if qtd_no_carrinho + qtd > estoque_atual:
            messagebox.showerror("Erro", "Quantidade no carrinho excede o estoque disponível.")
            return

        if cod in carrinho:
            carrinho[cod]["qtd"] += qtd
        else:
            carrinho[cod] = {"produto": nome, "preco": preco_venda, "custo": custo, "qtd": qtd}

        recarregar_carrinho()
        entry_codigo.delete(0, tk.END)
        entry_qtd.delete(0, tk.END)
        entry_codigo.focus()

    def remover_item_por_codigo(cod):
        if cod not in carrinho:
            return
        carrinho.pop(cod, None)
        recarregar_carrinho()

    def remover_item_selecionado():
        sel = tabela_car.selection()
        if not sel:
            messagebox.showwarning("Atenção", "Selecione um item no carrinho.")
            return
        cod = tabela_car.item(sel[0], "values")[0]
        remover_item_por_codigo(cod)

    def limpar_carrinho(confirmar=True):
        if confirmar and carrinho:
            if not messagebox.askyesno("Confirmar", "Deseja limpar o carrinho?"):
                return
        carrinho.clear()
        recarregar_carrinho()
        entry_codigo.focus()

    def aumentar_qtd():
        sel = tabela_car.selection()
        if not sel:
            messagebox.showwarning("Atenção", "Selecione um item no carrinho.")
            return
        cod = tabela_car.item(sel[0], "values")[0]
        recarregar_estoque_cache()
        produto = buscar_produto_por_codigo(cod)
        if not produto:
            return
        estoque_atual = int(produto[4])
        if carrinho[cod]["qtd"] + 1 > estoque_atual:
            messagebox.showerror("Erro", "Não há estoque suficiente.")
            return
        carrinho[cod]["qtd"] += 1
        recarregar_carrinho()

    def diminuir_qtd():
        sel = tabela_car.selection()
        if not sel:
            messagebox.showwarning("Atenção", "Selecione um item no carrinho.")
            return
        cod = tabela_car.item(sel[0], "values")[0]
        carrinho[cod]["qtd"] -= 1
        if carrinho[cod]["qtd"] <= 0:
            carrinho.pop(cod, None)
        recarregar_carrinho()

    def editar_item_popup(cod):
        top = tk.Toplevel(caixa)
        top.title("Editar item")
        top.geometry("360x220")
        top.resizable(False, False)

        tk.Label(top, text=f"{carrinho[cod]['produto']}  ({cod})", font=("Arial", 10, "bold")).pack(pady=(10, 6))

        frame = tk.Frame(top)
        frame.pack(pady=6)

        tk.Label(frame, text="Quantidade:").grid(row=0, column=0, padx=6, pady=6, sticky="e")
        entry_q = tk.Entry(frame, width=14)
        entry_q.grid(row=0, column=1, padx=6, pady=6)
        entry_q.insert(0, str(carrinho[cod]["qtd"]))

        tk.Label(frame, text="Preço unitário (R$):").grid(row=1, column=0, padx=6, pady=6, sticky="e")
        entry_p = tk.Entry(frame, width=14)
        entry_p.grid(row=1, column=1, padx=6, pady=6)
        entry_p.insert(0, brl(carrinho[cod]["preco"]))

        def salvar():
            try:
                nova_qtd = int(entry_q.get().strip())
                if nova_qtd <= 0:
                    raise ValueError
            except Exception:
                messagebox.showerror("Erro", "Quantidade deve ser um inteiro > 0.")
                return

            try:
                novo_preco = parse_preco(entry_p.get().strip())
                if novo_preco < 0:
                    raise ValueError
            except Exception:
                messagebox.showerror("Erro", "Preço inválido. Ex: 150,00")
                return

            recarregar_estoque_cache()
            produto = buscar_produto_por_codigo(cod)
            if produto:
                estoque_atual = int(produto[4])
                if nova_qtd > estoque_atual:
                    messagebox.showerror("Erro", "Quantidade maior que o estoque disponível.")
                    return

            carrinho[cod]["qtd"] = nova_qtd
            carrinho[cod]["preco"] = float(novo_preco)

            recarregar_carrinho()
            top.destroy()

        btns = tk.Frame(top)
        btns.pack(pady=10)

        tk.Button(btns, text="Salvar", width=10, command=salvar).pack(side="left", padx=6)
        tk.Button(btns, text="Cancelar", width=10, command=top.destroy).pack(side="left", padx=6)

        top.bind("<Return>", lambda e: salvar())
        top.bind("<Escape>", lambda e: top.destroy())
        top.grab_set()
        entry_q.focus()

    def editar_item_selecionado():
        sel = tabela_car.selection()
        if not sel:
            return
        cod = tabela_car.item(sel[0], "values")[0]
        if cod in carrinho:
            editar_item_popup(cod)

    def duplo_clique_carrinho(event=None):
        sel = tabela_car.selection()
        if not sel:
            return
        cod = tabela_car.item(sel[0], "values")[0]
        if cod in carrinho:
            editar_item_popup(cod)

    def clique_carrinho(event):
        item_id = tabela_car.identify_row(event.y)
        col = tabela_car.identify_column(event.x)
        if not item_id:
            return
        if col == "#6":
            cod = tabela_car.item(item_id, "values")[0]
            remover_item_por_codigo(cod)

    def _map_forma_pagamento(rotulo: str) -> str:
        rotulo = (rotulo or "").strip().lower()
        if rotulo == "pix":
            return "PIX"
        if rotulo == "dinheiro":
            return "DINHEIRO"
        if "crédito" in rotulo or "credito" in rotulo:
            return "CARTAO_CREDITO"
        if "débito" in rotulo or "debito" in rotulo:
            return "CARTAO_DEBITO"
        return "OUTRO"

    def finalizar_venda():
        sess = get_sessao_aberta_cb()
        if not sess:
            messagebox.showwarning("Caixa fechado", "Abra o caixa antes de finalizar vendas.\nMenu > Abrir Caixa")
            return
        sessao_id = int(sess[0])

        if not carrinho:
            messagebox.showwarning("Atenção", "Carrinho vazio.")
            return

        recarregar_estoque_cache()

        for cod, item in carrinho.items():
            prod = buscar_produto_por_codigo(cod)
            if not prod:
                messagebox.showerror("Erro", f"Produto {cod} não existe mais no estoque.")
                return
            qtd_disp = int(prod[4])
            if item["qtd"] > qtd_disp:
                messagebox.showerror("Erro", f"Estoque insuficiente para {item['produto']}.")
                return

        pagamento_rotulo = cb_pag.get()
        forma_pag = _map_forma_pagamento(pagamento_rotulo)

        try:
            desconto = parse_preco(entry_desc.get())
        except Exception:
            messagebox.showerror("Erro", "Desconto inválido.")
            return
        if desconto < 0:
            messagebox.showerror("Erro", "Desconto não pode ser negativo.")
            return

        subtotal = total_carrinho()
        total_liq = max(0.0, subtotal - desconto)

        try:
            recebido = parse_preco(entry_rec.get())
        except Exception:
            recebido = 0.0

        if pagamento_rotulo == "Dinheiro":
            if recebido < total_liq:
                messagebox.showerror("Erro", "Valor recebido menor que o total com desconto.")
                return
            troco = recebido - total_liq
        else:
            troco = 0.0

        # Monta itens para salvar no DB (novo modelo)
        itens_db = []
        for cod, item in carrinho.items():
            total_item = item["preco"] * item["qtd"]
            itens_db.append({
                "codigo": cod,
                "produto": item["produto"],
                "preco_unit": float(item["preco"]),
                "preco_custo": float(item.get("custo") or 0.0),
                "qtd": int(item["qtd"]),
                "total_item": float(total_item),
            })

        # Baixa estoque no cache -> escreve estoque
        for cod, item in carrinho.items():
            linha = estoque_cache[cod]
            qtd_disp = int(linha[4])
            linha[4] = str(qtd_disp - item["qtd"])

        escrever_estoque(list(estoque_cache.values()))

        # Salva venda completa
        venda_id = registrar_venda_completa(
            sessao_id=sessao_id,
            itens=itens_db,
            forma_pagamento=forma_pag,
            subtotal=subtotal,
            desconto=desconto,
            total=total_liq,
            recebido=recebido,
            troco=troco,
        )

        numero_venda = gerar_numero_venda(venda_id)
        texto = gerar_texto_cupom(numero_venda, pagamento_rotulo, desconto, recebido, troco)
        abrir_preview_comprovante(caixa, texto, numero_venda=numero_venda)

        carrinho.clear()
        recarregar_carrinho()
        entry_desc.delete(0, tk.END)
        entry_desc.insert(0, "0")
        cb_pag.set("Pix")
        entry_rec.config(state="disabled")
        entry_rec.delete(0, tk.END)
        entry_rec.insert(0, "0")
        entry_codigo.focus()

    # ===================== ESQUERDA =====================
    header = tk.Label(left, text="Caixa (PDV)", font=("Arial", 14), bg=THEME["CARD_BG"])
    header.grid(row=0, column=0, sticky="w", pady=(0, 8), padx=10)

    frame_busca = tk.Frame(left, bg=THEME["CARD_BG"])
    frame_busca.grid(row=1, column=0, sticky="ew", pady=(0, 8), padx=10)
    frame_busca.grid_columnconfigure(1, weight=1)

    tk.Label(frame_busca, text="Código:", bg=THEME["CARD_BG"]).grid(row=0, column=0, padx=6, pady=4, sticky="e")
    entry_codigo = tk.Entry(frame_busca, width=28, font=("Arial", 12))
    entry_codigo.grid(row=0, column=1, padx=6, pady=4, sticky="w")

    tk.Label(frame_busca, text="Qtd:", bg=THEME["CARD_BG"]).grid(row=0, column=2, padx=6, pady=4, sticky="e")
    entry_qtd = tk.Entry(frame_busca, width=8, font=("Arial", 12))
    entry_qtd.grid(row=0, column=3, padx=6, pady=4, sticky="w")

    btn_add = tk.Button(frame_busca, text="Adicionar (Enter)", width=16, command=adicionar_ao_carrinho)
    btn_add.grid(row=0, column=4, padx=6, pady=4)

    tk.Label(left, text="Carrinho", bg=THEME["CARD_BG"]).grid(row=2, column=0, sticky="w", pady=(4, 4), padx=10)

    colunas_car = ("Codigo", "Produto", "Preço Unit.", "Qtd", "Subtotal", "Ação")
    tabela_car = ttk.Treeview(left, columns=colunas_car, show="headings", height=16)

    for col in colunas_car:
        tabela_car.heading(col, text=col)
        tabela_car.column(col, anchor="center", width=130)

    tabela_car.column("Produto", width=320)
    tabela_car.column("Ação", width=60)
    tabela_car.grid(row=3, column=0, sticky="nsew", padx=10, pady=(0, 6))

    frame_car_btn = tk.Frame(left, bg=THEME["CARD_BG"])
    frame_car_btn.grid(row=4, column=0, sticky="ew", pady=8, padx=10)

    tk.Button(frame_car_btn, text="+ Qtd", width=10, command=aumentar_qtd).pack(side="left", padx=6)
    tk.Button(frame_car_btn, text="- Qtd", width=10, command=diminuir_qtd).pack(side="left", padx=6)
    tk.Button(frame_car_btn, text="Remover (Del)", width=14, command=remover_item_selecionado).pack(side="left", padx=6)
    tk.Button(frame_car_btn, text="Limpar", width=12, command=lambda: limpar_carrinho(confirmar=True)).pack(side="left", padx=6)
    tk.Label(frame_car_btn, text="Editar: duplo clique ou F2", bg=THEME["CARD_BG"]).pack(side="left", padx=10)

    # ===================== DIREITA =====================
    box_pag = tk.LabelFrame(right, text="Pagamento", padx=10, pady=10, bg=THEME["CARD_BG"])
    box_pag.grid(row=0, column=0, sticky="new", pady=(0, 10), padx=10)

    tk.Label(box_pag, text="Forma:", bg=THEME["CARD_BG"]).grid(row=0, column=0, sticky="e", padx=6, pady=6)
    cb_pag = ttk.Combobox(
        box_pag, values=["Pix", "Dinheiro", "Cartão Crédito", "Cartão Débito"], state="readonly", width=18
    )
    cb_pag.grid(row=0, column=1, sticky="w", padx=6, pady=6)
    cb_pag.set("Pix")

    tk.Label(box_pag, text="Desconto (R$):", bg=THEME["CARD_BG"]).grid(row=1, column=0, sticky="e", padx=6, pady=6)
    entry_desc = tk.Entry(box_pag, width=12)
    entry_desc.grid(row=1, column=1, sticky="w", padx=6, pady=6)
    entry_desc.insert(0, "0")

    tk.Label(box_pag, text="Recebido (dinheiro):", bg=THEME["CARD_BG"]).grid(row=2, column=0, sticky="e", padx=6, pady=6)
    entry_rec = tk.Entry(box_pag, width=12)
    entry_rec.grid(row=2, column=1, sticky="w", padx=6, pady=6)
    entry_rec.insert(0, "0")
    entry_rec.config(state="disabled")

    resumo = tk.LabelFrame(right, text="Resumo", padx=10, pady=10, bg=THEME["CARD_BG"])
    resumo.grid(row=1, column=0, sticky="new", pady=(0, 10), padx=10)

    def linha_resumo(parent_frame, r, titulo):
        tk.Label(parent_frame, text=titulo, bg=THEME["CARD_BG"]).grid(row=r, column=0, sticky="w", padx=4, pady=4)
        lbl = tk.Label(parent_frame, text="R$ 0,00", font=("Arial", 12), bg=THEME["CARD_BG"])
        lbl.grid(row=r, column=1, sticky="e", padx=4, pady=4)
        parent_frame.grid_columnconfigure(1, weight=1)
        return lbl

    lbl_total_val = linha_resumo(resumo, 0, "Total:")
    lbl_desc_val = linha_resumo(resumo, 1, "Desconto:")
    lbl_total_liq_val = linha_resumo(resumo, 2, "Total a pagar:")
    lbl_troco_val = linha_resumo(resumo, 3, "Troco:")

    btn_finish = tk.Button(
        right, text="FINALIZAR VENDA (F4)", font=("Arial", 12, "bold"), height=2, command=finalizar_venda
    )
    btn_finish.grid(row=3, column=0, sticky="ew", pady=(10, 10), padx=10)

    # ===================== Eventos / atalhos =====================
    entry_codigo.bind("<Return>", lambda e: adicionar_ao_carrinho())
    entry_qtd.bind("<Return>", lambda e: adicionar_ao_carrinho())

    tabela_car.bind("<Double-1>", duplo_clique_carrinho)
    tabela_car.bind("<Button-1>", clique_carrinho)

    cb_pag.bind("<<ComboboxSelected>>", atualizar_pagamento_ui)
    entry_desc.bind("<KeyRelease>", atualizar_pagamento_ui)
    entry_rec.bind("<KeyRelease>", atualizar_pagamento_ui)

    caixa.bind_all("<F4>", lambda e: finalizar_venda())
    caixa.bind_all("<Delete>", lambda e: remover_item_selecionado())
    caixa.bind_all("<F2>", lambda e: editar_item_selecionado())

    carregar_cfg_loja()
    recarregar_estoque_cache()
    recarregar_carrinho()
    atualizar_status_caixa_ui()
    entry_codigo.focus()

    return caixa


# =========================================================
# FRAMES
# =========================================================
class MenuFrame(tk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent, bg=THEME["BG_APP"])
        tk.Label(self, text="MENU PRINCIPAL", font=("Arial", 22, "bold"), bg=THEME["BG_APP"]).pack(pady=18)

        box = tk.Frame(self, bg=THEME["BG_APP"])
        box.pack(pady=10)

        tk.Button(box, text="🔓 Abrir Caixa", width=26, height=2,
                  command=controller.abrir_caixa_ui).pack(pady=6)

        tk.Button(box, text="🔒 Fechar Caixa", width=26, height=2,
                  command=controller.fechar_caixa_ui).pack(pady=6)

        tk.Frame(box, height=10, bg=THEME["BG_APP"]).pack()

        tk.Button(box, text="🧾 Caixa (PDV)", width=26, height=2,
                  command=lambda: controller.show("CaixaFrame")).pack(pady=6)

        tk.Button(box, text="📦 Estoque", width=26, height=2,
                  command=lambda: controller.show("EstoqueFrame")).pack(pady=6)

        tk.Button(box, text="📈 Histórico de Vendas", width=26, height=2,
                  command=lambda: controller.show("HistoricoFrame")).pack(pady=6)

        tk.Button(box, text="📅 Painel do Proprietário", width=26, height=2,
                  command=lambda: controller.show("PainelProprietarioFrame")).pack(pady=6)


class CaixaFrame(tk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent, bg=THEME["BG_APP"])
        self.controller = controller
        self._montado = False

    def on_show(self):
        if not self._montado:
            montar_caixa(
                self,
                ler_estoque=self.controller.ler_estoque,
                escrever_estoque=self.controller.escrever_estoque,
                registrar_venda_completa=self.controller.registrar_venda_completa,
                carregar_na_tabela=carregar_na_tabela,
                get_sessao_aberta_cb=self.controller.get_sessao_aberta,
            )
            self._montado = True


class EstoqueFrame(tk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent, bg=THEME["BG_APP"])
        self.controller = controller
        self.codigo_selecionado = None

        form = tk.Frame(self, bg=THEME["CARD_BG"])
        form.pack(pady=10, padx=10, fill="x")

        tk.Label(form, text="Código (barras)", bg=THEME["CARD_BG"]).grid(row=0, column=0, padx=6, pady=4, sticky="e")
        self.entry_codigo = tk.Entry(form, width=28)
        self.entry_codigo.grid(row=0, column=1, padx=6, pady=4)

        tk.Label(form, text="Produto", bg=THEME["CARD_BG"]).grid(row=1, column=0, padx=6, pady=4, sticky="e")
        self.entry_nome = tk.Entry(form, width=28)
        self.entry_nome.grid(row=1, column=1, padx=6, pady=4)

        tk.Label(form, text="Preço de custo", bg=THEME["CARD_BG"]).grid(row=2, column=0, padx=6, pady=4, sticky="e")
        self.entry_custo = tk.Entry(form, width=28)
        self.entry_custo.grid(row=2, column=1, padx=6, pady=4)

        tk.Label(form, text="% Lucro", bg=THEME["CARD_BG"]).grid(row=3, column=0, padx=6, pady=4, sticky="e")
        self.entry_percent = tk.Entry(form, width=28)
        self.entry_percent.grid(row=3, column=1, padx=6, pady=4)

        tk.Label(form, text="Preço venda (auto)", bg=THEME["CARD_BG"]).grid(row=4, column=0, padx=6, pady=4, sticky="e")
        self.entry_venda = tk.Entry(form, width=28, state="readonly")
        self.entry_venda.grid(row=4, column=1, padx=6, pady=4)

        tk.Label(form, text="Quantidade", bg=THEME["CARD_BG"]).grid(row=5, column=0, padx=6, pady=4, sticky="e")
        self.entry_qtd = tk.Entry(form, width=28)
        self.entry_qtd.grid(row=5, column=1, padx=6, pady=4)

        colunas = ("Codigo", "Produto", "Custo", "Venda", "Quantidade", "% Lucro")
        self.tabela = ttk.Treeview(self, columns=colunas, show="headings", height=14)
        for col in colunas:
            self.tabela.heading(col, text=col)
            self.tabela.column(col, anchor="center", width=130)
        self.tabela.column("Produto", width=320)
        self.tabela.pack(padx=10, pady=10, fill="both", expand=True)

        btns = tk.Frame(self, bg=THEME["BG_APP"])
        btns.pack(pady=8)
        tk.Button(btns, text="Salvar", width=14, command=self.salvar).pack(side="left", padx=5)
        tk.Button(btns, text="Atualizar", width=14, command=self.atualizar).pack(side="left", padx=5)
        tk.Button(btns, text="Excluir", width=14, command=self.excluir).pack(side="left", padx=5)
        tk.Button(btns, text="Limpar", width=14, command=self.limpar).pack(side="left", padx=5)
        tk.Button(btns, text="Recarregar", width=14, command=self.recarregar).pack(side="left", padx=5)

        self.tabela.bind("<<TreeviewSelect>>", self.ao_selecionar)
        self.entry_custo.bind("<KeyRelease>", self.calcular_venda)
        self.entry_percent.bind("<KeyRelease>", self.calcular_venda)

    def on_show(self):
        self.recarregar()

    def set_readonly(self, entry, value):
        entry.config(state="normal")
        entry.delete(0, tk.END)
        entry.insert(0, value)
        entry.config(state="readonly")

    def calcular_venda(self, *_):
        try:
            custo = to_float(self.entry_custo.get())
            perc = to_float(self.entry_percent.get())
            if custo < 0 or perc < 0:
                raise ValueError
            venda = custo * (1 + perc / 100.0)
            self.set_readonly(self.entry_venda, brl(venda))
        except Exception:
            self.set_readonly(self.entry_venda, "")

    def montar_linhas(self):
        linhas = []
        for codigo, nome, custo_txt, venda_txt, qtd_txt in self.controller.ler_estoque():
            try:
                custo = to_float(custo_txt)
                venda = to_float(venda_txt)
                perc = ((venda - custo) / custo * 100.0) if custo > 0 else 0.0
            except Exception:
                perc = 0.0
            linhas.append([codigo, nome, custo_txt, venda_txt, qtd_txt, f"{perc:.1f}%".replace(".", ",")])
        return linhas

    def recarregar(self):
        carregar_na_tabela(self.tabela, self.montar_linhas())

    def limpar(self):
        self.codigo_selecionado = None
        self.entry_codigo.delete(0, tk.END)
        self.entry_nome.delete(0, tk.END)
        self.entry_custo.delete(0, tk.END)
        self.entry_percent.delete(0, tk.END)
        self.set_readonly(self.entry_venda, "")
        self.entry_qtd.delete(0, tk.END)

    def ao_selecionar(self, _=None):
        sel = self.tabela.selection()
        if not sel:
            return
        codigo, produto, custo_txt, venda_txt, qtd_txt, perc_txt = self.tabela.item(sel[0], "values")
        self.codigo_selecionado = codigo

        self.entry_codigo.delete(0, tk.END)
        self.entry_codigo.insert(0, codigo)
        self.entry_nome.delete(0, tk.END)
        self.entry_nome.insert(0, produto)
        self.entry_custo.delete(0, tk.END)
        self.entry_custo.insert(0, custo_txt)
        self.entry_percent.delete(0, tk.END)
        self.entry_percent.insert(0, perc_txt.replace("%", "").strip())
        self.calcular_venda()
        self.entry_qtd.delete(0, tk.END)
        self.entry_qtd.insert(0, qtd_txt)

    def salvar(self):
        codigo = self.entry_codigo.get().strip()
        nome = self.entry_nome.get().strip()
        custo_txt = self.entry_custo.get().strip()
        venda_txt = self.entry_venda.get().strip()
        qtd_txt = self.entry_qtd.get().strip()

        if not (codigo and nome and custo_txt and venda_txt and qtd_txt):
            messagebox.showerror("Erro", "Preencha todos os campos.")
            return

        for item in self.controller.ler_estoque():
            if item[0].strip() == codigo:
                messagebox.showerror("Erro", "Já existe um produto com esse código.")
                return

        try:
            custo = to_float(custo_txt)
            venda = to_float(venda_txt)
            qtd = int(qtd_txt)
            if custo < 0 or venda <= 0 or qtd < 0:
                raise ValueError
        except Exception:
            messagebox.showerror("Erro", "Custo>=0, Venda>0, Quantidade inteiro >=0.")
            return

        itens = self.controller.ler_estoque()
        itens.append([codigo, nome, brl(custo), brl(venda), str(qtd)])
        self.controller.escrever_estoque(itens)

        self.recarregar()
        self.limpar()
        messagebox.showinfo("OK", "Produto cadastrado!")

    def atualizar(self):
        if not self.codigo_selecionado:
            messagebox.showwarning("Atenção", "Selecione um produto na tabela para editar.")
            return

        codigo_novo = self.entry_codigo.get().strip()
        nome = self.entry_nome.get().strip()
        custo_txt = self.entry_custo.get().strip()
        venda_txt = self.entry_venda.get().strip()
        qtd_txt = self.entry_qtd.get().strip()

        if not (codigo_novo and nome and custo_txt and venda_txt and qtd_txt):
            messagebox.showerror("Erro", "Preencha todos os campos.")
            return

        if codigo_novo != self.codigo_selecionado:
            for item in self.controller.ler_estoque():
                if item[0].strip() == codigo_novo:
                    messagebox.showerror("Erro", "Já existe um produto com esse código.")
                    return

        try:
            custo = to_float(custo_txt)
            venda = to_float(venda_txt)
            qtd = int(qtd_txt)
            if custo < 0 or venda <= 0 or qtd < 0:
                raise ValueError
        except Exception:
            messagebox.showerror("Erro", "Custo>=0, Venda>0, Quantidade inteiro >=0.")
            return

        itens = self.controller.ler_estoque()
        novos = []
        for row in itens:
            if row[0] == self.codigo_selecionado:
                novos.append([codigo_novo, nome, brl(custo), brl(venda), str(qtd)])
            else:
                novos.append(row)

        self.controller.escrever_estoque(novos)
        self.recarregar()
        self.limpar()
        messagebox.showinfo("OK", "Produto atualizado!")

    def excluir(self):
        sel = self.tabela.selection()
        if not sel:
            messagebox.showwarning("Atenção", "Selecione um produto.")
            return
        codigo = self.tabela.item(sel[0], "values")[0]
        nome = self.tabela.item(sel[0], "values")[1]

        if not messagebox.askyesno("Confirmar", f"Excluir '{nome}' (cód: {codigo})?"):
            return

        itens = self.controller.ler_estoque()
        itens = [r for r in itens if r[0] != codigo]
        self.controller.escrever_estoque(itens)

        self.recarregar()
        self.limpar()


class HistoricoFrame(tk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent, bg=THEME["BG_APP"])
        self.controller = controller

        filtro = tk.Frame(self, bg=THEME["CARD_BG"])
        filtro.pack(fill="x", padx=10, pady=8)

        tk.Label(filtro, text="Filtrar por produto:", bg=THEME["CARD_BG"]).pack(side="left")
        self.entry_filtro = tk.Entry(filtro, width=35)
        self.entry_filtro.pack(side="left", padx=8)

        tk.Button(filtro, text="Atualizar", command=self.atualizar).pack(side="left")

        colunas = ("DataHora", "Codigo", "Produto", "Preço Unit.", "Qtd", "Total")
        self.tabela = ttk.Treeview(self, columns=colunas, show="headings", height=16)
        for col in colunas:
            self.tabela.heading(col, text=col)
            self.tabela.column(col, anchor="center", width=140)
        self.tabela.column("Produto", width=320)
        self.tabela.pack(fill="both", expand=True, padx=10, pady=10)

        self.lbl_total = tk.Label(self, text="Total vendido: R$ 0,00", font=("Arial", 12), bg=THEME["BG_APP"])
        self.lbl_total.pack(pady=(0, 10))

    def on_show(self):
        self.atualizar()

    def atualizar(self):
        filtro = self.entry_filtro.get().strip().lower()

        vendas = self.controller.ler_vendas()
        if filtro:
            vendas = [v for v in vendas if filtro in v[2].lower()]

        carregar_na_tabela(self.tabela, vendas)

        soma = 0.0
        for v in vendas:
            try:
                soma += to_float(v[5])
            except Exception:
                pass
        self.lbl_total.config(text=f"Total vendido: R$ {brl(soma)}")


# =========================================================
# APP PRINCIPAL
# =========================================================
class PDVApp(tk.Tk):
    def __init__(self):
        super().__init__()

        self.title(APP_TITLE)
        self.geometry("1100x700")
        self.minsize(960, 600)
        self.configure(bg=THEME["BG_APP"])

        garantir_config()
        inicializar_banco()

        self.historico = []
        self.frame_atual = None

        # ===================== Barra superior =====================
        barra = tk.Frame(self, height=48, bg=THEME["BAR_BG"])
        barra.pack(fill="x")

        self.btn_voltar = tk.Button(
            barra, text="⬅ Voltar", width=10, command=self.voltar,
            bg=THEME["BTN_BG"], relief="flat"
        )
        self.btn_voltar.pack(side="left", padx=8, pady=8)

        self.btn_menu = tk.Button(
            barra, text="🏠 Menu", width=10, command=self.ir_menu,
            bg=THEME["BTN_BG"], relief="flat"
        )
        self.btn_menu.pack(side="left", pady=8)

        self.lbl_titulo = tk.Label(
            barra, text="Menu Principal", font=("Arial", 14),
            bg=THEME["BAR_BG"], fg=THEME["BAR_FG"]
        )
        self.lbl_titulo.pack(side="left", padx=12)

        self.lbl_caixa_topo = tk.Label(
            barra, text="", font=("Arial", 10, "bold"),
            bg=THEME["BAR_BG"], fg=THEME["BAR_FG"]
        )
        self.lbl_caixa_topo.pack(side="right", padx=12)

        tk.Frame(self, height=1, bg="#cccccc").pack(fill="x")

        container = tk.Frame(self, bg=THEME["BG_APP"])
        container.pack(fill="both", expand=True)
        container.grid_rowconfigure(0, weight=1)
        container.grid_columnconfigure(0, weight=1)

        self.frames = {}

        # Frames padrão
        for F in (MenuFrame, EstoqueFrame, CaixaFrame, HistoricoFrame):
            frame = F(container, controller=self)
            self.frames[F.__name__] = frame
            frame.grid(row=0, column=0, sticky="nsew")

        # Painel do Proprietário
        painel = PainelProprietarioFrame(
            container,
            controller=self,
            listar_vendas_por_periodo=listar_vendas_por_periodo,
            total_por_periodo=total_por_periodo,
            totais_por_dia_do_mes=totais_por_dia_do_mes,
        )
        self.frames["PainelProprietarioFrame"] = painel
        painel.grid(row=0, column=0, sticky="nsew")

        self.titulos = {
            "MenuFrame": "Menu Principal",
            "EstoqueFrame": "Estoque",
            "CaixaFrame": "Caixa (PDV)",
            "HistoricoFrame": "Histórico de Vendas",
            "PainelProprietarioFrame": "Painel do Proprietário",
        }

        self.bind("<Escape>", lambda e: self.voltar())
        self.show("MenuFrame", registrar=False)
        self.atualizar_status_caixa_topo()

    # ---------- Caixa: UI ----------
    def atualizar_status_caixa_topo(self):
        sess = self.get_sessao_aberta()
        if not sess:
            self.lbl_caixa_topo.config(text="CAIXA: FECHADO")
        else:
            sid, aberto_em, saldo_ini, operador = sess
            op = f" • {operador}" if operador else ""
            self.lbl_caixa_topo.config(text=f"CAIXA: ABERTO (Sessão #{sid}{op})")

    def abrir_caixa_ui(self):
        if self.get_sessao_aberta():
            messagebox.showinfo("Caixa", "Já existe um caixa ABERTO. Para abrir outro, feche o caixa atual.")
            return

        win = tk.Toplevel(self)
        win.title("Abrir Caixa")
        win.geometry("420x240")
        win.resizable(False, False)
        win.transient(self)
        win.grab_set()

        frm = tk.Frame(win, padx=12, pady=12)
        frm.pack(fill="both", expand=True)

        tk.Label(frm, text="Abertura de Caixa", font=("Arial", 12, "bold")).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 10))

        tk.Label(frm, text="Saldo inicial (fundo):").grid(row=1, column=0, sticky="e", padx=6, pady=6)
        e_saldo = tk.Entry(frm, width=18)
        e_saldo.grid(row=1, column=1, sticky="w", padx=6, pady=6)
        e_saldo.insert(0, "0")

        tk.Label(frm, text="Operador (opcional):").grid(row=2, column=0, sticky="e", padx=6, pady=6)
        e_op = tk.Entry(frm, width=24)
        e_op.grid(row=2, column=1, sticky="w", padx=6, pady=6)

        tk.Label(frm, text="Observação (opcional):").grid(row=3, column=0, sticky="e", padx=6, pady=6)
        e_obs = tk.Entry(frm, width=24)
        e_obs.grid(row=3, column=1, sticky="w", padx=6, pady=6)

        btns = tk.Frame(frm)
        btns.grid(row=4, column=0, columnspan=2, pady=14, sticky="e")

        def confirmar():
            try:
                saldo = to_float(e_saldo.get())
                if saldo < 0:
                    raise ValueError
            except Exception:
                messagebox.showerror("Erro", "Saldo inicial inválido.")
                return
            try:
                sid = abrir_caixa_db(saldo_inicial=saldo, operador=e_op.get().strip(), obs=e_obs.get().strip())
            except Exception as ex:
                messagebox.showerror("Erro", str(ex))
                return

            messagebox.showinfo("OK", f"Caixa ABERTO com sucesso!\nSessão #{sid}")
            win.destroy()
            self.atualizar_status_caixa_topo()

        tk.Button(btns, text="Cancelar", width=12, command=win.destroy).pack(side="right", padx=6)
        tk.Button(btns, text="Abrir", width=12, command=confirmar).pack(side="right")

        win.bind("<Escape>", lambda e: win.destroy())
        e_saldo.focus_set()

    def fechar_caixa_ui(self):
        sess = self.get_sessao_aberta()
        if not sess:
            messagebox.showwarning("Caixa", "Não existe caixa ABERTO.")
            return

        sessao_id, aberto_em, saldo_ini, operador = sess
        saldo_ini = float(saldo_ini or 0.0)

        rel = relatorio_pagamentos_sessao(sessao_id)
        saldo_inicial, total_vendas, saldo_final_sistema, _ = totais_sessao(sessao_id)

        win = tk.Toplevel(self)
        win.title("Fechar Caixa")
        win.geometry("560x520")
        win.minsize(520, 460)
        win.transient(self)
        win.grab_set()

        topo = tk.Frame(win, padx=12, pady=12)
        topo.pack(fill="x")
        tk.Label(topo, text=f"Fechamento de Caixa — Sessão #{sessao_id}", font=("Arial", 12, "bold")).pack(anchor="w")
        info = f"Aberto em: {aberto_em}  |  Saldo inicial: R$ {brl(saldo_ini)}"
        if operador:
            info += f"  |  Operador: {operador}"
        tk.Label(topo, text=info).pack(anchor="w", pady=(4, 0))

        mid = tk.Frame(win, padx=12, pady=0)
        mid.pack(fill="both", expand=True)

        tk.Label(mid, text="Relatório por forma de pagamento:", font=("Arial", 10, "bold")).pack(anchor="w", pady=(10, 6))

        cols = ("Forma", "Total (R$)")
        tv = ttk.Treeview(mid, columns=cols, show="headings", height=8)
        tv.heading("Forma", text="Forma")
        tv.heading("Total (R$)", text="Total (R$)")
        tv.column("Forma", width=220, anchor="w")
        tv.column("Total (R$)", width=140, anchor="e")
        tv.pack(fill="x")

        total_rel = 0.0
        for forma, total in rel:
            tv.insert("", "end", values=(forma, brl(total)))
            total_rel += float(total or 0.0)

        resumo = tk.Frame(mid, pady=10)
        resumo.pack(fill="x")

        tk.Label(resumo, text=f"Total de vendas (sistema): R$ {brl(total_vendas)}", font=("Arial", 10, "bold")).pack(anchor="w")
        tk.Label(resumo, text=f"Saldo final (sistema): R$ {brl(saldo_final_sistema)}").pack(anchor="w", pady=(2, 0))

        frm_inf = tk.Frame(mid)
        frm_inf.pack(fill="x", pady=10)

        tk.Label(frm_inf, text="Valor contado (informado):").grid(row=0, column=0, sticky="e", padx=6, pady=6)
        e_contado = tk.Entry(frm_inf, width=18)
        e_contado.grid(row=0, column=1, sticky="w", padx=6, pady=6)
        e_contado.insert(0, brl(saldo_final_sistema))

        tk.Label(frm_inf, text="Observação (opcional):").grid(row=1, column=0, sticky="e", padx=6, pady=6)
        e_obs = tk.Entry(frm_inf, width=28)
        e_obs.grid(row=1, column=1, sticky="w", padx=6, pady=6)

        rodape = tk.Frame(win, padx=12, pady=12)
        rodape.pack(fill="x")

        def confirmar():
            try:
                contado = to_float(e_contado.get())
            except Exception:
                messagebox.showerror("Erro", "Valor contado inválido.")
                return

            try:
                resultado = fechar_caixa_db(sessao_id=int(sessao_id), saldo_informado=contado, obs=e_obs.get().strip())
            except Exception as ex:
                messagebox.showerror("Erro", str(ex))
                return

            dif = resultado["diferenca"]
            msg = (
                f"Caixa FECHADO!\n\n"
                f"Total vendas: R$ {brl(resultado['total_vendas'])}\n"
                f"Saldo final (sistema): R$ {brl(resultado['saldo_final_sistema'])}\n"
                f"Saldo contado: R$ {brl(resultado['saldo_informado'])}\n"
                f"Diferença: R$ {brl(dif)}\n"
                f"Fechado em: {resultado['fechado_em']}"
            )
            messagebox.showinfo("Fechamento concluído", msg)
            win.destroy()
            self.atualizar_status_caixa_topo()

        tk.Button(rodape, text="Cancelar", width=12, command=win.destroy).pack(side="right", padx=6)
        tk.Button(rodape, text="Fechar Caixa", width=12, command=confirmar).pack(side="right")

        win.bind("<Escape>", lambda e: win.destroy())
        e_contado.focus_set()

    # ---------- Navegação ----------
    def show(self, name: str, registrar=True):
        if registrar and self.frame_atual is not None:
            self.historico.append(self.frame_atual)

        self.frame_atual = name
        frame = self.frames[name]
        frame.tkraise()

        if hasattr(frame, "on_show"):
            frame.on_show()

        self.lbl_titulo.config(text=self.titulos.get(name, "Sistema PDV"))
        self.btn_voltar.config(state=("normal" if self.historico else "disabled"))
        self.atualizar_status_caixa_topo()

    def voltar(self):
        if not self.historico:
            return
        anterior = self.historico.pop()
        self.show(anterior, registrar=False)

    def ir_menu(self):
        self.historico.clear()
        self.show("MenuFrame", registrar=False)

    # ---------- Dados ----------
    def ler_estoque(self):
        produtos = listar_produtos()
        linhas = []
        for codigo, nome, preco_custo, preco_venda, quantidade in produtos:
            linhas.append([
                str(codigo),
                str(nome),
                brl(float(preco_custo)),
                brl(float(preco_venda)),
                str(int(quantidade)),
            ])
        return linhas

    def escrever_estoque(self, itens):
        sincronizar_produtos(itens, parse_float=to_float)

    def registrar_venda_completa(self, sessao_id: int, itens: list, forma_pagamento: str,
                                 subtotal: float, desconto: float, total: float, recebido: float, troco: float):
        return registrar_venda_completa_db(
            sessao_id=sessao_id,
            itens=itens,
            forma_pagamento=forma_pagamento,
            subtotal=subtotal,
            desconto=desconto,
            total=total,
            recebido=recebido,
            troco=troco,
            status="FINALIZADA",
        )

    def ler_vendas(self):
        vendas = listar_vendas()
        linhas = []
        for datahora, codigo, produto, preco_unit, qtd, total in vendas:
            linhas.append([
                str(datahora),
                str(codigo) if codigo is not None else "",
                str(produto),
                brl(float(preco_unit)),
                str(int(qtd)),
                brl(float(total)),
            ])
        return linhas

    def get_sessao_aberta(self):
        return get_sessao_aberta()


if __name__ == "__main__":
    PDVApp().mainloop()
