import sqlite3
from datetime import datetime, timedelta
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "pdv.db")


def conectar():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def inicializar_banco():
    with conectar() as conn:
        cur = conn.cursor()

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

        cur.execute("CREATE INDEX IF NOT EXISTS idx_vendas_cabecalho_datahora ON vendas_cabecalho(datahora)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_vendas_cabecalho_sessao ON vendas_cabecalho(sessao_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_vendas_itens_venda ON vendas_itens(venda_id)")

        conn.commit()


# -------------------------
# Caixa
# -------------------------
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
        return cur.fetchone()


def abrir_caixa(saldo_inicial: float, operador: str = "", obs: str = "") -> int:
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
            SELECT COALESCE(SUM(total), 0)
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


def fechar_caixa(sessao_id: int, saldo_informado: float, obs: str = ""):
    saldo_inicial, total_vendas, saldo_final_sistema, _ = totais_sessao(sessao_id)
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


# -------------------------
# Produtos
# -------------------------
def listar_produtos():
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT codigo, nome, preco_custo, preco_venda, quantidade
            FROM produtos
            ORDER BY nome
        """)
        return cur.fetchall()


def buscar_produto(codigo: str):
    codigo = (codigo or "").strip()
    if not codigo:
        return None
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT codigo, nome, preco_custo, preco_venda, quantidade
            FROM produtos
            WHERE codigo=?
            LIMIT 1
        """, (codigo,))
        return cur.fetchone()


def atualizar_produto(codigo: str, nome: str, preco_custo: float, preco_venda: float, quantidade: int):
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO produtos (codigo, nome, preco_custo, preco_venda, quantidade)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(codigo) DO UPDATE SET
                nome=excluded.nome,
                preco_custo=excluded.preco_custo,
                preco_venda=excluded.preco_venda,
                quantidade=excluded.quantidade
        """, (codigo, nome, float(preco_custo), float(preco_venda), int(quantidade)))
        conn.commit()


def baixar_estoque(codigo: str, qtd: int):
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute("SELECT quantidade FROM produtos WHERE codigo=?", (codigo,))
        row = cur.fetchone()
        if not row:
            raise RuntimeError("Produto não encontrado.")
        atual = int(row[0] or 0)
        if qtd > atual:
            raise RuntimeError("Estoque insuficiente.")
        cur.execute("UPDATE produtos SET quantidade=? WHERE codigo=?", (atual - int(qtd), codigo))
        conn.commit()


# -------------------------
# Vendas
# -------------------------
def registrar_venda(sessao_id: int, itens: list, forma_pagamento: str, desconto: float, recebido: float, troco: float) -> int:
    """
    itens: lista de dicts:
      {"codigo": str, "produto": str, "preco_unit": float, "preco_custo": float, "qtd": int}
    """
    subtotal = sum(float(i["preco_unit"]) * int(i["qtd"]) for i in itens)
    desconto = float(desconto or 0.0)
    total = max(0.0, subtotal - desconto)

    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with conectar() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO vendas_cabecalho
                (datahora, sessao_id, subtotal, desconto, total, forma_pagamento, recebido, troco, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'FINALIZADA')
        """, (agora, int(sessao_id), float(subtotal), float(desconto), float(total),
              str(forma_pagamento), float(recebido or 0.0), float(troco or 0.0)))
        venda_id = int(cur.lastrowid)

        for it in itens:
            preco = float(it["preco_unit"])
            qtd = int(it["qtd"])
            total_item = preco * qtd
            cur.execute("""
                INSERT INTO vendas_itens
                    (venda_id, codigo, produto, preco_unit, preco_custo, qtd, total_item)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (venda_id, str(it.get("codigo") or ""), str(it.get("produto") or ""),
                  preco, float(it.get("preco_custo") or 0.0), qtd, float(total_item)))

        conn.commit()

    # baixa estoque
    for it in itens:
        baixar_estoque(str(it.get("codigo") or ""), int(it.get("qtd") or 0))

    return venda_id


def listar_vendas_itens(limit=500):
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                c.id as cupom,
                c.datahora,
                c.forma_pagamento,
                i.codigo,
                i.produto,
                i.preco_unit,
                i.qtd,
                i.total_item
            FROM vendas_itens i
            JOIN vendas_cabecalho c ON c.id = i.venda_id
            WHERE c.status='FINALIZADA'
            ORDER BY c.id DESC, i.id ASC
            LIMIT ?
        """, (int(limit),))
        return cur.fetchall()
