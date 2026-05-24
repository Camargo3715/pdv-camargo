import sqlite3
import secrets
from datetime import datetime

DB_PATH = "pdv.db"


def conectar():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def coluna_existe(conn, tabela: str, coluna: str) -> bool:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({tabela})")
    cols = [row["name"] for row in cur.fetchall()]
    return coluna in cols


def inicializar_assistencia():
    with conectar() as conn:
        cur = conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS ordens_servico (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            loja_id INTEGER NOT NULL DEFAULT 1,

            cliente_nome TEXT NOT NULL,
            cpf TEXT,
            telefone TEXT,
            endereco TEXT,

            aparelho TEXT,
            marca TEXT,
            modelo TEXT,
            imei TEXT,

            defeito TEXT,
            senha_aparelho TEXT,
            checklist TEXT,

            status TEXT NOT NULL DEFAULT 'EM ANALISE',
            token_publico TEXT UNIQUE,

            orcamento REAL NOT NULL DEFAULT 0,
            tecnico TEXT,

            data_entrada TEXT,
            data_saida TEXT,
            observacoes TEXT
        )
        """)

        if not coluna_existe(conn, "ordens_servico", "token_publico"):
            cur.execute("ALTER TABLE ordens_servico ADD COLUMN token_publico TEXT")

        if not coluna_existe(conn, "ordens_servico", "orcamento"):
            cur.execute("ALTER TABLE ordens_servico ADD COLUMN orcamento REAL NOT NULL DEFAULT 0")

        conn.commit()


def criar_os(
    cliente_nome,
    telefone,
    aparelho,
    marca,
    modelo,
    defeito,
    senha_aparelho,
    valor_servico=0,
    loja_id=1
):
    token_publico = secrets.token_hex(16)

    with conectar() as conn:
        cur = conn.cursor()

        cur.execute("""
        INSERT INTO ordens_servico (
            loja_id,
            cliente_nome,
            telefone,
            aparelho,
            marca,
            modelo,
            defeito,
            senha_aparelho,
            token_publico,
            orcamento,
            data_entrada
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            loja_id,
            cliente_nome,
            telefone,
            aparelho,
            marca,
            modelo,
            defeito,
            senha_aparelho,
            token_publico,
            float(valor_servico or 0),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ))

        conn.commit()
        return cur.lastrowid, token_publico


def listar_os():
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute("""
        SELECT *
        FROM ordens_servico
        ORDER BY id DESC
        """)
        return cur.fetchall()


def buscar_os_por_id(os_id: int):
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute("""
        SELECT *
        FROM ordens_servico
        WHERE id = ?
        """, (int(os_id),))
        return cur.fetchone()


def buscar_os_por_token(token_publico: str):
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute("""
        SELECT *
        FROM ordens_servico
        WHERE token_publico = ?
        """, (str(token_publico),))
        return cur.fetchone()


def atualizar_status_os(os_id: int, novo_status: str):
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute("""
        UPDATE ordens_servico
        SET status = ?
        WHERE id = ?
        """, (novo_status, int(os_id)))
        conn.commit()