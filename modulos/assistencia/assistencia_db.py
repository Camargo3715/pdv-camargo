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

        # ==========================================
        # TABELA LOJAS
        # ==========================================

        cur.execute("""
        CREATE TABLE IF NOT EXISTS lojas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT NOT NULL,
            subtitulo TEXT,
            whatsapp TEXT,
            rua TEXT,
            numero TEXT,
            bairro TEXT,
            cidade TEXT,
            cep TEXT
        )
        """)

        # GARANTE COLUNAS EM BANCOS ANTIGOS

        for coluna, tipo in [
            ("subtitulo", "TEXT"),
            ("whatsapp", "TEXT"),
            ("rua", "TEXT"),
            ("numero", "TEXT"),
            ("bairro", "TEXT"),
            ("cidade", "TEXT"),
            ("cep", "TEXT"),
        ]:

            if not coluna_existe(conn, "lojas", coluna):

                cur.execute(
                    f"ALTER TABLE lojas ADD COLUMN {coluna} {tipo}"
                )

        # ==========================================
        # TABELA ORDENS DE SERVIÇO
        # ==========================================

        cur.execute("""
        CREATE TABLE IF NOT EXISTS ordens_servico (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            loja_id INTEGER NOT NULL DEFAULT 1,

            cliente_nome TEXT NOT NULL,
            cpf TEXT,
            telefone TEXT,

            endereco TEXT,
            cep TEXT,

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

        # GARANTE COLUNAS EM BANCOS ANTIGOS

        for coluna, tipo in [
            ("loja_id", "INTEGER NOT NULL DEFAULT 1"),
            ("cpf", "TEXT"),
            ("telefone", "TEXT"),
            ("endereco", "TEXT"),
            ("cep", "TEXT"),
            ("token_publico", "TEXT"),
            ("orcamento", "REAL NOT NULL DEFAULT 0"),
            ("tecnico", "TEXT"),
            ("data_saida", "TEXT"),
            ("observacoes", "TEXT"),
        ]:

            if not coluna_existe(conn, "ordens_servico", coluna):

                cur.execute(
                    f"ALTER TABLE ordens_servico ADD COLUMN {coluna} {tipo}"
                )

        # ==========================================
        # LOJA PADRÃO
        # ==========================================

        cur.execute("SELECT COUNT(*) FROM lojas")

        total_lojas = cur.fetchone()[0]

        if total_lojas == 0:

            cur.execute("""
            INSERT INTO lojas (
                nome,
                subtitulo,
                whatsapp,
                rua,
                numero,
                bairro,
                cidade,
                cep
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                "CAMARGO CELULARES",
                "Assistência Técnica Especializada",
                "(11) 99999-9999",
                "",
                "",
                "",
                "",
                ""
            ))

        conn.commit()


# =====================================================
# LOJAS
# =====================================================

def listar_lojas():

    with conectar() as conn:

        cur = conn.cursor()

        cur.execute("""
        SELECT * FROM lojas
        ORDER BY id ASC
        """)

        return cur.fetchall()


def buscar_loja_por_id(loja_id: int):

    with conectar() as conn:

        cur = conn.cursor()

        cur.execute("""
        SELECT * FROM lojas
        WHERE id = ?
        """, (int(loja_id),))

        return cur.fetchone()


def criar_loja(
    nome,
    subtitulo="Assistência Técnica Especializada",
    whatsapp="",
    rua="",
    numero="",
    bairro="",
    cidade="",
    cep=""
):

    with conectar() as conn:

        cur = conn.cursor()

        cur.execute("""
        INSERT INTO lojas (
            nome,
            subtitulo,
            whatsapp,
            rua,
            numero,
            bairro,
            cidade,
            cep
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            nome,
            subtitulo,
            whatsapp,
            rua,
            numero,
            bairro,
            cidade,
            cep
        ))

        conn.commit()

        return cur.lastrowid


def atualizar_loja(
    loja_id,
    nome,
    subtitulo,
    whatsapp,
    rua,
    numero,
    bairro,
    cidade,
    cep
):

    with conectar() as conn:

        cur = conn.cursor()

        cur.execute("""
        UPDATE lojas
        SET
            nome = ?,
            subtitulo = ?,
            whatsapp = ?,
            rua = ?,
            numero = ?,
            bairro = ?,
            cidade = ?,
            cep = ?
        WHERE id = ?
        """, (
            nome,
            subtitulo,
            whatsapp,
            rua,
            numero,
            bairro,
            cidade,
            cep,
            int(loja_id)
        ))

        conn.commit()


# =====================================================
# ORDENS DE SERVIÇO
# =====================================================

def criar_os(
    cliente_nome,
    cpf,
    telefone,
    endereco,
    cep,
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
            cpf,
            telefone,

            endereco,
            cep,

            aparelho,
            marca,
            modelo,

            defeito,

            senha_aparelho,

            token_publico,
            orcamento,

            data_entrada

        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (

            loja_id,

            cliente_nome,
            cpf,
            telefone,

            endereco,
            cep,

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
        SELECT

            os.*,

            lojas.nome AS loja_nome

        FROM ordens_servico os

        LEFT JOIN lojas
            ON lojas.id = os.loja_id

        ORDER BY os.id DESC
        """)

        return cur.fetchall()


def buscar_os_por_id(os_id: int):

    with conectar() as conn:

        cur = conn.cursor()

        cur.execute("""
        SELECT

            os.*,

            lojas.nome AS loja_nome,
            lojas.subtitulo AS loja_subtitulo,
            lojas.whatsapp AS loja_whatsapp,
            lojas.rua AS loja_rua,
            lojas.numero AS loja_numero,
            lojas.bairro AS loja_bairro,
            lojas.cidade AS loja_cidade,
            lojas.cep AS loja_cep

        FROM ordens_servico os

        LEFT JOIN lojas
            ON lojas.id = os.loja_id

        WHERE os.id = ?
        """, (int(os_id),))

        return cur.fetchone()


def buscar_os_por_token(token_publico: str):

    with conectar() as conn:

        cur = conn.cursor()

        cur.execute("""
        SELECT

            os.*,

            lojas.nome AS loja_nome,
            lojas.subtitulo AS loja_subtitulo,
            lojas.whatsapp AS loja_whatsapp,
            lojas.rua AS loja_rua,
            lojas.numero AS loja_numero,
            lojas.bairro AS loja_bairro,
            lojas.cidade AS loja_cidade,
            lojas.cep AS loja_cep

        FROM ordens_servico os

        LEFT JOIN lojas
            ON lojas.id = os.loja_id

        WHERE os.token_publico = ?
        """, (str(token_publico),))

        return cur.fetchone()


def atualizar_status_os(os_id: int, novo_status: str):

    with conectar() as conn:

        cur = conn.cursor()

        cur.execute("""
        UPDATE ordens_servico
        SET status = ?
        WHERE id = ?
        """, (
            novo_status,
            int(os_id)
        ))

        conn.commit()


def excluir_os(os_id: int):

    with conectar() as conn:

        cur = conn.cursor()

        cur.execute("""
        DELETE FROM ordens_servico
        WHERE id = ?
        """, (int(os_id),))

        conn.commit()