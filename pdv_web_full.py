# pdv_web_full.py
# PDV Camargo Celulares — Web (Streamlit) | Completo: Caixa + Estoque + Histórico + Relatórios + Login
# ✅ Multi-loja: 1 banco, dados separados por loja_id (estoque/vendas/caixa/usuários)
# ✅ Admin: botão para ZERAR UMA LOJA (estoque + vendas + caixa) com segurança
# ✅ Backup automático (SQLite): diário + retenção + backup seguro via SQLite backup API

import os
import sqlite3
import secrets
import hashlib
import shutil
import glob

from datetime import datetime, timedelta, date

import streamlit as st
import pandas as pd

APP_TITLE = "PDV Camargo Celulares — Web"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# =========================
# DB PATH (Render / Local)
# =========================
DEFAULT_DB_LOCAL = os.path.join(BASE_DIR, "pdv.db")

# Detecta Render
IS_RENDER = bool(os.getenv("RENDER")) or bool(os.getenv("RENDER_SERVICE_ID"))

# No Render Free, /var/data NÃO existe (sem Disk). Então usa /tmp.
# Se você estiver no Render com Disk, pode setar DATABASE_PATH=/var/data/pdv.db
if IS_RENDER:
    DB_PATH = os.getenv("DATABASE_PATH", "/tmp/pdv.db")
else:
    DB_PATH = os.getenv("DATABASE_PATH", DEFAULT_DB_LOCAL)

# Garante que a pasta do banco exista
try:
    db_dir = os.path.dirname(DB_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
except Exception:
    pass

# =========================
# BACKUP (Automático)
# =========================
# Onde salvar backups:
# - Local: ./backups
# - Render: se tiver Disk: /var/data/backups (recomendado via BACKUP_DIR)
# - Render free: /tmp/backups (vai sumir ao reiniciar)
DEFAULT_BACKUP_DIR = os.path.join(BASE_DIR, "backups")
if IS_RENDER:
    BACKUP_DIR = os.getenv("BACKUP_DIR", "/tmp/backups")
else:
    BACKUP_DIR = os.getenv("BACKUP_DIR", DEFAULT_BACKUP_DIR)

BACKUP_ENABLED = os.getenv("BACKUP_ENABLED", "1") == "1"
BACKUP_ON_STARTUP = os.getenv("BACKUP_ON_STARTUP", "1") == "1"
BACKUP_RETENTION_DAYS = int(os.getenv("BACKUP_RETENTION_DAYS", "14"))  # mantém últimos N dias

def garantir_pasta_backup():
    try:
        os.makedirs(BACKUP_DIR, exist_ok=True)
    except Exception:
        pass

def _backup_filename(prefix: str = "pdv") -> str:
    # 1 backup por dia (padrão): pdv_YYYY-MM-DD.db
    # Se você quiser mais granular, troque por %H-%M também.
    hoje = datetime.now().strftime("%Y-%m-%d")
    return f"{prefix}_{hoje}.db"

def listar_backups(prefix: str = "pdv"):
    garantir_pasta_backup()
    pattern = os.path.join(BACKUP_DIR, f"{prefix}_*.db")
    files = sorted(glob.glob(pattern), reverse=True)
    return files

def limpar_backups_antigos(prefix: str = "pdv"):
    """
    Apaga backups antigos mantendo apenas os dentro da janela de retenção.
    """
    if not BACKUP_ENABLED:
        return
    garantir_pasta_backup()

    limite = datetime.now() - timedelta(days=BACKUP_RETENTION_DAYS)
    for fp in listar_backups(prefix=prefix):
        try:
            base = os.path.basename(fp)  # pdv_YYYY-MM-DD.db
            # tenta extrair data
            parte = base.replace(f"{prefix}_", "").replace(".db", "")
            dt = datetime.strptime(parte, "%Y-%m-%d")
            if dt < limite:
                os.remove(fp)
        except Exception:
            # se falhar parse, não apaga
            pass

def sqlite_backup_seguro(db_path: str, backup_path: str):
    """
    Faz backup seguro usando a API de backup do SQLite (melhor que copiar arquivo).
    """
    garantir_pasta_backup()
    src = None
    dst = None
    try:
        src = sqlite3.connect(db_path, check_same_thread=False)
        src.execute("PRAGMA foreign_keys = ON;")
        dst = sqlite3.connect(backup_path, check_same_thread=False)
        src.backup(dst)
        dst.commit()
    finally:
        try:
            if dst:
                dst.close()
        except Exception:
            pass
        try:
            if src:
                src.close()
        except Exception:
            pass

def criar_backup_agora(prefix: str = "pdv") -> str:
    """
    Cria um backup do banco atual e retorna o caminho do arquivo gerado.
    """
    if not BACKUP_ENABLED:
        return ""

    garantir_pasta_backup()
    nome = _backup_filename(prefix=prefix)
    backup_path = os.path.join(BACKUP_DIR, nome)

    # evita refazer backup se já existe no dia
    if os.path.exists(backup_path):
        return backup_path

    # Se DB ainda não existe, não cria
    if not os.path.exists(DB_PATH):
        return ""

    sqlite_backup_seguro(DB_PATH, backup_path)
    limpar_backups_antigos(prefix=prefix)
    return backup_path

def auto_backup_se_precisar(prefix: str = "pdv"):
    """
    Faz 1 backup por dia automaticamente quando o app inicia/recarrrega.
    """
    if not BACKUP_ENABLED or not BACKUP_ON_STARTUP:
        return
    try:
        criar_backup_agora(prefix=prefix)
    except Exception:
        # nunca travar o app por backup
        pass


# =========================
# Helpers (DB / Datas)
# =========================
def agora_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def tabela_existe(conn: sqlite3.Connection, nome: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (nome,),
    )
    return cur.fetchone() is not None


def coluna_existe(conn: sqlite3.Connection, tabela: str, coluna: str) -> bool:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({tabela})")
    cols = [r[1] for r in cur.fetchall()]  # r[1] = name
    return coluna in cols


def add_coluna_se_nao_existe(conn: sqlite3.Connection, tabela: str, coluna_sql: str, nome_coluna: str):
    if not coluna_existe(conn, tabela, nome_coluna):
        conn.execute(f"ALTER TABLE {tabela} ADD COLUMN {coluna_sql}")


def garantir_lojas_padrao(conn: sqlite3.Connection):
    """
    Garante a tabela lojas e cadastra 3 lojas padrão se estiver vazia.
    """
    conn.execute("""
    CREATE TABLE IF NOT EXISTS lojas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT NOT NULL,
        ativa INTEGER NOT NULL DEFAULT 1
    )
    """)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM lojas")
    total = int(cur.fetchone()[0] or 0)
    if total == 0:
        conn.execute("INSERT INTO lojas (nome) VALUES (?)", ("Loja 1",))
        conn.execute("INSERT INTO lojas (nome) VALUES (?)", ("Loja 2",))
        conn.execute("INSERT INTO lojas (nome) VALUES (?)", ("Loja 3",))
    conn.commit()


def migrar_produtos_para_multiloja(conn: sqlite3.Connection):
    """
    Migra tabela produtos antiga (codigo UNIQUE global) para multi-loja (UNIQUE(loja_id, codigo)).
    Faz rebuild seguro: cria produtos_new, copia, drop, rename.
    """
    if not tabela_existe(conn, "produtos"):
        return

    # Se já tem loja_id e UNIQUE por loja, não faz nada
    if coluna_existe(conn, "produtos", "loja_id"):
        return

    cur = conn.cursor()
    cur.execute("SELECT id, codigo, nome, preco_custo, preco_venda, quantidade FROM produtos")
    rows = cur.fetchall()

    conn.execute("""
    CREATE TABLE IF NOT EXISTS produtos_new (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        loja_id INTEGER NOT NULL,
        codigo TEXT NOT NULL,
        nome TEXT NOT NULL,
        preco_custo REAL NOT NULL DEFAULT 0,
        preco_venda REAL NOT NULL,
        quantidade INTEGER NOT NULL DEFAULT 0,
        UNIQUE(loja_id, codigo),
        FOREIGN KEY (loja_id) REFERENCES lojas(id)
    )
    """)

    for (pid, codigo, nome, pc, pv, qtd) in rows:
        conn.execute(
            """
            INSERT INTO produtos_new (id, loja_id, codigo, nome, preco_custo, preco_venda, quantidade)
            VALUES (?, 1, ?, ?, ?, ?, ?)
            """,
            (pid, (codigo or "").strip(), nome, float(pc or 0), float(pv or 0), int(qtd or 0)),
        )

    conn.execute("DROP TABLE produtos")
    conn.execute("ALTER TABLE produtos_new RENAME TO produtos")
    conn.commit()


# =========================
# Banco
# =========================
def conectar():
    """
    Conecta no SQLite. Se o caminho atual falhar (permissão/pasta),
    cai automaticamente para /tmp/pdv.db para não dar tela preta.
    """
    global DB_PATH
    try:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn
    except sqlite3.OperationalError:
        # fallback final (Render Free)
        DB_PATH = "/tmp/pdv.db"
        try:
            os.makedirs("/tmp", exist_ok=True)
        except Exception:
            pass
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn


def inicializar_banco():
    """
    ✅ Inicializa banco já no padrão MULTI-LOJA.
    - Cria tabela lojas + cadastra 3 lojas (se vazio)
    - Migra tabelas antigas para receber loja_id (tudo vira loja_id=1)
    - ✅ Auto-backup diário + retenção (se habilitado)
    """
    with conectar() as conn:
        cur = conn.cursor()

        # 1) Lojas
        garantir_lojas_padrao(conn)

        # 2) Produtos (rebuild se era legado)
        #    (se já existia sem loja_id, migra para UNIQUE(loja_id, codigo))
        if tabela_existe(conn, "produtos"):
            migrar_produtos_para_multiloja(conn)
        else:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS produtos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                loja_id INTEGER NOT NULL,
                codigo TEXT NOT NULL,
                nome TEXT NOT NULL,
                preco_custo REAL NOT NULL DEFAULT 0,
                preco_venda REAL NOT NULL,
                quantidade INTEGER NOT NULL DEFAULT 0,
                UNIQUE(loja_id, codigo),
                FOREIGN KEY (loja_id) REFERENCES lojas(id)
            )
            """)

        # (LEGADO) vendas - mantém por compatibilidade, mas agora com loja_id
        cur.execute("""
        CREATE TABLE IF NOT EXISTS vendas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            loja_id INTEGER NOT NULL DEFAULT 1,
            datahora TEXT NOT NULL,
            codigo TEXT,
            produto TEXT NOT NULL,
            preco_unit REAL NOT NULL,
            qtd INTEGER NOT NULL,
            total REAL NOT NULL,
            FOREIGN KEY (loja_id) REFERENCES lojas(id)
        )
        """)

        # Caixa sessões (agora por loja)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS caixa_sessoes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            loja_id INTEGER NOT NULL DEFAULT 1,
            aberto_em TEXT NOT NULL,
            fechado_em TEXT,
            status TEXT NOT NULL DEFAULT 'ABERTO',
            saldo_inicial REAL NOT NULL DEFAULT 0,
            saldo_final_sistema REAL NOT NULL DEFAULT 0,
            saldo_final_informado REAL NOT NULL DEFAULT 0,
            diferenca REAL NOT NULL DEFAULT 0,
            operador TEXT,
            obs_abertura TEXT,
            obs_fechamento TEXT,
            FOREIGN KEY (loja_id) REFERENCES lojas(id)
        )
        """)

        # Vendas cabeçalho (agora por loja)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS vendas_cabecalho (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            loja_id INTEGER NOT NULL DEFAULT 1,
            datahora TEXT NOT NULL,
            sessao_id INTEGER,
            subtotal REAL NOT NULL DEFAULT 0,
            desconto REAL NOT NULL DEFAULT 0,
            total REAL NOT NULL DEFAULT 0,
            forma_pagamento TEXT NOT NULL,
            recebido REAL NOT NULL DEFAULT 0,
            troco REAL NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'FINALIZADA',
            FOREIGN KEY (loja_id) REFERENCES lojas(id),
            FOREIGN KEY (sessao_id) REFERENCES caixa_sessoes(id)
        )
        """)

        # Vendas itens (agora por loja)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS vendas_itens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            loja_id INTEGER NOT NULL DEFAULT 1,
            venda_id INTEGER NOT NULL,
            codigo TEXT,
            produto TEXT NOT NULL,
            preco_unit REAL NOT NULL,
            preco_custo REAL NOT NULL DEFAULT 0,
            qtd INTEGER NOT NULL,
            total_item REAL NOT NULL,
            FOREIGN KEY (loja_id) REFERENCES lojas(id),
            FOREIGN KEY (venda_id) REFERENCES vendas_cabecalho(id) ON DELETE CASCADE
        )
        """)

        # 3) Migração: adiciona loja_id nas tabelas existentes (se ainda não tiver)
        # Observação: produtos já foi tratado via rebuild acima.
        for tabela in ["vendas", "caixa_sessoes", "vendas_cabecalho", "vendas_itens"]:
            if tabela_existe(conn, tabela) and not coluna_existe(conn, tabela, "loja_id"):
                add_coluna_se_nao_existe(conn, tabela, "loja_id INTEGER NOT NULL DEFAULT 1", "loja_id")
                conn.execute(f"UPDATE {tabela} SET loja_id = 1 WHERE loja_id IS NULL")

        # Índices (inclui loja)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_produtos_loja_codigo ON produtos(loja_id, codigo)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_vendas_cabecalho_loja_datahora ON vendas_cabecalho(loja_id, datahora)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_vendas_cabecalho_sessao ON vendas_cabecalho(sessao_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_vendas_itens_venda ON vendas_itens(venda_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_caixa_loja_status ON caixa_sessoes(loja_id, status)")

        conn.commit()

    # ✅ Auto-backup diário (fora do with, pra não conflitar com a conexão ativa)
    auto_backup_se_precisar(prefix="pdv")

# =========================
# Usuários / Auth (Login)
# =========================
def gerar_hash_senha(senha: str) -> tuple[str, str]:
    """
    PBKDF2-HMAC SHA256 com salt aleatório.
    Retorna (salt_hex, hash_hex).
    """
    senha_b = (senha or "").encode("utf-8")
    salt = secrets.token_bytes(16)
    dk = hashlib.pbkdf2_hmac("sha256", senha_b, salt, 120_000)
    return salt.hex(), dk.hex()


def verificar_senha(senha: str, salt_hex: str, hash_hex: str) -> bool:
    senha_b = (senha or "").encode("utf-8")
    salt = bytes.fromhex(salt_hex)
    dk = hashlib.pbkdf2_hmac("sha256", senha_b, salt, 120_000)
    return dk.hex() == (hash_hex or "")


def inicializar_usuarios():
    """
    ✅ Cria a tabela usuarios (multi-loja) e, se não existir nenhum usuário,
    cria um ADMIN inicial usando ADMIN_USER/ADMIN_PASS (Render env),
    ou padrão admin/admin123.

    Roles:
    - ADMIN: vê todas as lojas (loja_id pode ser NULL)
    - DONO / OPERADOR: presos em uma loja (loja_id obrigatório)
    """
    with conectar() as conn:
        cur = conn.cursor()

        # garante lojas (pra FK)
        garantir_lojas_padrao(conn)

        # Cria tabela (se não existir)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS usuarios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            nome TEXT,
            role TEXT NOT NULL CHECK(role IN ('ADMIN','DONO','OPERADOR')),
            loja_id INTEGER,
            pass_salt TEXT NOT NULL,
            pass_hash TEXT NOT NULL,
            ativo INTEGER NOT NULL DEFAULT 1,
            criado_em TEXT NOT NULL,
            FOREIGN KEY (loja_id) REFERENCES lojas(id)
        )
        """)

        # ✅ Migração suave: se a tabela já existia e não tem loja_id
        # Observação: como a tabela já existe, IF NOT EXISTS não altera schema,
        # então precisamos checar e adicionar a coluna de verdade.
        if not coluna_existe(conn, "usuarios", "loja_id"):
            add_coluna_se_nao_existe(conn, "usuarios", "loja_id INTEGER", "loja_id")
            # Operadores antigos viram loja 1 por padrão
            try:
                conn.execute("UPDATE usuarios SET loja_id = 1 WHERE (role='OPERADOR') AND (loja_id IS NULL)")
            except Exception:
                pass
            conn.commit()

        # Índices úteis
        cur.execute("CREATE INDEX IF NOT EXISTS idx_usuarios_role ON usuarios(role)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_usuarios_loja ON usuarios(loja_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_usuarios_ativo ON usuarios(ativo)")

        cur.execute("SELECT COUNT(*) FROM usuarios")
        total = int(cur.fetchone()[0] or 0)

        if total == 0:
            admin_user = (os.getenv("ADMIN_USER") or "admin").strip().lower()
            admin_pass = (os.getenv("ADMIN_PASS") or "admin123").strip()

            salt, ph = gerar_hash_senha(admin_pass)
            cur.execute(
                """
                INSERT INTO usuarios (username, nome, role, loja_id, pass_salt, pass_hash, ativo, criado_em)
                VALUES (?, ?, 'ADMIN', NULL, ?, ?, 1, ?)
                """,
                (admin_user, "Administrador", salt, ph, agora_iso()),
            )
            conn.commit()


def get_usuario(username: str):
    u = (username or "").strip().lower()
    if not u:
        return None
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, username, nome, role, loja_id, pass_salt, pass_hash, ativo
            FROM usuarios
            WHERE username = ?
            """,
            (u,),
        )
        row = cur.fetchone()

    if not row:
        return None

    return {
        "id": int(row[0]),
        "username": str(row[1]),
        "nome": str(row[2] or ""),
        "role": str(row[3]),
        "loja_id": (int(row[4]) if row[4] is not None else None),
        "salt": str(row[5]),
        "hash": str(row[6]),
        "ativo": int(row[7] or 0),
    }


def autenticar(username: str, senha: str):
    user = get_usuario(username)
    if not user:
        return None
    if user["ativo"] != 1:
        return None
    if not verificar_senha(senha, user["salt"], user["hash"]):
        return None
    return user


def listar_usuarios_df():
    with conectar() as conn:
        return pd.read_sql_query(
            """
            SELECT username, nome, role, loja_id, ativo, criado_em
            FROM usuarios
            ORDER BY role DESC, username ASC
            """,
            conn,
        )


# =========================
# Usuários (CRUD)
# =========================
def criar_usuario(username: str, nome: str, role: str, senha: str, loja_id: int | None = None, ativo: int = 1):
    username = (username or "").strip().lower()
    nome = (nome or "").strip()
    role = (role or "").strip().upper()

    if role not in ("ADMIN", "DONO", "OPERADOR"):
        raise ValueError("Perfil inválido. Use ADMIN, DONO ou OPERADOR.")

    if not username:
        raise ValueError("Username é obrigatório.")
    if len((senha or "").strip()) < 4:
        raise ValueError("Senha muito curta (mín. 4).")

    # Regras multi-loja:
    # - ADMIN: loja_id pode ser None (vê todas)
    # - DONO/OPERADOR: loja_id obrigatório
    if role in ("DONO", "OPERADOR") and not loja_id:
        raise ValueError("Para DONO/OPERADOR é obrigatório informar loja_id.")
    if role == "ADMIN":
        loja_id = None

    salt, ph = gerar_hash_senha(senha)
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO usuarios (username, nome, role, loja_id, pass_salt, pass_hash, ativo, criado_em)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (username, nome, role, loja_id, salt, ph, int(1 if ativo else 0), agora_iso()),
        )
        conn.commit()


def atualizar_usuario_role_ativo(username: str, role: str, ativo: int, loja_id: int | None = None):
    username = (username or "").strip().lower()
    role = (role or "").strip().upper()

    if role not in ("ADMIN", "DONO", "OPERADOR"):
        raise ValueError("Perfil inválido.")

    if role in ("DONO", "OPERADOR") and not loja_id:
        raise ValueError("Para DONO/OPERADOR é obrigatório informar loja_id.")
    if role == "ADMIN":
        loja_id = None

    with conectar() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE usuarios
            SET role = ?, loja_id = ?, ativo = ?
            WHERE username = ?
            """,
            (role, loja_id, int(1 if ativo else 0), username),
        )
        if cur.rowcount == 0:
            raise ValueError("Usuário não encontrado.")
        conn.commit()


def atualizar_senha(username: str, nova_senha: str):
    username = (username or "").strip().lower()
    if len((nova_senha or "").strip()) < 4:
        raise ValueError("Senha muito curta (mín. 4).")
    salt, ph = gerar_hash_senha(nova_senha)
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE usuarios
            SET pass_salt = ?, pass_hash = ?
            WHERE username = ?
            """,
            (salt, ph, username),
        )
        if cur.rowcount == 0:
            raise ValueError("Usuário não encontrado.")
        conn.commit()


# =========================
# Utilitários
# =========================
def to_float(txt):
    s = str(txt).strip().replace(" ", "")
    if not s:
        return 0.0
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0


def brl(v: float) -> str:
    try:
        return f"{float(v or 0.0):.2f}".replace(".", ",")
    except Exception:
        return "0,00"


def map_forma_pagamento(rotulo_ui: str) -> str:
    r = (rotulo_ui or "").strip().lower()
    if r == "pix":
        return "PIX"
    if r == "dinheiro":
        return "DINHEIRO"
    if "crédito" in r or "credito" in r:
        return "CARTAO_CREDITO"
    if "débito" in r or "debito" in r:
        return "CARTAO_DEBITO"
    return "OUTRO"


# =========================
# Lojas (helpers)
# =========================
def listar_lojas_df():
    with conectar() as conn:
        df = pd.read_sql_query(
            "SELECT id, nome, ativa FROM lojas ORDER BY id ASC",
            conn,
        )
    return df


def get_loja_nome(loja_id: int) -> str:
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute("SELECT nome FROM lojas WHERE id = ?", (int(loja_id),))
        r = cur.fetchone()
    return str(r[0]) if r else f"Loja {loja_id}"

# =========================
# Produtos (Estoque) — MULTI-LOJA
# =========================
def listar_produtos_df(loja_id: int):
    with conectar() as conn:
        df = pd.read_sql_query(
            """
            SELECT codigo, nome, preco_custo, preco_venda, quantidade
            FROM produtos
            WHERE loja_id = ?
            ORDER BY nome
            """,
            conn,
            params=(int(loja_id),),
        )
    return df


def buscar_produto_por_codigo(loja_id: int, codigo: str):
    codigo = (codigo or "").strip()
    if not codigo:
        return None
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT codigo, nome, preco_custo, preco_venda, quantidade
            FROM produtos
            WHERE loja_id = ? AND codigo = ?
            """,
            (int(loja_id), codigo),
        )
        row = cur.fetchone()
    if not row:
        return None
    return {
        "codigo": str(row[0]),
        "nome": str(row[1]),
        "preco_custo": float(row[2] or 0.0),
        "preco_venda": float(row[3] or 0.0),
        "quantidade": int(row[4] or 0),
    }


def upsert_produto(loja_id: int, codigo: str, nome: str, preco_custo: float, preco_venda: float, quantidade: int):
    codigo = (codigo or "").strip()
    nome = (nome or "").strip()
    if not codigo or not nome:
        raise ValueError("Código e nome são obrigatórios.")
    if float(preco_venda or 0) <= 0:
        raise ValueError("Preço de venda deve ser > 0.")
    if float(preco_custo or 0) < 0:
        raise ValueError("Preço de custo deve ser >= 0.")
    if int(quantidade or 0) < 0:
        raise ValueError("Quantidade deve ser >= 0.")

    with conectar() as conn:
        cur = conn.cursor()
        # transação explícita reduz risco de conflito
        cur.execute("BEGIN IMMEDIATE")
        cur.execute(
            """
            INSERT INTO produtos (loja_id, codigo, nome, preco_custo, preco_venda, quantidade)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(loja_id, codigo) DO UPDATE SET
                nome=excluded.nome,
                preco_custo=excluded.preco_custo,
                preco_venda=excluded.preco_venda,
                quantidade=excluded.quantidade
            """,
            (int(loja_id), codigo, nome, float(preco_custo or 0.0), float(preco_venda or 0.0), int(quantidade or 0)),
        )
        conn.commit()


def excluir_produto(loja_id: int, codigo: str):
    codigo = (codigo or "").strip()
    if not codigo:
        return
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute("BEGIN IMMEDIATE")
        cur.execute("DELETE FROM produtos WHERE loja_id = ? AND codigo = ?", (int(loja_id), codigo))
        conn.commit()


def baixar_estoque_por_codigo(loja_id: int, codigo: str, qtd: int):
    codigo = (codigo or "").strip()
    qtd = int(qtd or 0)
    if not codigo:
        raise RuntimeError("Código inválido.")
    if qtd <= 0:
        raise RuntimeError("Quantidade inválida (precisa ser > 0).")

    with conectar() as conn:
        cur = conn.cursor()
        # BEGIN IMMEDIATE: garante lock de escrita, evitando corrida em multi-acesso
        cur.execute("BEGIN IMMEDIATE")
        cur.execute(
            """
            UPDATE produtos
            SET quantidade = quantidade - ?
            WHERE loja_id = ? AND codigo = ? AND quantidade >= ?
            """,
            (int(qtd), int(loja_id), str(codigo), int(qtd)),
        )
        if cur.rowcount == 0:
            conn.rollback()
            raise RuntimeError("Estoque insuficiente ou produto não encontrado.")
        conn.commit()


# =========================
# Caixa (Abertura/Fechamento) — MULTI-LOJA
# =========================
def get_sessao_aberta(loja_id: int):
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, aberto_em, saldo_inicial, operador
            FROM caixa_sessoes
            WHERE loja_id = ? AND status='ABERTO'
            ORDER BY id DESC
            LIMIT 1
            """,
            (int(loja_id),),
        )
        return cur.fetchone()


def abrir_caixa_db(loja_id: int, saldo_inicial: float, operador: str = "", obs: str = "") -> int:
    atual = get_sessao_aberta(loja_id)
    if atual:
        raise RuntimeError(f"Já existe um caixa ABERTO nesta loja (Sessão #{atual[0]}). Feche antes de abrir outro.")
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute("BEGIN IMMEDIATE")
        cur.execute(
            """
            INSERT INTO caixa_sessoes (loja_id, aberto_em, status, saldo_inicial, operador, obs_abertura)
            VALUES (?, ?, 'ABERTO', ?, ?, ?)
            """,
            (int(loja_id), agora_iso(), float(saldo_inicial or 0.0), (operador or "").strip(), (obs or "").strip()),
        )
        conn.commit()
        return int(cur.lastrowid)


def totais_sessao(loja_id: int, sessao_id: int):
    with conectar() as conn:
        cur = conn.cursor()

        # garante que sessão pertence à loja
        cur.execute(
            """
            SELECT saldo_inicial, aberto_em
            FROM caixa_sessoes
            WHERE id = ? AND loja_id = ?
            """,
            (int(sessao_id), int(loja_id)),
        )
        row = cur.fetchone()
        saldo_inicial = float(row[0] or 0.0) if row else 0.0
        aberto_em = row[1] if row else ""

        cur.execute(
            """
            SELECT COALESCE(SUM(total), 0)
            FROM vendas_cabecalho
            WHERE loja_id = ? AND sessao_id = ? AND status='FINALIZADA'
            """,
            (int(loja_id), int(sessao_id)),
        )
        total_vendas = float(cur.fetchone()[0] or 0.0)

        saldo_final_sistema = saldo_inicial + total_vendas
        return saldo_inicial, total_vendas, saldo_final_sistema, aberto_em


def relatorio_pagamentos_sessao(loja_id: int, sessao_id: int):
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT forma_pagamento, COALESCE(SUM(total), 0)
            FROM vendas_cabecalho
            WHERE loja_id = ? AND sessao_id = ? AND status='FINALIZADA'
            GROUP BY forma_pagamento
            ORDER BY forma_pagamento
            """,
            (int(loja_id), int(sessao_id)),
        )
        return [(str(fp), float(t or 0.0)) for fp, t in cur.fetchall()]


def fechar_caixa_db(loja_id: int, sessao_id: int, saldo_informado: float, obs: str = ""):
    saldo_inicial, total_vendas, saldo_final_sistema, _ = totais_sessao(loja_id, sessao_id)
    saldo_informado = float(saldo_informado or 0.0)
    diferenca = saldo_informado - saldo_final_sistema
    agora = agora_iso()

    with conectar() as conn:
        cur = conn.cursor()
        cur.execute("BEGIN IMMEDIATE")
        cur.execute(
            """
            UPDATE caixa_sessoes
            SET
                fechado_em = ?,
                status = 'FECHADO',
                saldo_final_sistema = ?,
                saldo_final_informado = ?,
                diferenca = ?,
                obs_fechamento = ?
            WHERE id = ? AND loja_id = ? AND status='ABERTO'
            """,
            (
                agora,
                float(saldo_final_sistema),
                float(saldo_informado),
                float(diferenca),
                (obs or "").strip(),
                int(sessao_id),
                int(loja_id),
            ),
        )
        if cur.rowcount == 0:
            conn.rollback()
            raise RuntimeError("Não foi possível fechar: sessão não está ABERTA (ou não existe) nesta loja.")
        conn.commit()

    return {
        "saldo_inicial": saldo_inicial,
        "total_vendas": total_vendas,
        "saldo_final_sistema": saldo_final_sistema,
        "saldo_informado": saldo_informado,
        "diferenca": diferenca,
        "fechado_em": agora,
    }


# =========================
# Vendas — MULTI-LOJA
# =========================
def registrar_venda_completa_db(
    loja_id: int,
    sessao_id: int,
    itens: list,
    forma_pagamento: str,
    subtotal: float,
    desconto: float,
    total: float,
    recebido: float,
    troco: float,
    status: str = "FINALIZADA",
    baixar_estoque: bool = False,  # ✅ opcional: modo transacional total
) -> int:
    with conectar() as conn:
        cur = conn.cursor()

        # segurança: sessão pertence à loja e está aberta
        cur.execute(
            "SELECT id FROM caixa_sessoes WHERE id = ? AND loja_id = ? AND status='ABERTO'",
            (int(sessao_id), int(loja_id)),
        )
        if not cur.fetchone():
            raise RuntimeError("Sessão de caixa inválida para esta loja (ou não está ABERTA).")

        cur.execute("BEGIN IMMEDIATE")

        cur.execute(
            """
            INSERT INTO vendas_cabecalho
                (loja_id, datahora, sessao_id, subtotal, desconto, total, forma_pagamento, recebido, troco, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(loja_id),
                agora_iso(),
                int(sessao_id),
                float(subtotal or 0.0),
                float(desconto or 0.0),
                float(total or 0.0),
                str(forma_pagamento),
                float(recebido or 0.0),
                float(troco or 0.0),
                str(status),
            ),
        )
        venda_id = int(cur.lastrowid)

        for it in itens:
            codigo = str(it.get("codigo") or "").strip()
            qtd = int(it.get("qtd") or 0)

            cur.execute(
                """
                INSERT INTO vendas_itens
                    (loja_id, venda_id, codigo, produto, preco_unit, preco_custo, qtd, total_item)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(loja_id),
                    venda_id,
                    codigo,
                    str(it.get("produto") or ""),
                    float(it.get("preco_unit") or 0.0),
                    float(it.get("preco_custo") or 0.0),
                    qtd,
                    float(it.get("total_item") or 0.0),
                ),
            )

            # ✅ opcional: baixa estoque dentro da mesma transação da venda
            if baixar_estoque and codigo and qtd > 0:
                cur.execute(
                    """
                    UPDATE produtos
                    SET quantidade = quantidade - ?
                    WHERE loja_id = ? AND codigo = ? AND quantidade >= ?
                    """,
                    (qtd, int(loja_id), codigo, qtd),
                )
                if cur.rowcount == 0:
                    conn.rollback()
                    raise RuntimeError(f"Estoque insuficiente para o código {codigo}.")

        conn.commit()
        return venda_id


def listar_vendas_itens_df(loja_id: int, filtro_produto: str = ""):
    filtro = (filtro_produto or "").strip().lower()
    with conectar() as conn:
        df = pd.read_sql_query(
            """
            SELECT
                c.datahora as datahora,
                i.codigo as codigo,
                i.produto as produto,
                i.preco_unit as preco_unit,
                i.qtd as qtd,
                i.total_item as total_item
            FROM vendas_itens i
            JOIN vendas_cabecalho c ON c.id = i.venda_id
            WHERE c.loja_id = ? AND i.loja_id = ? AND c.status='FINALIZADA'
            ORDER BY c.id DESC, i.id ASC
            """,
            conn,
            params=(int(loja_id), int(loja_id)),
        )
    if filtro and not df.empty:
        df = df[df["produto"].astype(str).str.lower().str.contains(filtro, na=False)]
    return df


def listar_vendas_por_periodo_df(loja_id: int, data_ini: datetime, data_fim: datetime):
    ini = data_ini.strftime("%Y-%m-%d %H:%M:%S")
    fim = data_fim.strftime("%Y-%m-%d %H:%M:%S")
    with conectar() as conn:
        df = pd.read_sql_query(
            """
            SELECT
                c.datahora as datahora,
                c.id as cupom,
                COALESCE(SUM(i.qtd), 0) as itens,
                c.total as total
            FROM vendas_cabecalho c
            LEFT JOIN vendas_itens i ON i.venda_id = c.id AND i.loja_id = c.loja_id
            WHERE c.loja_id = ?
              AND c.status='FINALIZADA'
              AND datetime(c.datahora) BETWEEN datetime(?) AND datetime(?)
            GROUP BY c.id, c.datahora, c.total
            ORDER BY datetime(c.datahora) DESC
            """,
            conn,
            params=(int(loja_id), ini, fim),
        )
    return df


def totais_por_dia_do_mes(loja_id: int, ano: int, mes: int):
    primeiro = datetime(ano, mes, 1, 0, 0, 0)
    if mes == 12:
        prox = datetime(ano + 1, 1, 1, 0, 0, 0)
    else:
        prox = datetime(ano, mes + 1, 1, 0, 0, 0)

    ini = primeiro.strftime("%Y-%m-%d %H:%M:%S")
    fim = (prox - timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")

    with conectar() as conn:
        df = pd.read_sql_query(
            """
            SELECT date(datahora) as dia, COALESCE(SUM(total), 0) as total
            FROM vendas_cabecalho
            WHERE loja_id = ?
              AND status='FINALIZADA'
              AND datetime(datahora) BETWEEN datetime(?) AND datetime(?)
            GROUP BY date(datahora)
            ORDER BY date(datahora)
            """,
            conn,
            params=(int(loja_id), ini, fim),
        )
    total_mes = float(df["total"].sum()) if not df.empty else 0.0
    return df, total_mes

# =========================
# Admin: Zerar dados de uma loja (estoque/vendas/caixa)
# =========================
def zerar_loja(loja_id: int):
    """
    Zera APENAS os dados da loja informada:
    - produtos (estoque)
    - vendas (cabecalho + itens)
    - caixa_sessoes
    Não mexe em usuários e não afeta outras lojas.

    Segurança:
    - não permite zerar se houver caixa ABERTO na loja
    - ✅ faz backup automático antes de zerar
    """
    # segurança: não zerar com caixa aberto
    if get_sessao_aberta(int(loja_id)):
        raise RuntimeError("Não é possível zerar: existe CAIXA ABERTO nesta loja. Feche o caixa antes.")

    # ✅ backup antes de ação destrutiva
    try:
        criar_backup_agora(prefix="pdv_before_reset")
    except Exception:
        pass

    with conectar() as conn:
        cur = conn.cursor()
        cur.execute("BEGIN IMMEDIATE")

        # Itens primeiro (FK)
        cur.execute("DELETE FROM vendas_itens WHERE loja_id = ?", (int(loja_id),))
        cur.execute("DELETE FROM vendas_cabecalho WHERE loja_id = ?", (int(loja_id),))
        cur.execute("DELETE FROM caixa_sessoes WHERE loja_id = ?", (int(loja_id),))
        cur.execute("DELETE FROM produtos WHERE loja_id = ?", (int(loja_id),))

        # Tabela "vendas" legada (se existir)
        try:
            cur.execute("DELETE FROM vendas WHERE loja_id = ?", (int(loja_id),))
        except Exception:
            pass

        conn.commit()


# =========================
# Cupom (TXT para download)
# (mantive igual; depois, se quiser, eu puxo os dados reais da loja do banco)
# =========================
def cupom_txt(itens: list, numero_venda: str, pagamento_ui: str, desconto: float, recebido: float, troco: float):
    largura = 40
    loja = {
        "nome": "Camargo Celulares",
        "cnpj": "",
        "ie": "",
        "endereco": "",
        "cidade": "",
        "telefone": "",
        "mensagem": "OBRIGADO! VOLTE SEMPRE :)",
        "mostrar_cupom_nao_fiscal": True,
    }

    def centralizar(t):
        t = (t or "").strip()
        return t[:largura] if len(t) >= largura else t.center(largura)

    def sep(ch="-"):
        return ch * largura

    def linha_valor(rotulo, valor):
        val = brl(valor)
        esp = max(1, largura - len(rotulo) - len(val))
        return f"{rotulo}{' ' * esp}{val}"

    def fmt_l2(qtd, unit, tot):
        left = f"{qtd} x {brl(unit)}"
        right = brl(tot)
        esp = max(1, largura - len(left) - len(right))
        return f"{left}{' ' * esp}{right}"

    dt = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    subtotal = sum(float(i["total_item"]) for i in itens)
    total_pagar = max(0.0, float(subtotal) - float(desconto))

    out = []
    if loja["mostrar_cupom_nao_fiscal"]:
        out.append(centralizar("CUPOM NAO FISCAL"))
    out.append(centralizar(loja["nome"]))
    if loja["cnpj"]:
        out.append(centralizar(f"CNPJ: {loja['cnpj']}"))
    out.append(sep("="))
    out.append(f"DATA: {dt}")
    out.append(f"VENDA: {numero_venda}")
    out.append(sep("-"))
    out.append("ITENS")
    out.append(sep("-"))

    for it in itens:
        nome = f"{it.get('produto','')} ({it.get('codigo','')})".strip()
        out.append(nome[:largura])
        out.append(fmt_l2(int(it["qtd"]), float(it["preco_unit"]), float(it["total_item"])))

    out.append(sep("-"))
    out.append(linha_valor("SUBTOTAL", subtotal))
    if float(desconto) > 0:
        out.append(linha_valor("DESCONTO", float(desconto)))
    out.append(linha_valor("TOTAL", total_pagar))
    out.append(sep("-"))
    out.append(f"PAGAMENTO: {pagamento_ui}")
    if pagamento_ui == "Dinheiro":
        out.append(linha_valor("RECEBIDO", float(recebido)))
        out.append(linha_valor("TROCO", float(troco)))
    out.append(sep("="))
    out.append(centralizar(loja["mensagem"]))
    out.append(sep("="))
    return "\n".join([l for l in out if l is not None])


# =========================
# App (UI)
# =========================
st.set_page_config(page_title=APP_TITLE, layout="wide")

# inicializa tabelas
inicializar_banco()
inicializar_usuarios()

if "cart" not in st.session_state:
    st.session_state.cart = []

st.title(APP_TITLE)

# ✅ informações úteis (sem expor demais)
st.caption(f"DB: {DB_PATH}")
st.caption(f"Backups: {BACKUP_DIR} | Retenção: {BACKUP_RETENTION_DAYS} dias | Ativo: {('SIM' if BACKUP_ENABLED else 'NÃO')}")

if IS_RENDER and (BACKUP_DIR or "").startswith("/tmp"):
    st.warning("⚠️ Render Free: backups em /tmp podem SUMIR ao reiniciar. "
               "Recomendado: usar Render Disk e setar BACKUP_DIR=/var/data/backups e DATABASE_PATH=/var/data/pdv.db")


# =========================
# Login (Sidebar)
# =========================
st.sidebar.divider()
st.sidebar.header("🔐 Login")

if "auth" not in st.session_state:
    st.session_state.auth = None  # dict com user

auth = st.session_state.auth

if not auth:
    with st.sidebar.form("login_form"):
        u = st.text_input("Usuário", value="")
        p = st.text_input("Senha", value="", type="password")
        entrar = st.form_submit_button("Entrar")
    if entrar:
        user = autenticar(u, p)
        if not user:
            st.sidebar.error("Usuário/senha inválidos (ou usuário inativo).")
        else:
            st.session_state.auth = {
                "username": user["username"],
                "nome": user["nome"],
                "role": user["role"],
                "loja_id": user.get("loja_id"),  # ✅ importante pro multi-loja
            }
            st.rerun()

    st.info("Faça login para usar o sistema.")
    st.stop()
else:
    st.sidebar.success(f"Logado: {auth['username']} ({auth['role']})")
    if st.sidebar.button("Sair"):
        st.session_state.auth = None
        # limpa loja selecionada ao sair
        if "loja_id" in st.session_state:
            del st.session_state["loja_id"]
        st.rerun()

# =========================
# Seleção / Fixo de Loja
# =========================
role = st.session_state.auth["role"]
user_loja_id = st.session_state.auth.get("loja_id")

st.sidebar.divider()
st.sidebar.header("🏪 Loja")

df_lojas = listar_lojas_df()
lojas_ativas = df_lojas[df_lojas["ativa"] == 1] if not df_lojas.empty else df_lojas

if "loja_id" not in st.session_state:
    # define default
    if role == "ADMIN":
        st.session_state.loja_id = int(lojas_ativas.iloc[0]["id"]) if not lojas_ativas.empty else 1
    else:
        # DONO/OPERADOR: loja fixa
        st.session_state.loja_id = int(user_loja_id or 1)

if role == "ADMIN":
    opcoes = [f"{int(r.id)} — {r.nome}" for r in lojas_ativas.itertuples(index=False)]
    ids = [int(r.id) for r in lojas_ativas.itertuples(index=False)]
    try:
        idx = ids.index(int(st.session_state.loja_id))
    except Exception:
        idx = 0

    escolha = st.sidebar.selectbox("Selecionar loja", opcoes, index=idx)
    loja_id_ativa = int(escolha.split("—")[0].strip())
    st.session_state.loja_id = loja_id_ativa
else:
    loja_id_ativa = int(st.session_state.loja_id)
    st.sidebar.info(f"Loja fixa: **{get_loja_nome(loja_id_ativa)}**")

st.sidebar.caption(f"Loja ativa ID: {loja_id_ativa}")


# =========================
# Admin — Backup manual (UI)
# =========================
if role == "ADMIN":
    st.sidebar.divider()
    st.sidebar.header("🧰 Admin: Backup")

    if st.sidebar.button("📦 Gerar backup agora"):
        try:
            bp = criar_backup_agora(prefix="pdv_manual")
            if bp:
                st.sidebar.success(f"Backup criado: {os.path.basename(bp)}")
            else:
                st.sidebar.warning("Backup não criado (DB ainda não existe ou backup desabilitado).")
        except Exception as e:
            st.sidebar.error(f"Falha ao gerar backup: {e}")

    # Lista backups e permite baixar
    try:
        backups = listar_backups(prefix="pdv_manual")[:10]
        if backups:
            st.sidebar.caption("Últimos backups manuais:")
            for fp in backups:
                try:
                    nome = os.path.basename(fp)
                    with open(fp, "rb") as f:
                        st.sidebar.download_button(
                            label=f"⬇️ {nome}",
                            data=f.read(),
                            file_name=nome,
                            mime="application/octet-stream",
                        )
                except Exception:
                    pass
        else:
            st.sidebar.caption("Nenhum backup manual ainda.")
    except Exception:
        st.sidebar.caption("Não foi possível listar backups.")

# =========================
# Navegação por perfil
# =========================
paginas = ["🧾 Caixa (PDV)", "📈 Histórico"]

# ✅ Agora OPERADOR também acessa Estoque e Relatórios
if role in ("ADMIN", "DONO", "OPERADOR"):
    paginas.insert(1, "📦 Estoque")
    paginas.append("📅 Relatórios")

# ✅ Só ADMIN gerencia usuários
if role == "ADMIN":
    paginas.append("👤 Usuários (Admin)")

pagina = st.sidebar.radio("Navegação", paginas, index=0)

# =========================
# Caixa (sidebar abrir/fechar sempre visível) — MULTI-LOJA
# =========================
st.sidebar.divider()
st.sidebar.header("Caixa (Abertura/Fechamento)")

sess = get_sessao_aberta(loja_id_ativa)

if not sess:
    st.sidebar.error("CAIXA FECHADO")
    with st.sidebar.form("abrir_caixa"):
        operador = st.text_input("Operador (opcional)", value=auth.get("username", ""))
        saldo_ini = st.number_input("Saldo inicial (fundo)", min_value=0.0, step=10.0, format="%.2f")
        obs = st.text_input("Observação (opcional)", value="")
        ok = st.form_submit_button("🔓 Abrir Caixa")
    if ok:
        try:
            sid = abrir_caixa_db(loja_id_ativa, saldo_ini, operador, obs)
            st.sidebar.success(f"Caixa aberto! Sessão #{sid}")
            st.rerun()
        except Exception as e:
            st.sidebar.error(str(e))
else:
    sid, aberto_em, saldo_ini, operador = sess
    st.sidebar.success(f"ABERTO — Sessão #{sid}")
    st.sidebar.caption(f"Aberto em: {aberto_em}")
    if operador:
        st.sidebar.caption(f"Operador: {operador}")
    st.sidebar.caption(f"Saldo inicial: R$ {brl(saldo_ini)}")

    saldo_inicial, total_vendas, saldo_final_sistema, _ = totais_sessao(loja_id_ativa, int(sid))
    st.sidebar.write(f"Vendas (sistema): **R$ {brl(total_vendas)}**")
    st.sidebar.write(f"Final (sistema): **R$ {brl(saldo_final_sistema)}**")

    rel = relatorio_pagamentos_sessao(loja_id_ativa, int(sid))
    if rel:
        df_rel = pd.DataFrame(rel, columns=["Forma", "Total"])
        df_rel["Total"] = df_rel["Total"].map(lambda x: f"R$ {brl(x)}")
        st.sidebar.dataframe(df_rel, use_container_width=True, hide_index=True)

    with st.sidebar.form("fechar_caixa"):
        contado = st.number_input(
            "Valor contado (informado)",
            min_value=0.0,
            step=10.0,
            value=float(saldo_final_sistema),
            format="%.2f",
        )
        obs_f = st.text_input("Observação (opcional)", value="")
        fechar = st.form_submit_button("🔒 Fechar Caixa")
    if fechar:
        try:
            res = fechar_caixa_db(loja_id_ativa, int(sid), float(contado), obs_f)

            # ✅ Backup no fechamento (ótimo para auditoria/restore)
            try:
                criar_backup_agora(prefix=f"pdv_close_loja{int(loja_id_ativa)}")
            except Exception:
                pass

            st.sidebar.success("Caixa fechado!")
            st.sidebar.write(f"Diferença: **R$ {brl(res['diferenca'])}**")
            st.rerun()
        except Exception as e:
            st.sidebar.error(str(e))


# =========================
# Página: Caixa (PDV)
# =========================
if pagina.startswith("🧾"):
    col1, col2 = st.columns([2.2, 1], gap="large")

    with col1:
        st.subheader(f"🧾 Caixa — {get_loja_nome(loja_id_ativa)}")
        st.caption("Lançar item por código")

        if not sess:
            st.info("Abra o caixa na barra lateral para vender.")
        else:
            with st.form("add_item", clear_on_submit=True):
                codigo = st.text_input("Código", placeholder="Bipe o código / digite e Enter")
                qtd = st.number_input("Quantidade", min_value=1, step=1, value=1)
                add = st.form_submit_button("Adicionar")
            if add:
                prod = buscar_produto_por_codigo(loja_id_ativa, codigo)
                if not prod:
                    st.error("Produto não encontrado pelo código (nesta loja).")
                else:
                    qtd = int(qtd)
                    if qtd > prod["quantidade"]:
                        st.error("Quantidade excede o estoque disponível.")
                    else:
                        st.session_state.cart.append(
                            {
                                "codigo": prod["codigo"],
                                "produto": prod["nome"],
                                "preco_unit": float(prod["preco_venda"]),
                                "preco_custo": float(prod["preco_custo"]),
                                "qtd": int(qtd),
                                "total_item": float(prod["preco_venda"]) * int(qtd),
                            }
                        )
                        st.success("Item adicionado!")

        st.subheader("Carrinho (editável)")

        if st.session_state.cart:
            df_cart = pd.DataFrame(st.session_state.cart)
            df_edit = df_cart[["codigo", "produto", "preco_unit", "qtd"]].copy()

            edited = st.data_editor(
                df_edit,
                use_container_width=True,
                hide_index=True,
                num_rows="fixed",
                disabled=["codigo", "produto"],
                column_config={
                    "preco_unit": st.column_config.NumberColumn("Preço unit.", min_value=0.0, step=0.5),
                    "qtd": st.column_config.NumberColumn("Qtd", min_value=1, step=1),
                },
                key="cart_editor",
            )

            # aplicar edições
            new_cart = []
            for i in range(len(edited)):
                row = edited.iloc[i].to_dict()
                preco = float(row["preco_unit"])
                qtd = int(row["qtd"])
                custo = float(df_cart.iloc[i]["preco_custo"]) if "preco_custo" in df_cart.columns else 0.0
                new_cart.append(
                    {
                        "codigo": str(row["codigo"]),
                        "produto": str(row["produto"]),
                        "preco_unit": preco,
                        "preco_custo": custo,
                        "qtd": qtd,
                        "total_item": preco * qtd,
                    }
                )
            st.session_state.cart = new_cart

            subtotal = float(sum(i["total_item"] for i in st.session_state.cart))
            st.metric("Subtotal", f"R$ {brl(subtotal)}")

            c1, c2, c3 = st.columns(3)
            with c1:
                if st.button("Limpar carrinho"):
                    st.session_state.cart = []
                    st.rerun()
            with c2:
                idx = st.number_input(
                    "Remover item (nº)",
                    min_value=1,
                    max_value=len(st.session_state.cart),
                    value=1,
                    step=1,
                )
            with c3:
                if st.button("Remover"):
                    st.session_state.cart.pop(int(idx) - 1)
                    st.rerun()
        else:
            st.caption("Carrinho vazio.")

    with col2:
        st.subheader("Finalizar venda")

        if not sess:
            st.info("Abra o caixa primeiro.")
        else:
            sid, *_ = sess
            forma_ui = st.selectbox("Forma de pagamento", ["Pix", "Dinheiro", "Cartão Crédito", "Cartão Débito"], index=0)

            desconto_txt = st.text_input("Desconto (R$)", value="0")
            recebido_txt = st.text_input("Recebido (somente dinheiro)", value="0", disabled=(forma_ui != "Dinheiro"))

            df_cart = pd.DataFrame(st.session_state.cart) if st.session_state.cart else pd.DataFrame()
            subtotal = float(df_cart["total_item"].sum()) if not df_cart.empty else 0.0

            desconto = to_float(desconto_txt)
            if desconto < 0:
                desconto = 0.0
            if desconto > subtotal:
                desconto = subtotal  # ✅ não deixa passar do subtotal

            total_liq = max(0.0, subtotal - float(desconto))

            recebido = to_float(recebido_txt) if forma_ui == "Dinheiro" else 0.0
            troco = max(0.0, recebido - total_liq) if forma_ui == "Dinheiro" else 0.0

            st.write(f"Total: **R$ {brl(subtotal)}**")
            st.write(f"Desconto: **R$ {brl(desconto)}**")
            st.write(f"Total a pagar: **R$ {brl(total_liq)}**")
            if forma_ui == "Dinheiro":
                st.write(f"Troco: **R$ {brl(troco)}**")

            if st.button("✅ FINALIZAR"):
                if not st.session_state.cart:
                    st.error("Carrinho vazio.")
                else:
                    # valida estoque (por loja) antes de gravar
                    for it in st.session_state.cart:
                        prod = buscar_produto_por_codigo(loja_id_ativa, it["codigo"])
                        if not prod:
                            st.error(f"Produto {it['codigo']} não encontrado no estoque desta loja.")
                            st.stop()
                        if int(it["qtd"]) > int(prod["quantidade"]):
                            st.error(f"Estoque insuficiente para {it['produto']}.")
                            st.stop()

                    if forma_ui == "Dinheiro" and recebido < total_liq:
                        st.error("Valor recebido menor que o total com desconto.")
                        st.stop()

                    forma_db = map_forma_pagamento(forma_ui)

                    # ✅ MODO PROFISSIONAL: venda + itens + baixa estoque (tudo em 1 transação)
                    try:
                        venda_id = registrar_venda_completa_db(
                            loja_id=loja_id_ativa,
                            sessao_id=int(sid),
                            itens=st.session_state.cart,
                            forma_pagamento=forma_db,
                            subtotal=subtotal,
                            desconto=float(desconto),
                            total=total_liq,
                            recebido=float(recebido),
                            troco=float(troco),
                            status="FINALIZADA",
                            baixar_estoque=True,
                        )
                    except Exception as e:
                        st.error(f"Erro ao registrar venda: {e}")
                        st.stop()

                    numero_venda = f"{datetime.now().strftime('%Y%m%d')}-{venda_id:06d}"
                    txt = cupom_txt(
                        st.session_state.cart,
                        numero_venda,
                        forma_ui,
                        float(desconto),
                        float(recebido),
                        float(troco),
                    )

                    st.success(f"Venda registrada! Cupom/ID: {venda_id}")
                    st.download_button(
                        "⬇️ Baixar Cupom TXT",
                        data=txt.encode("utf-8"),
                        file_name=f"cupom_{numero_venda}.txt",
                        mime="text/plain",
                    )

                    st.session_state.cart = []
                    st.rerun()


# =========================
# Página: Estoque (ADMIN/DONO/OPERADOR)
# =========================
elif pagina.startswith("📦"):
    if role not in ("ADMIN", "DONO", "OPERADOR"):
        st.error("Acesso negado. Apenas ADMIN, DONO ou OPERADOR pode acessar o Estoque.")
        st.stop()

    st.subheader(f"📦 Estoque — {get_loja_nome(loja_id_ativa)}")

    cA, cB = st.columns([1, 1], gap="large")

    with cA:
        st.markdown("### Cadastrar / Atualizar (por código)")
        with st.form("produto_form"):
            codigo = st.text_input("Código (barras)", value="")
            nome = st.text_input("Produto", value="")
            custo_txt = st.text_input("Preço de custo", value="0")
            perc_txt = st.text_input("% Lucro (opcional)", value="")
            venda_txt = st.text_input("Preço de venda", value="")
            qtd = st.number_input("Quantidade", min_value=0, step=1, value=0)

            auto_calc = st.checkbox("Calcular venda automaticamente (custo + %)", value=True)
            salvar = st.form_submit_button("💾 Salvar (Upsert)")

        if salvar:
            try:
                custo = to_float(custo_txt)
                perc = to_float(perc_txt) if str(perc_txt).strip() else None

                if auto_calc and (not str(venda_txt).strip()) and perc is not None:
                    venda = float(custo) * (1.0 + float(perc) / 100.0)
                else:
                    venda = to_float(venda_txt)

                upsert_produto(loja_id_ativa, codigo, nome, float(custo), float(venda), int(qtd))
                st.success("Produto salvo!")
                st.rerun()
            except Exception as e:
                st.error(str(e))

        st.markdown("### Excluir produto")
        cod_del = st.text_input("Código para excluir", value="", key="del_code")
        if st.button("🗑️ Excluir"):
            if not cod_del.strip():
                st.warning("Informe um código.")
            else:
                excluir_produto(loja_id_ativa, cod_del.strip())
                st.success("Excluído (se existia) nesta loja.")
                st.rerun()

    with cB:
        st.markdown("### Buscar por código")
        cod_busca = st.text_input("Código", value="", key="busca_code")
        if st.button("🔎 Buscar"):
            prod = buscar_produto_por_codigo(loja_id_ativa, cod_busca)
            if not prod:
                st.warning("Não encontrado nesta loja.")
            else:
                st.info(
                    f"**{prod['nome']}**\n\n"
                    f"Custo: R$ {brl(prod['preco_custo'])} | Venda: R$ {brl(prod['preco_venda'])} | Qtd: {prod['quantidade']}"
                )

    st.divider()
    st.markdown("### Lista de produtos (desta loja)")
    df = listar_produtos_df(loja_id_ativa)
    if df.empty:
        st.info("Sem produtos cadastrados nesta loja.")
    else:
        df_show = df.copy()
        df_show["preco_custo"] = df_show["preco_custo"].map(lambda x: f"R$ {brl(x)}")
        df_show["preco_venda"] = df_show["preco_venda"].map(lambda x: f"R$ {brl(x)}")
        st.dataframe(df_show, use_container_width=True, hide_index=True)


# =========================
# Página: Histórico
# =========================
elif pagina.startswith("📈"):
    st.subheader(f"📈 Histórico de Vendas (itens) — {get_loja_nome(loja_id_ativa)}")

    filtro = st.text_input("Filtrar por produto (contém)", value="")
    df = listar_vendas_itens_df(loja_id_ativa, filtro_produto=filtro)

    if df.empty:
        st.info("Sem vendas (ou filtro sem resultados) nesta loja.")
    else:
        total = float(df["total_item"].sum())
        st.metric("Total vendido (itens filtrados)", f"R$ {brl(total)}")

        df_show = df.copy()
        df_show["preco_unit"] = df_show["preco_unit"].map(lambda x: f"R$ {brl(x)}")
        df_show["total_item"] = df_show["total_item"].map(lambda x: f"R$ {brl(x)}")
        st.dataframe(df_show, use_container_width=True, hide_index=True)


# =========================
# Página: Usuários (ADMIN)
# =========================
elif pagina.startswith("👤"):
    if role != "ADMIN":
        st.error("Acesso negado. Apenas ADMIN pode acessar Usuários.")
        st.stop()

    st.subheader("👤 Usuários (Admin)")

    # ✅ BOTÃO: ZERAR LOJA (estoque/vendas/caixa)
    st.divider()
    st.markdown("### ⚠️ Manutenção (Admin) — Zerar dados de uma loja")
    with st.form("zerar_loja_form"):
        lojas_df = listar_lojas_df()
        opcoes = [f"{int(r.id)} — {r.nome}" for r in lojas_df.itertuples(index=False)]
        loja_sel = st.selectbox("Selecionar loja para ZERAR", opcoes, index=0)
        confirmar = st.checkbox("Confirmo que quero apagar TODOS os dados da loja selecionada")
        apagar = st.form_submit_button("🔥 ZERAR LOJA")

    if apagar:
        if not confirmar:
            st.error("Você precisa confirmar antes de apagar.")
        else:
            loja_id_apagar = int(loja_sel.split("—")[0].strip())
            try:
                # ✅ backup dedicado antes de zerar
                try:
                    criar_backup_agora(prefix=f"pdv_before_reset_loja{int(loja_id_apagar)}")
                except Exception:
                    pass

                zerar_loja(loja_id_apagar)
                st.success("Loja zerada com sucesso!")
                st.rerun()
            except Exception as e:
                st.error(str(e))

    st.divider()
    st.markdown("### Criar novo usuário")
    with st.form("criar_usuario_form"):
        nu = st.text_input("Username (ex: joao)", value="").strip().lower()
        nn = st.text_input("Nome", value="")
        nr = st.selectbox("Perfil", ["OPERADOR", "DONO", "ADMIN"], index=0)

        # loja para dono/operador
        lojas_ativas2 = listar_lojas_df()
        lojas_ativas2 = lojas_ativas2[lojas_ativas2["ativa"] == 1] if not lojas_ativas2.empty else lojas_ativas2
        opcoes_lojas = [f"{int(r.id)} — {r.nome}" for r in lojas_ativas2.itertuples(index=False)]
        loja_sel2 = st.selectbox("Loja do usuário (Dono/Operador)", opcoes_lojas, index=0)
        loja_sel_id = int(loja_sel2.split("—")[0].strip())

        ns = st.text_input("Senha", value="", type="password")
        nativo = st.checkbox("Ativo", value=True)
        criar = st.form_submit_button("Criar")

    if criar:
        try:
            loja_param = None if nr == "ADMIN" else loja_sel_id
            criar_usuario(nu, nn, nr, ns, loja_id=loja_param, ativo=1 if nativo else 0)
            st.success("Usuário criado!")
            st.rerun()
        except Exception as e:
            st.error(str(e))

    st.divider()
    st.markdown("### Lista de usuários")
    dfu = listar_usuarios_df()
    st.dataframe(dfu, use_container_width=True, hide_index=True)

    st.markdown("### Alterar perfil / loja / ativar-desativar")
    with st.form("editar_usuario_form"):
        eu = st.text_input("Username para editar", value="").strip().lower()
        er = st.selectbox("Novo perfil", ["OPERADOR", "DONO", "ADMIN"], index=0)

        lojas_ativas3 = listar_lojas_df()
        lojas_ativas3 = lojas_ativas3[lojas_ativas3["ativa"] == 1] if not lojas_ativas3.empty else lojas_ativas3
        opcoes_lojas3 = [f"{int(r.id)} — {r.nome}" for r in lojas_ativas3.itertuples(index=False)]
        loja_edit = st.selectbox("Nova loja (Dono/Operador)", opcoes_lojas3, index=0)
        loja_edit_id = int(loja_edit.split("—")[0].strip())

        ea = st.checkbox("Ativo", value=True)
        salvar = st.form_submit_button("Salvar alterações")

    if salvar:
        try:
            loja_param = None if er == "ADMIN" else loja_edit_id
            atualizar_usuario_role_ativo(eu, er, 1 if ea else 0, loja_id=loja_param)
            st.success("Atualizado!")
            st.rerun()
        except Exception as e:
            st.error(str(e))

    st.markdown("### Trocar senha")
    with st.form("senha_form"):
        su = st.text_input("Username", value="").strip().lower()
        sp = st.text_input("Nova senha", value="", type="password")
        trocar = st.form_submit_button("Trocar senha")
    if trocar:
        try:
            atualizar_senha(su, sp)
            st.success("Senha alterada!")
        except Exception as e:
            st.error(str(e))


# =========================
# Página: Relatórios (ADMIN/DONO/OPERADOR)
# =========================
else:
    if role not in ("ADMIN", "DONO", "OPERADOR"):
        st.error("Acesso negado. Apenas ADMIN, DONO ou OPERADOR pode acessar Relatórios.")
        st.stop()

    st.subheader(f"📅 Relatórios — {get_loja_nome(loja_id_ativa)}")

    st.markdown("### Vendas por período (por cupom)")
    c1, c2 = st.columns(2)
    with c1:
        d_ini = st.date_input("Data inicial", value=date.today().replace(day=1))
    with c2:
        d_fim = st.date_input("Data final", value=date.today())

    dt_ini = datetime(d_ini.year, d_ini.month, d_ini.day, 0, 0, 0)
    dt_fim = datetime(d_fim.year, d_fim.month, d_fim.day, 23, 59, 59)

    dfp = listar_vendas_por_periodo_df(loja_id_ativa, dt_ini, dt_fim)
    if dfp.empty:
        st.info("Sem vendas no período nesta loja.")
    else:
        total_periodo = float(dfp["total"].sum())
        st.metric("Total do período", f"R$ {brl(total_periodo)}")
        st.dataframe(dfp, use_container_width=True, hide_index=True)

    st.divider()
    st.markdown("### Total por dia do mês")
    ano = st.number_input("Ano", min_value=2000, max_value=2100, value=date.today().year, step=1)
    mes = st.number_input("Mês", min_value=1, max_value=12, value=date.today().month, step=1)
    dfd, total_mes = totais_por_dia_do_mes(loja_id_ativa, int(ano), int(mes))
    st.metric("Total do mês", f"R$ {brl(total_mes)}")
    if dfd.empty:
        st.info("Sem vendas no mês nesta loja.")
    else:
        dfd_show = dfd.copy()
        dfd_show["total"] = dfd_show["total"].map(lambda x: f"R$ {brl(x)}")
        st.dataframe(dfd_show, use_container_width=True, hide_index=True)
