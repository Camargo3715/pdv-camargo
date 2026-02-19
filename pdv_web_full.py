# pdv_web_full.py
# PDV Camargo Celulares — Web (Streamlit) | Completo: Caixa + Estoque + Histórico + Relatórios + Login
# ✅ Multi-loja: 1 banco, dados separados por loja_id (estoque/vendas/caixa/usuários)
# ✅ Admin: botão para ZERAR UMA LOJA (estoque + vendas + caixa) com segurança
# ✅ Backup automático (SQLite): diário + retenção + backup seguro via SQLite backup API

import os
import sqlite3
import secrets
import hashlib
import glob

from datetime import datetime, timedelta

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

    # Se DB ainda não existe, não tenta
    if not os.path.exists(db_path):
        return

    src = None
    dst = None
    try:
        src = sqlite3.connect(db_path, check_same_thread=False, timeout=30)
        src.execute("PRAGMA foreign_keys = ON;")
        # WAL melhora concorrência (Streamlit abre/conecta várias vezes)
        src.execute("PRAGMA journal_mode=WAL;")

        dst = sqlite3.connect(backup_path, check_same_thread=False, timeout=30)
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
    Migra tabela produtos antiga para multi-loja (UNIQUE(loja_id, codigo)).
    ✅ Corrigido: agora é robusto e não quebra se o schema antigo for diferente.
    """
    if not tabela_existe(conn, "produtos"):
        return

    # Se já tem loja_id, assume que já é multi-loja
    if coluna_existe(conn, "produtos", "loja_id"):
        return

    # Detecta colunas existentes (legados possíveis)
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(produtos)")
    cols = [r[1] for r in cur.fetchall()]

    # Mapeia possíveis nomes antigos -> padrão
    # Padrão alvo: codigo, nome, preco_custo, preco_venda, quantidade
    has_codigo = "codigo" in cols
    has_nome = "nome" in cols
    has_pc = "preco_custo" in cols
    has_pv = "preco_venda" in cols
    has_qtd = "quantidade" in cols

    # Legado CSV/primeiros modelos:
    # ["Produto", "Preço", "Quantidade"] ou similares
    legacy_prod = "Produto" in cols
    legacy_preco = "Preço" in cols or "Preco" in cols
    legacy_qtd = "Quantidade" in cols

    # Se não dá pra entender o formato antigo, não migra (evita quebrar o app)
    if not ((has_codigo and has_nome and has_pv and has_qtd) or (legacy_prod and legacy_preco and legacy_qtd)):
        return

    # Lê os dados do formato atual/legado
    rows = []
    try:
        if has_codigo:
            # formato mais novo sem loja_id
            cur.execute("SELECT id, codigo, nome, preco_custo, preco_venda, quantidade FROM produtos")
            rows = cur.fetchall()
        else:
            # formato bem antigo: sem codigo e sem custo
            # cria um "codigo" baseado no id (ou nome) para não perder produto
            col_preco = "Preço" if "Preço" in cols else ("Preco" if "Preco" in cols else None)
            if col_preco is None:
                return
            cur.execute(f"SELECT id, Produto, {col_preco}, Quantidade FROM produtos")
            raw = cur.fetchall()
            for (pid, prod_nome, pv, qtd) in raw:
                rows.append((pid, str(pid), str(prod_nome or ""), 0.0, float(pv or 0), int(qtd or 0)))
    except Exception:
        return

    # Rebuild seguro
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
            (pid, (str(codigo) or "").strip(), str(nome or ""), float(pc or 0), float(pv or 0), int(qtd or 0)),
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

    ✅ Ajustes:
    - timeout maior (evita "database is locked")
    - WAL melhora concorrência no Streamlit/Render
    """
    global DB_PATH
    try:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("PRAGMA journal_mode=WAL;")
        return conn
    except sqlite3.OperationalError:
        # fallback final (Render Free)
        DB_PATH = "/tmp/pdv.db"
        try:
            os.makedirs("/tmp", exist_ok=True)
        except Exception:
            pass
        conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("PRAGMA journal_mode=WAL;")
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
        if not coluna_existe(conn, "usuarios", "loja_id"):
            add_coluna_se_nao_existe(conn, "usuarios", "loja_id INTEGER", "loja_id")
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
from typing import Optional

def criar_usuario(username: str, nome: str, role: str, senha: str, loja_id: Optional[int] = None, ativo: int = 1):
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


def atualizar_usuario_role_ativo(username: str, role: str, ativo: int, loja_id: Optional[int] = None):
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
def to_float(txt) -> float:
    """
    Converte entrada do usuário para float.
    Aceita: "10", "10,50", "1.234,56", "R$ 10,00", "".
    """
    s = str(txt or "").strip()
    if not s:
        return 0.0

    # remove símbolos comuns
    s = s.replace("R$", "").replace("r$", "").strip()
    s = s.replace(" ", "")

    # pt-BR: 1.234,56 -> 1234.56
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


def get_loja_nome(loja_id) -> str:
    """
    Mais robusto: aceita loja_id None/str/int sem quebrar.
    """
    try:
        lid = int(loja_id)
    except Exception:
        return "Loja"

    with conectar() as conn:
        cur = conn.cursor()
        cur.execute("SELECT nome FROM lojas WHERE id = ?", (lid,))
        r = cur.fetchone()
    return str(r[0]) if r else f"Loja {lid}"


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
# App (UI)
# =========================
st.set_page_config(page_title=APP_TITLE, layout="wide")

# inicializa tabelas
inicializar_banco()
inicializar_usuarios()

# ✅ estados globais
if "cart" not in st.session_state:
    st.session_state.cart = []

if "cupom_txt" not in st.session_state:
    st.session_state.cupom_txt = None
if "cupom_nome" not in st.session_state:
    st.session_state.cupom_nome = None
if "cupom_id" not in st.session_state:
    st.session_state.cupom_id = None

if "auth" not in st.session_state:
    st.session_state.auth = None  # dict com user

st.title(APP_TITLE)

# ✅ informações úteis (sem expor demais)
st.caption(f"DB: {DB_PATH}")
st.caption(f"Backups: {BACKUP_DIR} | Retenção: {BACKUP_RETENTION_DAYS} dias | Ativo: {('SIM' if BACKUP_ENABLED else 'NÃO')}")

if IS_RENDER and (BACKUP_DIR or "").startswith("/tmp"):
    st.warning(
        "⚠️ Render Free: backups em /tmp podem SUMIR ao reiniciar. "
        "Recomendado: usar Render Disk e setar BACKUP_DIR=/var/data/backups e DATABASE_PATH=/var/data/pdv.db"
    )

# =========================
# Login (Sidebar)
# =========================
st.sidebar.divider()
st.sidebar.header("🔐 Login")

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
        if "loja_id" in st.session_state:
            del st.session_state["loja_id"]
        st.session_state.cart = []
        st.session_state.cupom_txt = None
        st.session_state.cupom_nome = None
        st.session_state.cupom_id = None
        st.rerun()

# =========================
# Seleção / Fixo de Loja
# =========================
role = st.session_state.auth["role"]
user_loja_id = st.session_state.auth.get("loja_id")

st.sidebar.divider()
st.sidebar.header("🏪 Loja")

df_lojas = listar_lojas_df()
lojas_ativas = df_lojas[df_lojas["ativa"] == 1] if (df_lojas is not None and not df_lojas.empty) else df_lojas

if "loja_id" not in st.session_state:
    if role == "ADMIN":
        st.session_state.loja_id = int(lojas_ativas.iloc[0]["id"]) if (lojas_ativas is not None and not lojas_ativas.empty) else 1
    else:
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

if role in ("ADMIN", "DONO", "OPERADOR"):
    paginas.insert(1, "📦 Estoque")
    paginas.append("📅 Relatórios")

if role == "ADMIN":
    paginas.append("👤 Usuários (Admin)")

# ✅ estado da navegação (sem NameError)
if "pagina" not in st.session_state:
    st.session_state.pagina = paginas[0]

if st.session_state.pagina not in paginas:
    st.session_state.pagina = paginas[0]

pagina = st.sidebar.radio("Navegação", paginas, index=paginas.index(st.session_state.pagina), key="pagina")

# =========================
# Cupom (TXT para download)
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
# Página: Caixa (PDV)
# =========================
# ✅ Atenção: agora esta parte vem DEPOIS do login/loja/navegação

# ✅ sess da loja ativa (função correta é get_sessao_aberta)
sess = None
try:
    sess = get_sessao_aberta(loja_id_ativa)
except Exception:
    sess = None

if isinstance(pagina, str) and pagina.startswith("🧾"):
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
                codigo = (codigo or "").strip()
                if not codigo:
                    st.warning("Digite/bipe um código.")
                else:
                    prod = buscar_produto_por_codigo(loja_id_ativa, codigo)
                    if not prod:
                        st.error("Produto não encontrado pelo código (nesta loja).")
                    else:
                        qtd = int(qtd)
                        if qtd > int(prod.get("quantidade", 0)):
                            st.error("Quantidade excede o estoque disponível.")
                        else:
                            st.session_state.cart.append(
                                {
                                    "codigo": str(prod["codigo"]),
                                    "produto": str(prod["nome"]),
                                    "preco_unit": float(prod["preco_venda"]),
                                    "preco_custo": float(prod.get("preco_custo", 0.0)),
                                    "qtd": int(qtd),
                                    "total_item": float(prod["preco_venda"]) * int(qtd),
                                }
                            )
                            st.success("Item adicionado!")

        st.subheader("Carrinho (editável)")

        if st.session_state.cart:
            df_cart = pd.DataFrame(st.session_state.cart)

            for col in ["codigo", "produto", "preco_unit", "qtd", "preco_custo", "total_item"]:
                if col not in df_cart.columns:
                    df_cart[col] = 0

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

            new_cart = []
            for i in range(len(edited)):
                row = edited.iloc[i].to_dict()
                preco = float(row.get("preco_unit", 0.0))
                qtd_i = int(row.get("qtd", 1))
                custo = float(df_cart.iloc[i].get("preco_custo", 0.0)) if i < len(df_cart) else 0.0

                new_cart.append(
                    {
                        "codigo": str(row.get("codigo", "")),
                        "produto": str(row.get("produto", "")),
                        "preco_unit": preco,
                        "preco_custo": custo,
                        "qtd": qtd_i,
                        "total_item": preco * qtd_i,
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
            sid = int(sess[0])

            forma_ui = st.selectbox(
                "Forma de pagamento",
                ["Pix", "Dinheiro", "Cartão Crédito", "Cartão Débito"],
                index=0
            )

            desconto_txt = st.text_input("Desconto (R$)", value="0")
            recebido_txt = st.text_input(
                "Recebido (somente dinheiro)",
                value="0",
                disabled=(forma_ui != "Dinheiro")
            )

            df_cart = pd.DataFrame(st.session_state.cart) if st.session_state.cart else pd.DataFrame()
            subtotal = float(df_cart["total_item"].sum()) if (not df_cart.empty and "total_item" in df_cart.columns) else 0.0

            desconto = to_float(desconto_txt)
            desconto = max(0.0, min(desconto, subtotal))

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
                    # valida estoque antes de gravar
                    for it in st.session_state.cart:
                        prod = buscar_produto_por_codigo(loja_id_ativa, it["codigo"])
                        if not prod:
                            st.error(f"Produto {it['codigo']} não encontrado no estoque desta loja.")
                            st.stop()
                        if int(it["qtd"]) > int(prod.get("quantidade", 0)):
                            st.error(f"Estoque insuficiente para {it['produto']}.")
                            st.stop()

                    if forma_ui == "Dinheiro" and recebido < total_liq:
                        st.error("Valor recebido menor que o total com desconto.")
                        st.stop()

                    forma_db = map_forma_pagamento(forma_ui)

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

                    numero_venda = f"{datetime.now().strftime('%Y%m%d')}-{int(venda_id):06d}"

                    txt = cupom_txt(
                        st.session_state.cart,
                        numero_venda,
                        forma_ui,
                        float(desconto),
                        float(recebido),
                        float(troco),
                    )

                    st.session_state.cupom_txt = txt
                    st.session_state.cupom_nome = f"cupom_{numero_venda}.txt"
                    st.session_state.cupom_id = venda_id

                    st.session_state.cart = []
                    st.success("Venda registrada com sucesso!")

            if st.session_state.cupom_txt:
                st.divider()
                st.success(f"🧾 Cupom/ID: {st.session_state.cupom_id}")

                st.text_area("Cupom gerado", value=st.session_state.cupom_txt, height=420)

                st.download_button(
                    "⬇️ Baixar Cupom TXT",
                    data=st.session_state.cupom_txt.encode("utf-8"),
                    file_name=st.session_state.cupom_nome or "cupom.txt",
                    mime="text/plain",
                )

                if st.button("🆕 Nova venda (limpar cupom)"):
                    st.session_state.cupom_txt = None
                    st.session_state.cupom_nome = None
                    st.session_state.cupom_id = None
                    st.rerun()
# =========================
# Registrar venda completa (cabecalho + itens + baixa estoque)
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
    baixar_estoque: bool = True,
):
    if not itens:
        raise RuntimeError("Nenhum item informado.")

    with conectar() as conn:
        cur = conn.cursor()
        cur.execute("BEGIN IMMEDIATE")

        # 1️⃣ Inserir cabeçalho
        cur.execute(
            """
            INSERT INTO vendas_cabecalho
            (loja_id, datahora, sessao_id, subtotal, desconto, total,
             forma_pagamento, recebido, troco, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(loja_id),
                agora_iso(),
                int(sessao_id),
                float(subtotal),
                float(desconto),
                float(total),
                forma_pagamento,
                float(recebido),
                float(troco),
                status,
            ),
        )

        venda_id = cur.lastrowid

        # 2️⃣ Inserir itens
        for it in itens:
            cur.execute(
                """
                INSERT INTO vendas_itens
                (loja_id, venda_id, codigo, produto,
                 preco_unit, preco_custo, qtd, total_item)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(loja_id),
                    int(venda_id),
                    it.get("codigo"),
                    it.get("produto"),
                    float(it.get("preco_unit", 0)),
                    float(it.get("preco_custo", 0)),
                    int(it.get("qtd", 0)),
                    float(it.get("total_item", 0)),
                ),
            )

            # 3️⃣ Baixar estoque (se habilitado)
            if baixar_estoque:
                cur.execute(
                    """
                    UPDATE produtos
                    SET quantidade = quantidade - ?
                    WHERE loja_id = ? AND codigo = ? AND quantidade >= ?
                    """,
                    (
                        int(it.get("qtd", 0)),
                        int(loja_id),
                        it.get("codigo"),
                        int(it.get("qtd", 0)),
                    ),
                )
                if cur.rowcount == 0:
                    conn.rollback()
                    raise RuntimeError(f"Estoque insuficiente para {it.get('produto')}.")

        conn.commit()

    return int(venda_id)

# =========================
# Caixa (sidebar abrir/fechar sempre visível) — MULTI-LOJA
# =========================
st.sidebar.divider()
st.sidebar.header("💰 Caixa (Abertura/Fechamento)")

# ✅ sempre recalcula a sessão atual da loja ativa
sess = None
try:
    sess = get_sessao_aberta(loja_id_ativa)
except Exception:
    sess = None

if not sess:
    st.sidebar.error("CAIXA FECHADO")
    with st.sidebar.form("abrir_caixa"):
        operador = st.text_input("Operador (opcional)", value=st.session_state.auth.get("username", ""))
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

            # limpa carrinho e cupom ao fechar (opcional, mas evita confusão)
            st.session_state.cart = []
            st.session_state.cupom_txt = None
            st.session_state.cupom_nome = None
            st.session_state.cupom_id = None

            st.rerun()
        except Exception as e:
            st.sidebar.error(str(e))

# =========================
# PÁGINAS (Obs: NÃO duplicar "Caixa" aqui)
# =========================
# ✅ A página "🧾 Caixa (PDV)" já está renderizada na Parte 3 (no final).
# Aqui ficam apenas as OUTRAS páginas: Estoque, Histórico, Usuários, Relatórios.
