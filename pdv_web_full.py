# pdv_web_full.py
# PDV Camargo Celulares — Web (Streamlit) | Completo: Caixa + Estoque + Histórico + Relatórios + Login
# ✅ Multi-loja: 1 banco, dados separados por loja_id (estoque/vendas/caixa/usuários)
# ✅ Admin: gestão de usuários + seleção de loja + opção (futura) de zerar loja com segurança
# ✅ Backup automático (SQLite): diário + retenção + backup seguro via SQLite backup API

import os
import sqlite3
import secrets
import hashlib
import glob
from typing import Optional

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
# Se você estiver no Render com Disk, set DATABASE_PATH=/var/data/pdv.db
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
        try:
            src.execute("PRAGMA journal_mode=WAL;")
        except Exception:
            pass

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
# ADMIN — ZERAR LOJA (pode ficar aqui embaixo do BACKUP)
# =========================
def zerar_loja_db(loja_id: int) -> dict:
    """
    Zera completamente uma loja:
    - produtos (estoque)
    - vendas_itens + vendas_cabecalho
    - caixa_sessoes
    - vendas (legado, se existir)

    Segurança:
    - BLOQUEIA se existir caixa ABERTO na loja.
    - Transação única (BEGIN IMMEDIATE).
    - NÃO depende de conectar() nem tabela_existe() (pode ficar logo após BACKUP).
    """
    loja_id = int(loja_id)

    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        try:
            conn.execute("PRAGMA foreign_keys = ON;")
        except Exception:
            pass
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
        except Exception:
            pass

        cur = conn.cursor()
        cur.execute("BEGIN IMMEDIATE")

        # 1) Bloqueia se tiver caixa aberto
        cur.execute(
            """
            SELECT id
            FROM caixa_sessoes
            WHERE loja_id = ? AND status = 'ABERTO'
            ORDER BY id DESC
            LIMIT 1
            """,
            (loja_id,),
        )
        row_aberto = cur.fetchone()
        if row_aberto:
            sid = int(row_aberto["id"])
            conn.rollback()
            raise RuntimeError(
                f"Não pode zerar: existe CAIXA ABERTO na loja {loja_id} (Sessão #{sid}). Feche primeiro."
            )

        # 2) Contagens
        cur.execute("SELECT COUNT(*) AS n FROM produtos WHERE loja_id = ?", (loja_id,))
        n_prod = int((cur.fetchone()["n"] or 0))

        cur.execute("SELECT COUNT(*) AS n FROM vendas_cabecalho WHERE loja_id = ?", (loja_id,))
        n_vendas = int((cur.fetchone()["n"] or 0))

        cur.execute("SELECT COUNT(*) AS n FROM vendas_itens WHERE loja_id = ?", (loja_id,))
        n_itens = int((cur.fetchone()["n"] or 0))

        cur.execute("SELECT COUNT(*) AS n FROM caixa_sessoes WHERE loja_id = ?", (loja_id,))
        n_caixas = int((cur.fetchone()["n"] or 0))

        # 3) Apaga em ordem segura
        cur.execute("DELETE FROM vendas_itens WHERE loja_id = ?", (loja_id,))
        cur.execute("DELETE FROM vendas_cabecalho WHERE loja_id = ?", (loja_id,))
        cur.execute("DELETE FROM caixa_sessoes WHERE loja_id = ?", (loja_id,))

        # tabela legado "vendas" pode existir
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='vendas'")
        if cur.fetchone():
            cur.execute("DELETE FROM vendas WHERE loja_id = ?", (loja_id,))

        cur.execute("DELETE FROM produtos WHERE loja_id = ?", (loja_id,))

        conn.commit()

        return {
            "loja_id": loja_id,
            "produtos_apagados": n_prod,
            "vendas_apagadas": n_vendas,
            "itens_apagados": n_itens,
            "caixas_apagados": n_caixas,
        }

    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise

    finally:
        try:
            conn.close()
        except Exception:
            pass


# =========================
# Helpers (DB / Datas)
# =========================
def agora_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def tabela_existe(conn: sqlite3.Connection, nome: str) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (nome,))
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
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS lojas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT NOT NULL,
            ativa INTEGER NOT NULL DEFAULT 1
        )
        """
    )
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
    ✅ Robusto e não quebra se o schema antigo for diferente.
    """
    if not tabela_existe(conn, "produtos"):
        return

    # Se já tem loja_id, assume que já é multi-loja
    if coluna_existe(conn, "produtos", "loja_id"):
        return

    cur = conn.cursor()
    cur.execute("PRAGMA table_info(produtos)")
    cols = [r[1] for r in cur.fetchall()]

    has_codigo = "codigo" in cols
    has_nome = "nome" in cols
    has_pc = "preco_custo" in cols
    has_pv = "preco_venda" in cols
    has_qtd = "quantidade" in cols

    legacy_prod = "Produto" in cols
    legacy_preco = "Preço" in cols or "Preco" in cols
    legacy_qtd = "Quantidade" in cols

    if not ((has_codigo and has_nome and has_pv and has_qtd) or (legacy_prod and legacy_preco and legacy_qtd)):
        return

    rows = []
    try:
        if has_codigo:
            cur.execute("SELECT id, codigo, nome, preco_custo, preco_venda, quantidade FROM produtos")
            rows = cur.fetchall()
        else:
            col_preco = "Preço" if "Preço" in cols else ("Preco" if "Preco" in cols else None)
            if col_preco is None:
                return
            cur.execute(f"SELECT id, Produto, {col_preco}, Quantidade FROM produtos")
            raw = cur.fetchall()
            for (pid, prod_nome, pv, qtd) in raw:
                rows.append((pid, str(pid), str(prod_nome or ""), 0.0, float(pv or 0), int(qtd or 0)))
    except Exception:
        return

    conn.execute(
        """
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
        """
    )

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
    Conecta no SQLite. Se o caminho atual falhar, cai para /tmp/pdv.db.
    """
    global DB_PATH
    try:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
        except Exception:
            pass
        return conn
    except sqlite3.OperationalError:
        DB_PATH = "/tmp/pdv.db"
        try:
            os.makedirs("/tmp", exist_ok=True)
        except Exception:
            pass
        conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
        except Exception:
            pass
        return conn


def executar_query(sql: str, params: tuple = ()):
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        return cur.fetchall()


def executar_exec(sql: str, params: tuple = ()):
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        conn.commit()
        return cur.lastrowid
def inicializar_banco():
    """
    ✅ Inicializa banco MULTI-LOJA.
    - Cria tabela lojas + cadastra 3 lojas (se vazio)
    - Migra produtos legado para multi-loja (tudo vira loja_id=1)
    - Auto-backup diário + retenção (se habilitado)
    """
    with conectar() as conn:
        cur = conn.cursor()

        # 1) Lojas
        garantir_lojas_padrao(conn)

        # 2) Produtos
        if tabela_existe(conn, "produtos"):
            migrar_produtos_para_multiloja(conn)
        else:
            cur.execute(
                """
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
                """
            )

        # (LEGADO) vendas
        cur.execute(
            """
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
            """
        )

        # Caixa sessões
        cur.execute(
            """
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
            """
        )

        # Vendas cabeçalho
        cur.execute(
            """
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
            """
        )

        # Vendas itens
        cur.execute(
            """
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
            """
        )

        # Migração: adiciona loja_id nas tabelas existentes
        for tabela in ["vendas", "caixa_sessoes", "vendas_cabecalho", "vendas_itens"]:
            if tabela_existe(conn, tabela) and not coluna_existe(conn, tabela, "loja_id"):
                add_coluna_se_nao_existe(conn, tabela, "loja_id INTEGER NOT NULL DEFAULT 1", "loja_id")
                conn.execute(f"UPDATE {tabela} SET loja_id = 1 WHERE loja_id IS NULL")

        # Índices
        cur.execute("CREATE INDEX IF NOT EXISTS idx_produtos_loja_codigo ON produtos(loja_id, codigo)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_vendas_cabecalho_loja_datahora ON vendas_cabecalho(loja_id, datahora)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_vendas_cabecalho_sessao ON vendas_cabecalho(sessao_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_vendas_itens_venda ON vendas_itens(venda_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_caixa_loja_status ON caixa_sessoes(loja_id, status)")

        conn.commit()

    auto_backup_se_precisar(prefix="pdv")


# =========================
# Usuários / Auth (Login)
# =========================
def gerar_hash_senha(senha: str) -> tuple[str, str]:
    senha_b = (senha or "").encode("utf-8")
    salt = secrets.token_bytes(16)
    dk = hashlib.pbkdf2_hmac("sha256", senha_b, salt, 120_000)
    return salt.hex(), dk.hex()


def verificar_senha(senha: str, salt_hex: str, hash_hex: str) -> bool:
    senha_b = (senha or "").encode("utf-8")
    salt = bytes.fromhex(salt_hex)
    dk = hashlib.pbkdf2_hmac("sha256", senha_b, salt, 120_000)
    return dk.hex() == (hash_hex or "")


def role_to_tipo(role: str) -> str:
    r = (role or "").strip().upper()
    if r == "ADMIN":
        return "admin"
    if r == "DONO":
        return "dono"
    return "operador"


def inicializar_usuarios():
    """
    Cria tabela usuarios (multi-loja) e cria ADMIN inicial se vazio.
    """
    with conectar() as conn:
        cur = conn.cursor()
        garantir_lojas_padrao(conn)

        cur.execute(
            """
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
            """
        )

        # Migração suave
        if not coluna_existe(conn, "usuarios", "loja_id"):
            add_coluna_se_nao_existe(conn, "usuarios", "loja_id INTEGER", "loja_id")
            try:
                conn.execute("UPDATE usuarios SET loja_id = 1 WHERE (role='OPERADOR') AND (loja_id IS NULL)")
            except Exception:
                pass
            conn.commit()

        cur.execute("CREATE INDEX IF NOT EXISTS idx_usuarios_role ON usuarios(role)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_usuarios_loja ON usuarios(loja_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_usuarios_ativo ON usuarios(ativo)")

        cur.execute("SELECT COUNT(*) AS total FROM usuarios")
        total = int((cur.fetchone()["total"] or 0))

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
        "id": int(row["id"]),
        "username": str(row["username"]),
        "nome": str(row["nome"] or ""),
        "role": str(row["role"]),
        "tipo": role_to_tipo(str(row["role"])),
        "loja_id": (int(row["loja_id"]) if row["loja_id"] is not None else None),
        "salt": str(row["pass_salt"]),
        "hash": str(row["pass_hash"]),
        "ativo": int(row["ativo"] or 0),
    }


def autenticar(username: str, senha: str):
    user = get_usuario(username)
    if not user or user["ativo"] != 1:
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
    s = str(txt or "").strip()
    if not s:
        return 0.0
    s = s.replace("R$", "").replace("r$", "").strip().replace(" ", "")
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
        df = pd.read_sql_query("SELECT id, nome, ativa FROM lojas ORDER BY id ASC", conn)
    return df


def get_loja_nome(loja_id) -> str:
    try:
        lid = int(loja_id)
    except Exception:
        return "Loja"

    with conectar() as conn:
        cur = conn.cursor()
        cur.execute("SELECT nome FROM lojas WHERE id = ?", (lid,))
        r = cur.fetchone()
    return str(r["nome"]) if r else f"Loja {lid}"


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
        "codigo": str(row["codigo"]),
        "nome": str(row["nome"]),
        "preco_custo": float(row["preco_custo"] or 0.0),
        "preco_venda": float(row["preco_venda"] or 0.0),
        "quantidade": int(row["quantidade"] or 0),
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
# Histórico (itens vendidos) — MULTI-LOJA
# =========================
def listar_vendas_itens_df(loja_id: int, filtro_produto: str = ""):
    filtro = f"%{(filtro_produto or '').strip()}%"
    with conectar() as conn:
        df = pd.read_sql_query(
            """
            SELECT
                vc.datahora,
                vi.codigo,
                vi.produto,
                vi.preco_unit,
                vi.qtd,
                vi.total_item,
                vc.forma_pagamento,
                vc.id AS venda_id
            FROM vendas_itens vi
            JOIN vendas_cabecalho vc ON vc.id = vi.venda_id
            WHERE
                vi.loja_id = ?
                AND vc.status = 'FINALIZADA'
                AND vi.produto LIKE ?
            ORDER BY vc.datahora DESC, vi.id DESC
            """,
            conn,
            params=(int(loja_id), filtro),
        )
    return df


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
        raise RuntimeError(f"Já existe um caixa ABERTO nesta loja (Sessão #{atual['id']}). Feche antes de abrir outro.")
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

        cur.execute(
            """
            SELECT saldo_inicial, aberto_em
            FROM caixa_sessoes
            WHERE id = ? AND loja_id = ?
            """,
            (int(sessao_id), int(loja_id)),
        )
        row = cur.fetchone()
        saldo_inicial = float(row["saldo_inicial"] or 0.0) if row else 0.0
        aberto_em = row["aberto_em"] if row else ""

        cur.execute(
            """
            SELECT COALESCE(SUM(total), 0) AS total
            FROM vendas_cabecalho
            WHERE loja_id = ? AND sessao_id = ? AND status='FINALIZADA'
            """,
            (int(loja_id), int(sessao_id)),
        )
        total_vendas = float((cur.fetchone()["total"] or 0.0))

        saldo_final_sistema = saldo_inicial + total_vendas
        return saldo_inicial, total_vendas, saldo_final_sistema, aberto_em


def relatorio_pagamentos_sessao(loja_id: int, sessao_id: int):
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT forma_pagamento, COALESCE(SUM(total), 0) AS total
            FROM vendas_cabecalho
            WHERE loja_id = ? AND sessao_id = ? AND status='FINALIZADA'
            GROUP BY forma_pagamento
            ORDER BY forma_pagamento
            """,
            (int(loja_id), int(sessao_id)),
        )
        return [(str(r["forma_pagamento"]), float(r["total"] or 0.0)) for r in cur.fetchall()]


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
# Vendas (registrar venda completa) — MULTI-LOJA
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
) -> int:
    if not itens:
        raise RuntimeError("Carrinho vazio.")

    with conectar() as conn:
        cur = conn.cursor()
        try:
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
                    str(forma_pagamento or "OUTRO"),
                    float(recebido or 0.0),
                    float(troco or 0.0),
                    str(status or "FINALIZADA"),
                ),
            )
            venda_id = int(cur.lastrowid)

            for it in itens:
                codigo = str(it.get("codigo", "")).strip()
                produto = str(it.get("produto", "")).strip()
                preco_unit = float(it.get("preco_unit", 0.0) or 0.0)
                preco_custo = float(it.get("preco_custo", 0.0) or 0.0)
                qtd = int(it.get("qtd", 0) or 0)
                total_item = float(it.get("total_item", 0.0) or (preco_unit * qtd))

                if qtd <= 0:
                    raise RuntimeError("Quantidade inválida no carrinho.")

                # baixa estoque dentro da mesma transação (garante consistência)
                if baixar_estoque:
                    cur.execute(
                        """
                        UPDATE produtos
                        SET quantidade = quantidade - ?
                        WHERE loja_id = ? AND codigo = ? AND quantidade >= ?
                        """,
                        (qtd, int(loja_id), codigo, qtd),
                    )
                    if cur.rowcount == 0:
                        raise RuntimeError(f"Estoque insuficiente para o item: {produto} ({codigo})")

                cur.execute(
                    """
                    INSERT INTO vendas_itens
                    (loja_id, venda_id, codigo, produto, preco_unit, preco_custo, qtd, total_item)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (int(loja_id), venda_id, codigo, produto, preco_unit, preco_custo, qtd, total_item),
                )

            conn.commit()
            return venda_id

        except Exception:
            conn.rollback()
            raise


# =========================
# Cupom TXT (simples)
# =========================
def cupom_txt(itens: list, numero_venda: str, forma_ui: str, desconto: float, recebido: float, troco: float) -> str:
    linhas = []
    linhas.append("PDV Camargo Celulares")
    linhas.append(f"Venda: {numero_venda}")
    linhas.append(f"Data: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    linhas.append("-" * 38)
    for it in itens:
        nome = str(it.get("produto", ""))[:28]
        qtd = int(it.get("qtd", 0) or 0)
        pu = float(it.get("preco_unit", 0.0) or 0.0)
        tt = float(it.get("total_item", pu * qtd))
        linhas.append(f"{nome}")
        linhas.append(f"  {qtd} x {brl(pu)} = {brl(tt)}")
    linhas.append("-" * 38)
    subtotal = float(sum(float(i.get("total_item", 0.0) or 0.0) for i in itens))
    total = max(0.0, subtotal - float(desconto or 0.0))
    linhas.append(f"Subtotal: R$ {brl(subtotal)}")
    linhas.append(f"Desconto: R$ {brl(desconto)}")
    linhas.append(f"Total:    R$ {brl(total)}")
    linhas.append(f"Pagamento: {forma_ui}")
    if (forma_ui or "").strip().lower() == "dinheiro":
        linhas.append(f"Recebido: R$ {brl(recebido)}")
        linhas.append(f"Troco:    R$ {brl(troco)}")
    linhas.append("-" * 38)
    linhas.append("OBRIGADO! VOLTE SEMPRE :)")
    return "\n".join(linhas)
# =========================
# Relatórios — MULTI-LOJA
# =========================
def listar_vendas_por_periodo_df(loja_id: int, dt_ini: datetime, dt_fim: datetime):
    ini = dt_ini.strftime("%Y-%m-%d %H:%M:%S")
    fim = dt_fim.strftime("%Y-%m-%d %H:%M:%S")
    with conectar() as conn:
        df = pd.read_sql_query(
            """
            SELECT
                id,
                datahora,
                subtotal,
                desconto,
                total,
                forma_pagamento,
                recebido,
                troco,
                status
            FROM vendas_cabecalho
            WHERE loja_id = ?
              AND status = 'FINALIZADA'
              AND datahora BETWEEN ? AND ?
            ORDER BY datahora DESC, id DESC
            """,
            conn,
            params=(int(loja_id), ini, fim),
        )
    return df


def totais_por_dia_do_mes(loja_id: int, ano: int, mes: int):
    # yyyy-mm
    ym = f"{int(ano):04d}-{int(mes):02d}"
    with conectar() as conn:
        df = pd.read_sql_query(
            """
            SELECT
                substr(datahora, 1, 10) AS dia,
                COALESCE(SUM(total), 0) AS total
            FROM vendas_cabecalho
            WHERE loja_id = ?
              AND status = 'FINALIZADA'
              AND substr(datahora, 1, 7) = ?
            GROUP BY substr(datahora, 1, 10)
            ORDER BY dia ASC
            """,
            conn,
            params=(int(loja_id), ym),
        )
    total_mes = float(df["total"].sum()) if (df is not None and not df.empty and "total" in df.columns) else 0.0
    return df, total_mes


# =========================
# UI (Streamlit) — ordem correta
# =========================
st.set_page_config(page_title=APP_TITLE, layout="wide")

# Inicializa banco e usuários
inicializar_banco()
inicializar_usuarios()

# ✅ Estados globais (blindagem NameError)
if "auth" not in st.session_state:
    st.session_state.auth = None
if "cart" not in st.session_state:
    st.session_state.cart = []
if "cupom_txt" not in st.session_state:
    st.session_state.cupom_txt = None
if "cupom_nome" not in st.session_state:
    st.session_state.cupom_nome = None
if "cupom_id" not in st.session_state:
    st.session_state.cupom_id = None
if "loja_id_ativa" not in st.session_state:
    st.session_state.loja_id_ativa = None
if "pagina" not in st.session_state:
    st.session_state.pagina = "🧾 Caixa (PDV)"

st.title(APP_TITLE)
st.caption(f"DB: {DB_PATH}")
st.caption(f"Backup dir: {BACKUP_DIR} | Enabled: {BACKUP_ENABLED}")

# =========================
# Login
# =========================
auth = st.session_state.get("auth") or {}

def _tipo_atual(a: dict) -> str:
    t = (a.get("tipo") or a.get("role") or "").strip().lower()
    if (a.get("role") or "").strip().upper() == "ADMIN":
        return "admin"
    if (a.get("role") or "").strip().upper() == "DONO":
        return "dono"
    if (a.get("role") or "").strip().upper() == "OPERADOR":
        return "operador"
    return t or "operador"

tipo = _tipo_atual(auth)

with st.sidebar:
    st.header("🔐 Acesso")
    if auth:
        st.success(f"Logado: {auth.get('username','')} ({auth.get('role','')})")
        if st.button("Sair"):
            st.session_state.auth = None
            st.session_state.cart = []
            st.session_state.cupom_txt = None
            st.session_state.cupom_nome = None
            st.session_state.cupom_id = None
            st.rerun()
    else:
        with st.form("login_form"):
            u = st.text_input("Usuário", value="")
            p = st.text_input("Senha", value="", type="password")
            ok = st.form_submit_button("Entrar")
        if ok:
            user = autenticar(u, p)
            if not user:
                st.error("Usuário/senha inválidos ou usuário inativo.")
            else:
                st.session_state.auth = user
                st.rerun()

# Se não logou, para aqui (evita UI quebrar e evita NameError em loja/páginas)
auth = st.session_state.get("auth") or {}
if not auth:
    st.info("Faça login para acessar o PDV.")
    st.stop()

tipo = _tipo_atual(auth)

# =========================
# Loja ativa (antes do caixa/páginas)
# =========================
def _to_int(v, default=1):
    try:
        return int(v)
    except Exception:
        return int(default)

# Se dono/operador: força loja do usuário
if tipo in ("dono", "operador") and auth.get("loja_id"):
    st.session_state.loja_id_ativa = _to_int(auth.get("loja_id"), 1)

# Se admin: pode escolher no sidebar
if tipo == "admin":
    try:
        df_lojas = listar_lojas_df()
        lojas_ativas = df_lojas[df_lojas["ativa"] == 1] if (df_lojas is not None and not df_lojas.empty) else df_lojas
    except Exception:
        lojas_ativas = None

    with st.sidebar:
        st.divider()
        st.header("🏪 Loja")
        if lojas_ativas is None or lojas_ativas.empty:
            st.warning("Nenhuma loja encontrada.")
        else:
            opcoes = [(int(r["id"]), str(r["nome"])) for _, r in lojas_ativas.iterrows()]
            ids = [x[0] for x in opcoes]
            nomes = [f"{x[0]} — {x[1]}" for x in opcoes]

            atual = st.session_state.loja_id_ativa
            if atual is None or int(atual) not in ids:
                st.session_state.loja_id_ativa = ids[0]

            idx = ids.index(int(st.session_state.loja_id_ativa))
            escolhido = st.selectbox("Selecione a loja", options=list(range(len(opcoes))), format_func=lambda i: nomes[i], index=idx)
            st.session_state.loja_id_ativa = ids[int(escolhido)]

# fallback geral
if st.session_state.loja_id_ativa is None:
    st.session_state.loja_id_ativa = _to_int(auth.get("loja_id", 1), 1)

loja_id_ativa = _to_int(st.session_state.loja_id_ativa, 1)

# =========================
# Navegação (pagina definida ANTES de usar)
# =========================
paginas = ["🧾 Caixa (PDV)", "📦 Estoque", "📈 Histórico", "📅 Relatórios"]
if tipo == "admin":
    paginas.append("👤 Usuários (Admin)")

if st.session_state.pagina not in paginas:
    st.session_state.pagina = "🧾 Caixa (PDV)"

pagina = st.sidebar.radio("Navegação", paginas, index=paginas.index(st.session_state.pagina), key="pagina")

# =========================
# Sidebar — Caixa (sempre visível, mas só funciona com loja_id_ativa válido)
# =========================
st.sidebar.divider()
st.sidebar.header("💰 Caixa (Abertura/Fechamento)")

def _sess_get(sess_obj, key_or_index, default=None):
    if sess_obj is None:
        return default
    try:
        if hasattr(sess_obj, "keys"):
            return sess_obj[key_or_index]
    except Exception:
        pass
    try:
        return sess_obj[key_or_index]
    except Exception:
        return default

try:
    sess = get_sessao_aberta(loja_id_ativa)
except Exception:
    sess = None

if not sess:
    st.sidebar.error("CAIXA FECHADO")

    with st.sidebar.form("abrir_caixa"):
        operador = st.text_input("Operador (opcional)", value=(auth.get("username", "") if auth else ""))
        saldo_ini = st.number_input("Saldo inicial (fundo)", min_value=0.0, step=10.0, format="%.2f")
        obs = st.text_input("Observação (opcional)", value="")
        ok = st.form_submit_button("🔓 Abrir Caixa")

    if ok:
        try:
            sid_new = abrir_caixa_db(loja_id_ativa, float(saldo_ini), operador, obs)
            st.sidebar.success(f"Caixa aberto! Sessão #{sid_new}")
            st.rerun()
        except Exception as e:
            st.sidebar.error(str(e))
else:
    sid = int(_sess_get(sess, "id", 0) or 0)
    aberto_em = _sess_get(sess, "aberto_em", "")
    saldo_ini = float(_sess_get(sess, "saldo_inicial", 0.0) or 0.0)
    operador = _sess_get(sess, "operador", "") or ""

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
        contado = st.number_input("Valor contado (informado)", min_value=0.0, step=10.0, value=float(saldo_final_sistema), format="%.2f")
        obs_f = st.text_input("Observação (opcional)", value="")
        fechar = st.form_submit_button("🔒 Fechar Caixa")

    if fechar:
        try:
            res = fechar_caixa_db(loja_id_ativa, int(sid), float(contado), obs_f)
            try:
                criar_backup_agora(prefix=f"pdv_close_loja{int(loja_id_ativa)}")
            except Exception:
                pass

            st.sidebar.success("Caixa fechado!")
            st.sidebar.write(f"Diferença: **R$ {brl(res['diferenca'])}**")

            st.session_state.cart = []
            st.session_state.cupom_txt = None
            st.session_state.cupom_nome = None
            st.session_state.cupom_id = None
            st.rerun()
        except Exception as e:
            st.sidebar.error(str(e))

# =========================
# PÁGINAS
# =========================
try:
    sess = get_sessao_aberta(loja_id_ativa)
except Exception:
    sess = None

# Página: Caixa (PDV)
if pagina == "🧾 Caixa (PDV)":
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
                idx = st.number_input("Remover item (nº)", min_value=1, max_value=len(st.session_state.cart), value=1, step=1)
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
            sid = int(_sess_get(sess, "id", 0) or 0)

            forma_ui = st.selectbox("Forma de pagamento", ["Pix", "Dinheiro", "Cartão Crédito", "Cartão Débito"], index=0)
            desconto_txt = st.text_input("Desconto (R$)", value="0")
            recebido_txt = st.text_input("Recebido (somente dinheiro)", value="0", disabled=(forma_ui != "Dinheiro"))

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
                    st.stop()

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
                txt = cupom_txt(st.session_state.cart, numero_venda, forma_ui, float(desconto), float(recebido), float(troco))

                st.session_state.cupom_txt = txt
                st.session_state.cupom_nome = f"cupom_{numero_venda}.txt"
                st.session_state.cupom_id = venda_id
                st.session_state.cart = []
                st.rerun()

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

# Página: Estoque
elif pagina == "📦 Estoque":
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

# Página: Histórico
elif pagina == "📈 Histórico":
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

# Página: Relatórios
elif pagina == "📅 Relatórios":
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

# Página: Usuários (Admin)
elif pagina == "👤 Usuários (Admin)":
    if tipo != "admin":
        st.error("Acesso negado. Apenas ADMIN pode acessar Usuários.")
        st.stop()

    st.subheader("👤 Usuários (Admin)")
    st.dataframe(listar_usuarios_df(), use_container_width=True, hide_index=True)

    st.divider()
    st.markdown("### Criar novo usuário")
    with st.form("novo_usuario"):
        nu = st.text_input("Username", value="")
        nn = st.text_input("Nome", value="")
        nr = st.selectbox("Role", ["ADMIN", "DONO", "OPERADOR"], index=2)
        nl = st.number_input("Loja ID (DONO/OPERADOR)", min_value=1, step=1, value=int(loja_id_ativa))
        ns = st.text_input("Senha", value="", type="password")
        na = st.checkbox("Ativo", value=True)
        criar = st.form_submit_button("Criar")

    if criar:
        try:
            criar_usuario(nu, nn, nr, ns, loja_id=(None if nr == "ADMIN" else int(nl)), ativo=(1 if na else 0))
            st.success("Usuário criado!")
            st.rerun()
        except Exception as e:
            st.error(str(e))
