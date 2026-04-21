# pdv_web_full.py
# PDV Camargo Celulares — Web (Streamlit) | Completo: Caixa + Estoque + Histórico + Relatórios + Login
# ✅ Multi-loja: 1 banco, dados separados por loja_id (estoque/vendas/caixa/usuários)
# ✅ Admin: gestão de usuários + seleção de loja + opção (futura) de zerar loja com segurança
# ✅ Backup automático (SQLite): diário + retenção + backup seguro via SQLite backup API
# ✅ Caixa: autocomplete "igual Google" com streamlit-searchbox (fora do st.form)

import os
import sqlite3
import secrets
import hashlib
import glob
from typing import Optional
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo

import streamlit as st
import pandas as pd
from streamlit_searchbox import st_searchbox


APP_TITLE = "PDV Camargo Celulares 2.0 — Web"
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
    return datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%Y-%m-%d %H:%M:%S")


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
# Banco (ATUALIZADO com seed loja_config + helpers + alias get_loja_config)
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


# =========================
# Loja Config (seed + leitura)
# =========================
def _config_padrao(loja_nome: str = "") -> dict:
    return {
        "nome_fantasia": (loja_nome or "Camargo Celulares"),
        "razao_social": (loja_nome or "Camargo Celulares"),
        "cnpj": "",
        "ie": "ISENTO",
        "endereco": "",
        "cidade_uf": "",
        "telefone": "",
        "mensagem": "OBRIGADO! VOLTE SEMPRE :)",
        "mostrar_cupom_nao_fiscal": 1,
        "atualizado_em": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def seed_loja_config(conn):
    """
    ✅ Seed automático: cria registro padrão em loja_config para lojas sem config.
    """
    cur = conn.cursor()

    cur.execute("""
        SELECT l.id, l.nome
        FROM lojas l
        LEFT JOIN loja_config c ON c.loja_id = l.id
        WHERE c.loja_id IS NULL
    """)
    faltantes = cur.fetchall()

    for row in faltantes:
        loja_id = int(row[0])
        loja_nome = row[1] or ""
        cfg = _config_padrao(loja_nome)

        cur.execute("""
            INSERT INTO loja_config (
                loja_id, nome_fantasia, razao_social, cnpj, ie,
                endereco, cidade_uf, telefone, mensagem,
                mostrar_cupom_nao_fiscal, atualizado_em
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            loja_id,
            cfg["nome_fantasia"],
            cfg["razao_social"],
            cfg["cnpj"],
            cfg["ie"],
            cfg["endereco"],
            cfg["cidade_uf"],
            cfg["telefone"],
            cfg["mensagem"],
            cfg["mostrar_cupom_nao_fiscal"],
            cfg["atualizado_em"],
        ))

    conn.commit()


def obter_loja_config(loja_id: int) -> dict:
    """
    Lê config da loja. Se não existir, cria fallback na hora.
    """
    with conectar() as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        cur.execute("SELECT * FROM loja_config WHERE loja_id = ?", (int(loja_id),))
        row = cur.fetchone()

        if row is None:
            cur.execute("SELECT nome FROM lojas WHERE id = ?", (int(loja_id),))
            loja = cur.fetchone()
            loja_nome = (loja["nome"] if loja else "") or ""

            cfg = _config_padrao(loja_nome)
            cur.execute("""
                INSERT INTO loja_config (
                    loja_id, nome_fantasia, razao_social, cnpj, ie,
                    endereco, cidade_uf, telefone, mensagem,
                    mostrar_cupom_nao_fiscal, atualizado_em
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                int(loja_id),
                cfg["nome_fantasia"],
                cfg["razao_social"],
                cfg["cnpj"],
                cfg["ie"],
                cfg["endereco"],
                cfg["cidade_uf"],
                cfg["telefone"],
                cfg["mensagem"],
                cfg["mostrar_cupom_nao_fiscal"],
                cfg["atualizado_em"],
            ))
            conn.commit()

            cur.execute("SELECT * FROM loja_config WHERE loja_id = ?", (int(loja_id),))
            row = cur.fetchone()

        return dict(row) if row else {}


# ✅ Alias (mantém compatível com cfg = get_loja_config(...))
def get_loja_config(loja_id: int) -> dict:
    return obter_loja_config(loja_id)

# =========================
# Loja Config — salvar (upsert)
# =========================
def upsert_loja_config(loja_id: int, dados: dict):
    """
    Salva/atualiza dados em loja_config (por loja).
    Se não existir, cria (via obter_loja_config) e depois atualiza.
    """
    loja_id = int(loja_id)
    dados = dados or {}

    allowed = {
        "nome_fantasia",
        "razao_social",
        "cnpj",
        "ie",
        "endereco",
        "cidade_uf",
        "telefone",
        "mensagem",
        "mostrar_cupom_nao_fiscal",
    }
    clean = {k: dados.get(k) for k in allowed if k in dados}

    if "mostrar_cupom_nao_fiscal" in clean:
        clean["mostrar_cupom_nao_fiscal"] = 1 if int(clean["mostrar_cupom_nao_fiscal"] or 0) == 1 else 0

    atualizado_em = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # garante registro existente (seed/fallback)
    _ = obter_loja_config(loja_id)

    with conectar() as conn:
        cur = conn.cursor()
        cur.execute("""
            UPDATE loja_config
               SET nome_fantasia = COALESCE(?, nome_fantasia),
                   razao_social  = COALESCE(?, razao_social),
                   cnpj          = COALESCE(?, cnpj),
                   ie            = COALESCE(?, ie),
                   endereco      = COALESCE(?, endereco),
                   cidade_uf     = COALESCE(?, cidade_uf),
                   telefone      = COALESCE(?, telefone),
                   mensagem      = COALESCE(?, mensagem),
                   mostrar_cupom_nao_fiscal = COALESCE(?, mostrar_cupom_nao_fiscal),
                   atualizado_em = ?
             WHERE loja_id = ?
        """, (
            clean.get("nome_fantasia"),
            clean.get("razao_social"),
            clean.get("cnpj"),
            clean.get("ie"),
            clean.get("endereco"),
            clean.get("cidade_uf"),
            clean.get("telefone"),
            clean.get("mensagem"),
            clean.get("mostrar_cupom_nao_fiscal"),
            atualizado_em,
            loja_id,
        ))
        conn.commit()



# =========================
# Inicialização do Banco
# =========================
def inicializar_banco():
    """
    ✅ Inicializa banco MULTI-LOJA.
    - Cria tabela lojas + cadastra lojas padrão (se vazio)
    - Migra produtos legado para multi-loja (tudo vira loja_id=1)
    - Cria loja_config e faz seed automático por loja
    - Auto-backup diário + retenção (se habilitado)
    """
    with conectar() as conn:
        cur = conn.cursor()

        # 1) Lojas
        garantir_lojas_padrao(conn)

        # 1.1) Configuração do cupom por loja (Painel do Proprietário)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS loja_config (
                loja_id INTEGER PRIMARY KEY,
                nome_fantasia TEXT,
                razao_social TEXT,
                cnpj TEXT,
                ie TEXT,
                endereco TEXT,
                cidade_uf TEXT,
                telefone TEXT,
                mensagem TEXT,
                mostrar_cupom_nao_fiscal INTEGER DEFAULT 1,
                atualizado_em TEXT,
                FOREIGN KEY (loja_id) REFERENCES lojas(id) ON DELETE CASCADE
            )
        """)

        # ✅ SEED automático (antes/independente do resto)
        seed_loja_config(conn)

        # 2) Produtos
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

        # (LEGADO) vendas
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

        # Caixa sessões
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

        # Vendas cabeçalho
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

        # Vendas itens
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

        # Vendas pagamentos (novo: suporta pagamento misto)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS vendas_pagamentos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                loja_id INTEGER NOT NULL DEFAULT 1,
                venda_id INTEGER NOT NULL,
                sessao_id INTEGER,
                forma_pagamento TEXT NOT NULL,
                valor REAL NOT NULL DEFAULT 0,
                criado_em TEXT,
                FOREIGN KEY (loja_id) REFERENCES lojas(id),
                FOREIGN KEY (venda_id) REFERENCES vendas_cabecalho(id) ON DELETE CASCADE,
                FOREIGN KEY (sessao_id) REFERENCES caixa_sessoes(id)
            )
        """)

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
        cur.execute("CREATE INDEX IF NOT EXISTS idx_vendas_pagamentos_loja_sessao ON vendas_pagamentos(loja_id, sessao_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_vendas_pagamentos_venda ON vendas_pagamentos(venda_id)")

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

    if r in ["pix"]:
        return "PIX"

    if r in ["dinheiro", "espécie", "especie"]:
        return "DINHEIRO"

    if r in ["cartão de crédito", "cartao de credito", "crédito", "credito", "cartão crédito", "cartao credito"]:
        return "CARTAO_CREDITO"

    if r in ["cartão de débito", "cartao de debito", "débito", "debito", "cartão débito", "cartao debito"]:
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
# Histórico de pagamentos — MULTI-LOJA
# =========================
def listar_historico_pagamentos_df(loja_id: int):
    with conectar() as conn:
        df = pd.read_sql_query(
            """
            SELECT
                vc.id AS venda_id,
                vc.datahora,
                COALESCE(vc.total, 0) AS valor_total,
                COALESCE(vc.recebido, 0) AS valor_recebido,
                COALESCE(vc.troco, 0) AS troco,

                COALESCE(SUM(CASE
                    WHEN UPPER(vp.forma_pagamento) = 'DINHEIRO' THEN vp.valor
                    ELSE 0
                END), 0) AS valor_dinheiro,

                COALESCE(SUM(CASE
                    WHEN UPPER(vp.forma_pagamento) = 'PIX' THEN vp.valor
                    ELSE 0
                END), 0) AS valor_pix,

                COALESCE(SUM(CASE
                    WHEN UPPER(vp.forma_pagamento) IN (
                        'CARTAO', 'CARTÃO',
                        'CARTAO_CREDITO', 'CARTAO_DEBITO',
                        'CREDITO', 'CRÉDITO',
                        'DEBITO', 'DÉBITO'
                    ) THEN vp.valor
                    ELSE 0
                END), 0) AS valor_cartao

            FROM vendas_cabecalho vc
            LEFT JOIN vendas_pagamentos vp
                ON vp.venda_id = vc.id
               AND vp.loja_id = vc.loja_id
            WHERE
                vc.loja_id = ?
                AND vc.status = 'FINALIZADA'
            GROUP BY
                vc.id,
                vc.datahora,
                vc.total,
                vc.recebido,
                vc.troco
            ORDER BY vc.datahora DESC, vc.id DESC
            """,
            conn,
            params=(int(loja_id),),
        )

    if df.empty:
        return df

    def _f(v):
        try:
            return float(v or 0)
        except Exception:
            return 0.0

    def montar_resumo(row):
        partes = []

        if _f(row["valor_dinheiro"]) > 0:
            partes.append(f"DINHEIRO R$ {brl(_f(row['valor_dinheiro']))}")

        if _f(row["valor_pix"]) > 0:
            partes.append(f"PIX R$ {brl(_f(row['valor_pix']))}")

        if _f(row["valor_cartao"]) > 0:
            partes.append(f"CARTÃO R$ {brl(_f(row['valor_cartao']))}")

        return " + ".join(partes) if partes else "—"

    def classificar_pagamento(row):
        qtd_formas = 0

        if _f(row["valor_dinheiro"]) > 0:
            qtd_formas += 1
        if _f(row["valor_pix"]) > 0:
            qtd_formas += 1
        if _f(row["valor_cartao"]) > 0:
            qtd_formas += 1

        if qtd_formas > 1:
            return "MISTO"
        elif _f(row["valor_dinheiro"]) > 0:
            return "DINHEIRO"
        elif _f(row["valor_pix"]) > 0:
            return "PIX"
        elif _f(row["valor_cartao"]) > 0:
            return "CARTÃO"
        return "—"

    df["pagamentos"] = df.apply(montar_resumo, axis=1)
    df["tipo_pagamento"] = df.apply(classificar_pagamento, axis=1)

    return df


def excluir_venda_db(loja_id: int, venda_id: int, devolver_estoque: bool = True):
    loja_id = int(loja_id)
    venda_id = int(venda_id)

    with conectar() as conn:
        cur = conn.cursor()

        try:
            cur.execute("BEGIN IMMEDIATE")

            # verifica se a venda existe
            cur.execute(
                """
                SELECT id
                FROM vendas_cabecalho
                WHERE id = ? AND loja_id = ?
                """,
                (venda_id, loja_id),
            )
            venda = cur.fetchone()

            if not venda:
                raise RuntimeError("Venda não encontrada.")

            # busca itens da venda
            cur.execute(
                """
                SELECT codigo, qtd
                FROM vendas_itens
                WHERE venda_id = ? AND loja_id = ?
                """,
                (venda_id, loja_id),
            )
            itens = cur.fetchall()

            # devolve estoque
            if devolver_estoque:
                for item in itens:
                    codigo = str(item["codigo"] or "").strip()
                    qtd = int(item["qtd"] or 0)

                    if codigo and qtd > 0:
                        cur.execute(
                            """
                            UPDATE produtos
                            SET quantidade = quantidade + ?
                            WHERE loja_id = ? AND codigo = ?
                            """,
                            (qtd, loja_id, codigo),
                        )

            # apaga pagamentos
            cur.execute(
                """
                DELETE FROM vendas_pagamentos
                WHERE venda_id = ? AND loja_id = ?
                """,
                (venda_id, loja_id),
            )

            # apaga itens
            cur.execute(
                """
                DELETE FROM vendas_itens
                WHERE venda_id = ? AND loja_id = ?
                """,
                (venda_id, loja_id),
            )

            # apaga cabeçalho
            cur.execute(
                """
                DELETE FROM vendas_cabecalho
                WHERE id = ? AND loja_id = ?
                """,
                (venda_id, loja_id),
            )

            if cur.rowcount == 0:
                raise RuntimeError("Não foi possível excluir a venda.")

            conn.commit()

        except Exception:
            conn.rollback()
            raise


def listar_itens_da_venda_df(loja_id: int, venda_id: int):
    with conectar() as conn:
        df = pd.read_sql_query(
            """
            SELECT
                codigo,
                produto,
                preco_unit,
                qtd,
                total_item
            FROM vendas_itens
            WHERE loja_id = ? AND venda_id = ?
            ORDER BY id ASC
            """,
            conn,
            params=(int(loja_id), int(venda_id)),
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
        total_vendas = float(cur.fetchone()["total"] or 0.0)

        cur.execute(
            """
            SELECT COALESCE(SUM(vp.valor), 0) AS total_dinheiro
            FROM vendas_pagamentos vp
            JOIN vendas_cabecalho vc
              ON vc.id = vp.venda_id
             AND vc.loja_id = vp.loja_id
            WHERE vp.loja_id = ?
              AND vp.sessao_id = ?
              AND vc.status = 'FINALIZADA'
              AND UPPER(vp.forma_pagamento) = 'DINHEIRO'
            """,
            (int(loja_id), int(sessao_id)),
        )
        total_dinheiro = float(cur.fetchone()["total_dinheiro"] or 0.0)

        saldo_final_sistema = saldo_inicial + total_dinheiro
        return saldo_inicial, total_vendas, saldo_final_sistema, aberto_em


def relatorio_pagamentos_sessao(loja_id: int, sessao_id: int):
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT vp.forma_pagamento, COALESCE(SUM(vp.valor), 0) AS total
            FROM vendas_pagamentos vp
            JOIN vendas_cabecalho vc
              ON vc.id = vp.venda_id
             AND vc.loja_id = vp.loja_id
            WHERE vp.loja_id = ?
              AND vp.sessao_id = ?
              AND vc.status = 'FINALIZADA'
            GROUP BY vp.forma_pagamento
            ORDER BY vp.forma_pagamento
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
# (ATUALIZADO: gera cupom por loja_config)
# =========================
def registrar_venda_completa_db(
    loja_id: int,
    sessao_id: int,
    itens: list,
    pagamentos: list,
    subtotal: float,
    desconto: float,
    total: float,
    recebido: float,
    troco: float,
    status: str = "FINALIZADA",
    baixar_estoque: bool = True,
) -> tuple[int, str]:

    if not itens:
        raise RuntimeError("Carrinho vazio.")

    if not pagamentos:
        raise RuntimeError("Informe ao menos uma forma de pagamento.")

    total_pagamentos = sum(float(p.get("valor", 0)) for p in pagamentos)

    if round(total_pagamentos, 2) < round(total, 2):
        raise RuntimeError("Valor pago é menor que o total da venda.")

    total_dinheiro = sum(
        float(p.get("valor", 0))
        for p in pagamentos
        if str(p.get("forma_pagamento", "")).upper() == "DINHEIRO"
    )

    total_nao_dinheiro = total_pagamentos - total_dinheiro

    if round(total_nao_dinheiro, 2) > round(total, 2):
        raise RuntimeError("PIX/cartão não podem ultrapassar o total da venda.")

    tem_dinheiro = total_dinheiro > 0

    if not tem_dinheiro and round(total_pagamentos, 2) != round(total, 2):
        raise RuntimeError(
            "Sem dinheiro na venda, a soma dos pagamentos deve ser igual ao total."
        )

    # define forma resumo
    if len(pagamentos) == 1:
        forma_resumo = str(pagamentos[0].get("forma_pagamento", "OUTRO"))
    else:
        forma_resumo = "MISTO"

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
                    forma_resumo,
                    float(recebido or 0.0),
                    float(troco or 0.0),
                    str(status or "FINALIZADA"),
                ),
            )

            venda_id = int(cur.lastrowid)

            # =========================
            # Itens da venda
            # =========================
            for it in itens:

                codigo = str(it.get("codigo", "")).strip()
                produto = str(it.get("produto", "")).strip()
                preco_unit = float(it.get("preco_unit", 0))
                preco_custo = float(it.get("preco_custo", 0))
                qtd = int(it.get("qtd", 0))
                total_item = float(it.get("total_item", preco_unit * qtd))

                if qtd <= 0:
                    raise RuntimeError("Quantidade inválida no carrinho.")

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
                        raise RuntimeError(f"Estoque insuficiente para: {produto}")

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
                        produto,
                        preco_unit,
                        preco_custo,
                        qtd,
                        total_item,
                    ),
                )

            # =========================
            # Pagamentos da venda
            # =========================
            for p in pagamentos:

                forma = str(p.get("forma_pagamento", "OUTRO")).upper()
                valor = float(p.get("valor", 0))

                cur.execute(
                    """
                    INSERT INTO vendas_pagamentos
                    (loja_id, venda_id, sessao_id, forma_pagamento, valor, criado_em)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        int(loja_id),
                        venda_id,
                        int(sessao_id),
                        forma,
                        valor,
                        agora_iso(),
                    ),
                )

            conn.commit()

            # =========================
            # Cupom
            # =========================
            cupom = cupom_txt(
                itens=itens,
                numero_venda=str(venda_id),
                forma_ui=forma_resumo,
                desconto=float(desconto or 0.0),
                recebido=float(recebido or 0.0),
                troco=float(troco or 0.0),
                loja_id=int(loja_id),
            )

            return venda_id, cupom

        except Exception:
            conn.rollback()
            raise

# =========================
# Cupom TXT (por loja)
# =========================
def cupom_txt(
    itens: list,
    numero_venda: str,
    forma_ui: str,
    desconto: float,
    recebido: float,
    troco: float,
    loja_id: int
) -> str:
    tz_br = ZoneInfo("America/Sao_Paulo")
    agora = datetime.now(tz_br)

    # 🔹 busca config da loja
    cfg = obter_loja_config(loja_id)

    nome_fantasia = (cfg.get("nome_fantasia") or "Camargo Celulares").strip()
    razao_social  = (cfg.get("razao_social") or "").strip()
    cnpj          = (cfg.get("cnpj") or "").strip()
    ie            = (cfg.get("ie") or "").strip()
    endereco      = (cfg.get("endereco") or "").strip()
    cidade_uf     = (cfg.get("cidade_uf") or "").strip()
    telefone      = (cfg.get("telefone") or "").strip()
    mensagem      = (cfg.get("mensagem") or "OBRIGADO! VOLTE SEMPRE :)").strip()
    mostrar_nnf   = int(cfg.get("mostrar_cupom_nao_fiscal") or 0) == 1

    linhas = []

    # ===== Cabeçalho =====
    linhas.append(nome_fantasia.upper())
    if razao_social and razao_social.upper() != nome_fantasia.upper():
        linhas.append(razao_social)
    if cnpj:
        linhas.append(f"CNPJ: {cnpj}")
    if ie:
        linhas.append(f"IE: {ie}")
    if endereco:
        linhas.append(endereco)
    if cidade_uf:
        linhas.append(cidade_uf)
    if telefone:
        linhas.append(f"Tel: {telefone}")

    linhas.append("-" * 38)

    if mostrar_nnf:
        linhas.append("CUPOM NAO FISCAL")

    linhas.append(f"Venda: {numero_venda}")
    linhas.append(f"Data: {agora.strftime('%d/%m/%Y %H:%M:%S')}")
    linhas.append("-" * 38)

    # ===== Itens =====
    for it in itens:
        nome = str(it.get("produto", ""))[:28]
        qtd = int(it.get("qtd", 0) or 0)
        pu = float(it.get("preco_unit", 0.0) or 0.0)
        tt = float(it.get("total_item", pu * qtd))
        linhas.append(f"{nome}")
        linhas.append(f"  {qtd} x {brl(pu)} = {brl(tt)}")

    linhas.append("-" * 38)

    # ===== Totais =====
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
    linhas.append(mensagem)

    return "\n".join(linhas)

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

    # controle de reset da venda
if "reset_venda" not in st.session_state:
    st.session_state.reset_venda = 0

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

paginas = [
    "🧾 Caixa (PDV)",
    "📦 Estoque",
    "📈 Histórico",
    "📅 Relatórios",
]

# ✅ Painel do Proprietário (Admin e Dono)
if tipo in ("admin", "dono"):
    paginas.append("🏪 Painel do Proprietário")

# ✅ Páginas exclusivas do Admin
if tipo == "admin":
    paginas.append("👤 Usuários (Admin)")
    paginas.append("🧨 Zerar Loja (Admin)")

# Blindagem contra NameError / página inválida
if st.session_state.pagina not in paginas:
    st.session_state.pagina = "🧾 Caixa (PDV)"

pagina = st.sidebar.radio(
    "Navegação",
    paginas,
    index=paginas.index(st.session_state.pagina),
    key="pagina"
)



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
        st.sidebar.dataframe(df_rel, width="stretch", hide_index=True)

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
# ✅ Sugestões de produtos (para buscar por nome no Caixa)
# Cole este bloco ANTES do bloco "# PÁGINAS"
# =========================
def buscar_produtos_sugestoes(loja_id: int, termo: str, limit: int = 10):
    termo = (termo or "").strip()
    if not termo:
        return []

    like = f"%{termo}%"
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT codigo, nome, preco_venda, quantidade
            FROM produtos
            WHERE loja_id = ?
              AND (nome LIKE ? OR codigo LIKE ?)
            ORDER BY nome ASC
            LIMIT ?
            """,
            (int(loja_id), like, like, int(limit)),
        )
        rows = cur.fetchall()

    return [
        {
            "codigo": str(r["codigo"]),
            "nome": str(r["nome"]),
            "preco_venda": float(r["preco_venda"] or 0.0),
            "quantidade": int(r["quantidade"] or 0),
        }
        for r in rows
    ]



# =========================
# PÁGINAS
# =========================
try:
    sess = get_sessao_aberta(loja_id_ativa)
except Exception:
    sess = None




# =========================
# ✅ BUSCA PARA AUTOCOMPLETE (evita NameError)
# =========================
def buscar_produtos_sugestoes(loja_id: int, termo: str, limit: int = 10):
    termo = (termo or "").strip()
    if not termo:
        return []

    like = f"%{termo}%"
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT codigo, nome, preco_venda, quantidade
            FROM produtos
            WHERE loja_id = ?
              AND (nome LIKE ? OR codigo LIKE ?)
            ORDER BY
              CASE
                WHEN lower(nome) LIKE lower(?) THEN 0
                WHEN lower(codigo) LIKE lower(?) THEN 1
                ELSE 2
              END,
              nome ASC
            LIMIT ?
            """,
            (int(loja_id), like, like, f"{termo.lower()}%", f"{termo.lower()}%", int(limit)),
        )
        rows = cur.fetchall()

    out = []
    for r in rows:
        out.append(
            {
                "codigo": str(r["codigo"]),
                "nome": str(r["nome"]),
                "preco_venda": float(r["preco_venda"] or 0.0),
                "quantidade": int(r["quantidade"] or 0),
            }
        )
    return out


# Página: Caixa (PDV)
if pagina == "🧾 Caixa (PDV)":
    col1, col2 = st.columns([2.2, 1], gap="large")

    with col1:
        st.subheader(f"🧾 Caixa — {get_loja_nome(loja_id_ativa)}")
        st.caption("Digite para buscar no estoque (igual Google) ou bipe o código")

        if not sess:
            st.info("Abra o caixa na barra lateral para vender.")
        else:
            # ✅ token pra resetar os widgets sem mexer no session_state depois de instanciados
            if "caixa_reset" not in st.session_state:
                st.session_state.caixa_reset = 0

            reset = int(st.session_state.caixa_reset)

            # 🔎 callback do autocomplete (executa enquanto digita)
            def _search_produtos(query: str):
                q = (query or "").strip()
                if len(q) < 2:
                    return []
                sugestoes = buscar_produtos_sugestoes(loja_id_ativa, q, limit=15)
                return [
                    f"{p['nome']} — cód {p['codigo']} | Est: {p['quantidade']} | R$ {brl(p['preco_venda'])}"
                    for p in sugestoes
                ]

            # ✅ AUTOCOMPLETE (fora do form)
            escolha_label = st_searchbox(
                _search_produtos,
                key=f"caixa_autocomplete_{reset}",
                placeholder="Comece a digitar (ex: claro, vivo, capinha...)",
            )

            # ✅ FORM: Enter no código vai disparar o submit
            with st.form(key=f"form_add_item_{reset}", clear_on_submit=False):
                codigo_bipe = st.text_input(
                    "Código (opcional — leitor)",
                    placeholder="Bipe o código aqui e aperte Enter",
                    key=f"caixa_codigo_bipe_{reset}",
                )

                qtd = st.number_input(
                    "Quantidade",
                    min_value=1,
                    step=1,
                    value=1,
                    key=f"caixa_qtd_{reset}",
                )

                add = st.form_submit_button("Adicionar")

            if add:
                codigo_final = ""

                # prioridade: código bipado
                if (codigo_bipe or "").strip():
                    codigo_final = (codigo_bipe or "").strip()

                # senão, usa o selecionado no autocomplete
                elif escolha_label:
                    try:
                        codigo_final = (
                            escolha_label.split("— cód", 1)[1]
                            .split("|", 1)[0]
                            .strip()
                        )
                    except Exception:
                        codigo_final = ""

                if not codigo_final:
                    st.warning("Digite e selecione um produto, ou bipe um código.")
                else:
                    prod = buscar_produto_por_codigo(loja_id_ativa, codigo_final)
                    if not prod:
                        st.error("Produto não encontrado (nesta loja).")
                    else:
                        qtd_int = int(qtd)
                        if qtd_int > int(prod.get("quantidade", 0)):
                            st.error("Quantidade excede o estoque disponível.")
                        else:
                            st.session_state.cart.append(
                                {
                                    "codigo": str(prod["codigo"]),
                                    "produto": str(prod["nome"]),
                                    "preco_unit": float(prod["preco_venda"]),
                                    "preco_custo": float(prod.get("preco_custo", 0.0)),
                                    "qtd": int(qtd_int),
                                    "total_item": float(prod["preco_venda"]) * int(qtd_int),
                                }
                            )
                            st.success("Item adicionado!")

                            # ✅ força recriação dos widgets (limpa campos)
                            st.session_state.caixa_reset = reset + 1
                            st.rerun()

        st.subheader("Carrinho (editável)")

        if st.session_state.cart:
            df_cart = pd.DataFrame(st.session_state.cart)
            for col in ["codigo", "produto", "preco_unit", "qtd", "preco_custo", "total_item"]:
                if col not in df_cart.columns:
                    df_cart[col] = 0

            df_edit = df_cart[["codigo", "produto", "preco_unit", "qtd"]].copy()

            edited = st.data_editor(
    df_edit,
    width="stretch",
    hide_index=True,
    num_rows="fixed",
    disabled=["codigo", "produto"],
    column_config={
        "preco_unit": st.column_config.NumberColumn(
            "Preço unit.",
            min_value=0.0,
            step=0.01,
            format="%.2f",
        ),
        "qtd": st.column_config.NumberColumn(
            "Qtd",
            min_value=1,
            step=1
        ),
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
            sid = int(_sess_get(sess, "id", 0) or 0)

            # =========================
            # Desconto (corrigido)
            # =========================
            df_cart = pd.DataFrame(st.session_state.cart) if st.session_state.cart else pd.DataFrame()

            subtotal = (
            float(df_cart["total_item"].sum())
            if (not df_cart.empty and "total_item" in df_cart.columns)
            else 0.0
            )

            desconto = st.number_input(
            "Desconto (R$)",
            min_value=0.0,
            step=0.01,
            format="%.2f",
            key=f"desconto_{st.session_state.reset_venda}"
            )

            # garante que o desconto nunca passe do subtotal
            desconto = max(0.0, min(desconto, subtotal))

            total_liq = max(0.0, subtotal - desconto)

            st.markdown("### Pagamentos")

            dinheiro_txt = st.text_input(
             "Dinheiro (R$)",
             key=f"dinheiro_{st.session_state.reset_venda}"
            )

            pix_txt = st.text_input(
             "PIX (R$)",
              key=f"pix_{st.session_state.reset_venda}"
            )

            credito_txt = st.text_input(
             "Cartão Crédito (R$)",
             key=f"credito_{st.session_state.reset_venda}"
            )

            debito_txt = st.text_input(
             "Cartão Débito (R$)",
              key=f"debito_{st.session_state.reset_venda}"
            )

            recebido_dinheiro_txt = st.text_input(
              "Recebido em dinheiro (para troco)",
               key=f"recebido_{st.session_state.reset_venda}"
            )

            valor_dinheiro = max(0.0, to_float(dinheiro_txt))
            valor_pix = max(0.0, to_float(pix_txt))
            valor_credito = max(0.0, to_float(credito_txt))
            valor_debito = max(0.0, to_float(debito_txt))
            recebido_dinheiro = max(0.0, to_float(recebido_dinheiro_txt))

            pagamentos = []
            if valor_dinheiro > 0:
                pagamentos.append({"forma_pagamento": "DINHEIRO", "valor": valor_dinheiro})
            if valor_pix > 0:
                pagamentos.append({"forma_pagamento": "PIX", "valor": valor_pix})
            if valor_credito > 0:
                pagamentos.append({"forma_pagamento": "CARTAO_CREDITO", "valor": valor_credito})
            if valor_debito > 0:
                pagamentos.append({"forma_pagamento": "CARTAO_DEBITO", "valor": valor_debito})

            total_pago = valor_dinheiro + valor_pix + valor_credito + valor_debito
            falta = max(0.0, total_liq - total_pago)

            if valor_dinheiro > 0:
                base_troco = recebido_dinheiro if recebido_dinheiro > 0 else valor_dinheiro
                troco = max(0.0, base_troco - valor_dinheiro)
                recebido = base_troco + valor_pix + valor_credito + valor_debito
            else:
                troco = 0.0
                recebido = total_pago

            st.write(f"Total: **R$ {brl(subtotal)}**")
            st.write(f"Desconto: **R$ {brl(desconto)}**")
            st.write(f"Total a pagar: **R$ {brl(total_liq)}**")
            st.write(f"Total informado nos pagamentos: **R$ {brl(total_pago)}**")
            if falta > 0:
                st.error(f"Falta pagar: R$ {brl(falta)}")
            else:
                st.success("Pagamento completo.")
            if valor_dinheiro > 0:
                st.write(f"Troco: **R$ {brl(troco)}**")

            # ✅ estado de confirmação (uma vez só)
            if "confirmar_venda" not in st.session_state:
                st.session_state.confirmar_venda = False

            # 1) Primeiro clique: pedir confirmação (sem gravar no banco)
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

                if not pagamentos:
                    st.error("Informe ao menos uma forma de pagamento.")
                    st.stop()

                if total_pago < total_liq:
                    st.error("O total dos pagamentos está menor que o total da venda.")
                    st.stop()

                if valor_dinheiro > 0 and recebido_dinheiro < valor_dinheiro:
                    st.error("O valor recebido em dinheiro não pode ser menor que o valor pago em dinheiro.")
                    st.stop()

                # ✅ entrou em modo confirmação
                st.session_state.confirmar_venda = True
                st.rerun()

            # 2) Modo confirmação: mostra resumo + CONFIRMAR/CANCELAR
            if st.session_state.confirmar_venda:
                st.warning("⚠️ Confirme os dados antes de finalizar (gravar no sistema)")

                st.markdown("### Revisão da venda")
                st.write(f"Loja: **{get_loja_nome(loja_id_ativa)}**")
                st.write(f"Itens: **{len(st.session_state.cart)}**")
                st.write(f"Subtotal: **R$ {brl(subtotal)}**")
                st.write(f"Desconto: **R$ {brl(desconto)}**")
                st.write(f"Total a pagar: **R$ {brl(total_liq)}**")

                if pagamentos:
                    st.write("**Pagamentos:**")
                    for p in pagamentos:
                        st.write(f"- {p['forma_pagamento']}: R$ {brl(p['valor'])}")

                st.write(f"Total informado: **R$ {brl(total_pago)}**")
                if valor_dinheiro > 0:
                    st.write(f"Recebido em dinheiro: **R$ {brl(recebido_dinheiro)}**")
                    st.write(f"Troco: **R$ {brl(troco)}**")

                b1, b2 = st.columns(2)

                with b1:
                    if st.button("✅ CONFIRMAR VENDA", key="btn_confirmar_venda"):
                        try:
                            venda_id, txt = registrar_venda_completa_db(
                                loja_id=loja_id_ativa,
                                sessao_id=int(sid),
                                itens=st.session_state.cart,
                                pagamentos=pagamentos,
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

                        st.session_state.cupom_txt = txt
                        st.session_state.cupom_nome = f"cupom_{numero_venda}.txt"
                        st.session_state.cupom_id = venda_id

                        st.session_state.cart = []
                        st.session_state.confirmar_venda = False

                        # 💣 ESSA LINHA É A CHAVE
                        st.session_state.reset_venda += 1

                        st.rerun()


                with b2:
                    if st.button("❌ CANCELAR", key="btn_cancelar_venda"):
                        st.session_state.confirmar_venda = False
                        st.info("Venda cancelada. Você pode ajustar e finalizar novamente.")
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
        st.dataframe(df_show, width="stretch", hide_index=True)



        # ✅ NOVO: EDITAR PRODUTO EXISTENTE
        # =========================
        st.divider()
        st.markdown("### ✏️ Editar produto já cadastrado")

        df_edit = df.copy()

        filtro = st.text_input("Buscar para editar (código ou nome)", value="", key="edit_filtro")
        f = (filtro or "").strip().lower()

        # ✅ reset do selectbox quando o filtro muda (evita “puxar outro produto”)
        if "edit_reset" not in st.session_state:
            st.session_state.edit_reset = 0
        if "edit_last_filter" not in st.session_state:
            st.session_state.edit_last_filter = ""

        if f != st.session_state.edit_last_filter:
            st.session_state.edit_last_filter = f
            st.session_state.edit_reset += 1

        if f:
            cod_series = df_edit["codigo"].astype(str).str.lower()
            nome_series = df_edit["nome"].astype(str).str.lower()

            # ✅ Se parece código (só dígitos), tenta match exato primeiro
            if f.isdigit():
                exato = df_edit[cod_series == f]
                if not exato.empty:
                    df_edit = exato
                else:
                    # fallback: começa com (melhor que contains)
                    df_edit = df_edit[
                        cod_series.str.startswith(f) |
                        nome_series.str.contains(f, na=False)
                    ]
            else:
                # texto normal: busca no nome e no código (contains)
                df_edit = df_edit[
                    cod_series.str.contains(f, na=False) |
                    nome_series.str.contains(f, na=False)
                ]

        if df_edit.empty:
            st.info("Nenhum produto encontrado com esse filtro.")
        else:
            df_edit = df_edit.copy()  # evita SettingWithCopyWarning

            df_edit["__opt__"] = df_edit.apply(
                lambda r: f"{r['codigo']} — {r['nome']} (Qtd: {int(r['quantidade'])})",
                axis=1
            )

            opt = st.selectbox(
                "Selecione o produto",
                df_edit["__opt__"].tolist(),
                key=f"edit_select_{st.session_state.edit_reset}",
            )

            row = df_edit[df_edit["__opt__"] == opt].iloc[0]

            codigo_sel = str(row["codigo"])
            nome_sel = str(row["nome"])
            custo_sel = float(row["preco_custo"])
            venda_sel = float(row["preco_venda"])
            qtd_sel = int(row["quantidade"])

            with st.form("form_editar_produto_existente", clear_on_submit=False):
                c1, c2 = st.columns(2, gap="large")

                with c1:
                    st.caption("Dados do produto")
                    codigo_novo = st.text_input("Código", value=codigo_sel)
                    nome_novo = st.text_input("Nome", value=nome_sel)
                    custo_novo = st.number_input("Preço de custo", min_value=0.0, value=custo_sel, step=0.01)
                    venda_novo = st.number_input("Preço de venda", min_value=0.0, value=venda_sel, step=0.01)

                with c2:
                    st.caption("Quantidade em estoque")
                    modo_qtd = st.radio(
                        "Como ajustar a quantidade?",
                        ["Definir quantidade exata", "Somar (+) / Subtrair (-)"],
                        index=0,
                        key="modo_qtd"
                    )

                    if modo_qtd == "Definir quantidade exata":
                        qtd_final = st.number_input("Quantidade final", min_value=0, value=qtd_sel, step=1)
                    else:
                        delta = st.number_input("Alteração (use negativo para saída)", value=0, step=1)
                        qtd_final = max(0, qtd_sel + int(delta))
                        st.info(f"Quantidade atual: {qtd_sel} → Quantidade final: {qtd_final}")

                salvar_edit = st.form_submit_button("💾 Salvar alterações")

            if salvar_edit:
                try:
                    if not codigo_novo.strip():
                        st.warning("Código não pode ficar vazio.")
                    elif not nome_novo.strip():
                        st.warning("Nome não pode ficar vazio.")
                    else:
                        upsert_produto(
                            loja_id_ativa,
                            codigo_novo.strip(),
                            nome_novo.strip(),
                            float(custo_novo),
                            float(venda_novo),
                            int(qtd_final)
                        )
                        st.success("Produto atualizado ✅")
                        st.rerun()
                except Exception as e:
                    st.error(str(e))

                    st.error(str(e))# Página: Histórico
elif pagina == "📈 Histórico":
    st.subheader(f"📈 Histórico — {get_loja_nome(loja_id_ativa)}")

    aba1, aba2 = st.tabs(["🛒 Itens vendidos", "💳 Pagamentos"])

    # ==========================================================
    # ABA 1 - ITENS VENDIDOS
    # ==========================================================
    with aba1:
        col1, col2 = st.columns([2, 1])

        with col1:
            filtro = st.text_input(
                "Filtrar por produto (contém)",
                value="",
                key="hist_filtro_produto"
            )

        with col2:
            ordenar_por = st.selectbox(
                "Ordenar itens por",
                ["Mais recentes", "Maior valor total", "Produto (A-Z)"],
                key="hist_itens_ordem"
            )

        df = listar_vendas_itens_df(loja_id_ativa, filtro_produto=filtro)

        if df.empty:
            st.info("Sem vendas (ou filtro sem resultados) nesta loja.")
        else:
            if ordenar_por == "Maior valor total" and "total_item" in df.columns:
                df = df.sort_values(by="total_item", ascending=False)
            elif ordenar_por == "Produto (A-Z)" and "produto" in df.columns:
                df = df.sort_values(by="produto", ascending=True)
            elif "datahora" in df.columns:
                df = df.sort_values(by="datahora", ascending=False)

            total = float(df["total_item"].sum()) if "total_item" in df.columns else 0.0
            qtd_itens = int(df["qtd"].sum()) if "qtd" in df.columns else 0
            qtd_registros = len(df)

            c1, c2, c3 = st.columns(3)
            c1.metric("Total vendido", f"R$ {brl(total)}")
            c2.metric("Qtd. de itens", qtd_itens)
            c3.metric("Registros", qtd_registros)

            df_show = df.copy()

            colunas_preferidas = [
                "datahora",
                "venda_id",
                "codigo",
                "produto",
                "preco_unit",
                "qtd",
                "total_item",
            ]

            colunas_exibir = [c for c in colunas_preferidas if c in df_show.columns]
            df_show = df_show[colunas_exibir].copy()

            if "preco_unit" in df_show.columns:
                df_show["preco_unit"] = df_show["preco_unit"].map(
                    lambda x: f"R$ {brl(float(x or 0))}"
                )

            if "total_item" in df_show.columns:
                df_show["total_item"] = df_show["total_item"].map(
                    lambda x: f"R$ {brl(float(x or 0))}"
                )

            st.dataframe(df_show, use_container_width=True, hide_index=True)

    # ==========================================================
    # ABA 2 - PAGAMENTOS
    # ==========================================================
    with aba2:
        col1, col2 = st.columns([2, 1])

        with col1:
            filtro_pag = st.selectbox(
                "Filtrar por forma de pagamento",
                ["TODOS", "DINHEIRO", "PIX", "CARTÃO", "MISTO"],
                key="hist_pag_filtro_forma"
            )

        with col2:
            ordenar_pag = st.selectbox(
                "Ordenar pagamentos por",
                ["Mais recentes", "Maior valor", "Menor valor"],
                key="hist_pag_ordem"
            )

        df_pag = listar_historico_pagamentos_df(loja_id_ativa)

        if df_pag.empty:
            st.info("Nenhum pagamento encontrado nesta loja.")
        else:
            if filtro_pag != "TODOS":
                df_pag = df_pag[df_pag["tipo_pagamento"] == filtro_pag]

            if ordenar_pag == "Maior valor" and "valor_total" in df_pag.columns:
                df_pag = df_pag.sort_values(by="valor_total", ascending=False)
            elif ordenar_pag == "Menor valor" and "valor_total" in df_pag.columns:
                df_pag = df_pag.sort_values(by="valor_total", ascending=True)
            elif "datahora" in df_pag.columns:
                df_pag = df_pag.sort_values(by="datahora", ascending=False)

            if df_pag.empty:
                st.info("Nenhum pagamento encontrado com esse filtro.")
            else:
                total_vendas = float(df_pag["valor_total"].sum())
                total_dinheiro = float(df_pag["valor_dinheiro"].sum())
                total_pix = float(df_pag["valor_pix"].sum())
                total_cartao = float(df_pag["valor_cartao"].sum())
                qtd_vendas = len(df_pag)

                c1, c2, c3, c4, c5 = st.columns(5)
                c1.metric("Vendas", qtd_vendas)
                c2.metric("Total vendido", f"R$ {brl(total_vendas)}")
                c3.metric("Dinheiro", f"R$ {brl(total_dinheiro)}")
                c4.metric("PIX", f"R$ {brl(total_pix)}")
                c5.metric("Cartão", f"R$ {brl(total_cartao)}")

                df_show = df_pag.copy()

                colunas_preferidas = [
                    "datahora",
                    "venda_id",
                    "valor_total",
                    "pagamentos",
                    "troco",
                    "tipo_pagamento",
                ]

                colunas_exibir = [c for c in colunas_preferidas if c in df_show.columns]
                df_show = df_show[colunas_exibir].copy()

                if "valor_total" in df_show.columns:
                    df_show["valor_total"] = df_show["valor_total"].map(
                        lambda x: f"R$ {brl(float(x or 0))}"
                    )

                if "troco" in df_show.columns:
                    df_show["troco"] = df_show["troco"].map(
                        lambda x: f"R$ {brl(float(x or 0))}"
                    )

                st.dataframe(df_show, use_container_width=True, hide_index=True)
 
                st.divider()
                st.subheader("📄 Detalhes da venda")

                venda_detalhe = st.selectbox(
                    "Selecione a venda para ver os itens",
                    df_pag["venda_id"].astype(int).tolist(),
                    key="hist_detalhe_venda"
                )

                df_itens_venda = listar_itens_da_venda_df(
                    loja_id=loja_id_ativa,
                    venda_id=int(venda_detalhe)
                )

                if df_itens_venda.empty:
                    st.info("Nenhum item encontrado para esta venda.")
                else:
                    df_itens_show = df_itens_venda.copy()

                    if "preco_unit" in df_itens_show.columns:
                        df_itens_show["preco_unit"] = df_itens_show["preco_unit"].map(
                            lambda x: f"R$ {brl(float(x or 0))}"
                        )

                    if "total_item" in df_itens_show.columns:
                        df_itens_show["total_item"] = df_itens_show["total_item"].map(
                            lambda x: f"R$ {brl(float(x or 0))}"
                        )

                    st.dataframe(df_itens_show, use_container_width=True, hide_index=True)

                st.divider()
                st.subheader("🗑️ Excluir venda")

                venda_excluir = st.selectbox(
                    "Selecione a venda para excluir",
                    df_pag["venda_id"].astype(int).tolist(),
                    key="hist_excluir_venda"
                )

                devolver_estoque = st.checkbox(
                    "Devolver itens ao estoque",
                    value=True,
                    key="hist_devolver_estoque"
                )

                confirmar_exclusao = st.checkbox(
                    f"Confirmo a exclusão da venda #{venda_excluir}",
                    key="hist_confirmar_exclusao"
                )

                if st.button("Excluir venda selecionada", type="primary"):
                    if not confirmar_exclusao:
                        st.warning("Marque a confirmação antes de excluir.")
                    else:
                        try:
                            excluir_venda_db(
                                loja_id=loja_id_ativa,
                                venda_id=int(venda_excluir),
                                devolver_estoque=devolver_estoque
                            )
                            st.success(f"Venda #{venda_excluir} excluída com sucesso.")
                            st.rerun()
                        except Exception as e:
                            st.error(str(e))

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
        st.dataframe(dfp, width="stretch", hide_index=True)

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
        st.dataframe(dfd_show, width="stretch", hide_index=True)

# Página: Usuários (Admin)
elif pagina == "👤 Usuários (Admin)":
    if tipo != "admin":
        st.error("Acesso negado. Apenas ADMIN pode acessar Usuários.")
        st.stop()

    st.subheader("👤 Usuários (Admin)")
    st.dataframe(listar_usuarios_df(), width="stretch", hide_index=True)

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

# Página: Zerar Loja (Admin)
elif pagina == "🧨 Zerar Loja (Admin)":
    if tipo != "admin":
        st.error("Acesso negado. Apenas ADMIN pode acessar.")
        st.stop()

    st.subheader("🧨 Zerar Loja (Admin)")
    st.warning("⚠️ Isso APAGA tudo da loja selecionada: ESTOQUE + VENDAS + CAIXA.")
    st.write(f"Loja selecionada: **{get_loja_nome(loja_id_ativa)} (ID {loja_id_ativa})**")

    criar_backup = st.checkbox("Criar backup antes de zerar (recomendado)", value=True)
    confirm = st.text_input("Digite exatamente: ZERAR", value="")

    if st.button("🧨 ZERAR AGORA", type="primary"):
        if confirm.strip().upper() != "ZERAR":
            st.error("Confirmação incorreta. Digite ZERAR.")
            st.stop()

        if criar_backup:
            try:
                criar_backup_agora(prefix=f"pdv_before_reset_loja{int(loja_id_ativa)}")
            except Exception:
                pass

        try:
            res = zerar_loja_db(int(loja_id_ativa))
        except Exception as e:
            st.error(str(e))
            st.stop()

        # limpa estados locais
        st.session_state.cart = []
        st.session_state.cupom_txt = None
        st.session_state.cupom_nome = None
        st.session_state.cupom_id = None

        st.success("✅ Loja zerada com sucesso!")
        st.json(res)
        st.rerun()

# Página: Zerar Loja (Admin)
elif pagina == "🧨 Zerar Loja (Admin)":
    ...
    st.rerun()


# =========================
# Página: Painel do Proprietário
# =========================
elif pagina == "🏪 Painel do Proprietário":

    st.subheader(f"🏪 Painel do Proprietário — {get_loja_nome(loja_id_ativa)}")
    st.caption("Configura os dados que aparecem no cupom desta loja.")

    if tipo not in ("admin", "dono"):
        st.error("Acesso negado.")
        st.stop()

    cfg = get_loja_config(loja_id_ativa) or {}

    def _v(k, d=""):
        return cfg.get(k) if cfg.get(k) is not None else d

    with st.form("form_loja_config"):

        nome_fantasia = st.text_input("Nome Fantasia", value=_v("nome_fantasia", get_loja_nome(loja_id_ativa)))
        razao_social  = st.text_input("Razão Social", value=_v("razao_social", ""))
        cnpj          = st.text_input("CNPJ", value=_v("cnpj", ""))
        ie            = st.text_input("IE", value=_v("ie", "ISENTO"))

        telefone = st.text_input("Telefone", value=_v("telefone", ""))
        endereco = st.text_input("Endereço", value=_v("endereco", ""))
        cidade_uf = st.text_input("Cidade/UF", value=_v("cidade_uf", ""))

        mensagem = st.text_area(
            "Mensagem no rodapé do cupom",
            value=_v("mensagem", "OBRIGADO! VOLTE SEMPRE :)"),
            height=120
        )

        salvar = st.form_submit_button("💾 Salvar")

    if salvar:
        upsert_loja_config(loja_id_ativa, {
            "nome_fantasia": nome_fantasia,
            "razao_social": razao_social,
            "cnpj": cnpj,
            "ie": ie,
            "telefone": telefone,
            "endereco": endereco,
            "cidade_uf": cidade_uf,
            "mensagem": mensagem,
        })

        st.success("Configurações salvas com sucesso!")
        st.rerun()
