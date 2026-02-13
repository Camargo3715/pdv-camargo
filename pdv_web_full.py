# pdv_web_full.py
# PDV Camargo Celulares — Web (Streamlit) | Completo: Caixa + Estoque + Histórico + Relatórios + Login
# Compatível com o schema do seu pdv.py (produtos, caixa_sessoes, vendas_cabecalho, vendas_itens)

import os
import sqlite3
import secrets
import hashlib
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
    # Mesma estrutura do pdv.py
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

        # (LEGADO) mantém por compatibilidade
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
    Cria a tabela usuarios e, se não existir nenhum usuário,
    cria um ADMIN inicial usando ADMIN_USER/ADMIN_PASS (Render env),
    ou padrão admin/admin123.
    """
    with conectar() as conn:
        cur = conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS usuarios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            nome TEXT,
            role TEXT NOT NULL CHECK(role IN ('ADMIN','OPERADOR')),
            pass_salt TEXT NOT NULL,
            pass_hash TEXT NOT NULL,
            ativo INTEGER NOT NULL DEFAULT 1,
            criado_em TEXT NOT NULL
        )
        """)
        conn.commit()

        cur.execute("SELECT COUNT(*) FROM usuarios")
        total = int(cur.fetchone()[0] or 0)

        if total == 0:
            admin_user = (os.getenv("ADMIN_USER") or "admin").strip().lower()
            admin_pass = (os.getenv("ADMIN_PASS") or "admin123").strip()

            salt, ph = gerar_hash_senha(admin_pass)
            cur.execute(
                """
                INSERT INTO usuarios (username, nome, role, pass_salt, pass_hash, ativo, criado_em)
                VALUES (?, ?, 'ADMIN', ?, ?, 1, ?)
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
            SELECT id, username, nome, role, pass_salt, pass_hash, ativo
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
        "salt": str(row[4]),
        "hash": str(row[5]),
        "ativo": int(row[6] or 0),
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
            SELECT username, nome, role, ativo, criado_em
            FROM usuarios
            ORDER BY role DESC, username ASC
            """,
            conn,
        )


def criar_usuario(username: str, nome: str, role: str, senha: str, ativo: int = 1):
    username = (username or "").strip().lower()
    nome = (nome or "").strip()
    role = (role or "").strip().upper()

    if role not in ("ADMIN", "OPERADOR"):
        raise ValueError("Perfil inválido. Use ADMIN ou OPERADOR.")
    if not username:
        raise ValueError("Username é obrigatório.")
    if len((senha or "").strip()) < 4:
        raise ValueError("Senha muito curta (mín. 4).")

    salt, ph = gerar_hash_senha(senha)
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO usuarios (username, nome, role, pass_salt, pass_hash, ativo, criado_em)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (username, nome, role, salt, ph, int(1 if ativo else 0), agora_iso()),
        )
        conn.commit()


def atualizar_usuario_role_ativo(username: str, role: str, ativo: int):
    username = (username or "").strip().lower()
    role = (role or "").strip().upper()
    if role not in ("ADMIN", "OPERADOR"):
        raise ValueError("Perfil inválido.")
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE usuarios
            SET role = ?, ativo = ?
            WHERE username = ?
            """,
            (role, int(1 if ativo else 0), username),
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
    return float(s)


def brl(v: float) -> str:
    return f"{float(v or 0.0):.2f}".replace(".", ",")


def agora_iso():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


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
# Produtos (Estoque)
# =========================
def listar_produtos_df():
    with conectar() as conn:
        df = pd.read_sql_query(
            """
            SELECT codigo, nome, preco_custo, preco_venda, quantidade
            FROM produtos
            ORDER BY nome
            """,
            conn,
        )
    return df


def buscar_produto_por_codigo(codigo: str):
    codigo = (codigo or "").strip()
    if not codigo:
        return None
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT codigo, nome, preco_custo, preco_venda, quantidade
            FROM produtos
            WHERE codigo = ?
            """,
            (codigo,),
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


def upsert_produto(codigo: str, nome: str, preco_custo: float, preco_venda: float, quantidade: int):
    codigo = (codigo or "").strip()
    nome = (nome or "").strip()
    if not codigo or not nome:
        raise ValueError("Código e nome são obrigatórios.")
    if preco_venda <= 0:
        raise ValueError("Preço de venda deve ser > 0.")
    if preco_custo < 0:
        raise ValueError("Preço de custo deve ser >= 0.")
    if quantidade < 0:
        raise ValueError("Quantidade deve ser >= 0.")

    with conectar() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO produtos (codigo, nome, preco_custo, preco_venda, quantidade)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(codigo) DO UPDATE SET
                nome=excluded.nome,
                preco_custo=excluded.preco_custo,
                preco_venda=excluded.preco_venda,
                quantidade=excluded.quantidade
            """,
            (codigo, nome, float(preco_custo), float(preco_venda), int(quantidade)),
        )
        conn.commit()


def excluir_produto(codigo: str):
    codigo = (codigo or "").strip()
    if not codigo:
        return
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM produtos WHERE codigo = ?", (codigo,))
        conn.commit()


def baixar_estoque_por_codigo(codigo: str, qtd: int):
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE produtos
            SET quantidade = quantidade - ?
            WHERE codigo = ? AND quantidade >= ?
            """,
            (int(qtd), str(codigo), int(qtd)),
        )
        if cur.rowcount == 0:
            raise RuntimeError("Estoque insuficiente ou produto não encontrado.")
        conn.commit()


# =========================
# Caixa (Abertura/Fechamento)
# =========================
def get_sessao_aberta():
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, aberto_em, saldo_inicial, operador
            FROM caixa_sessoes
            WHERE status='ABERTO'
            ORDER BY id DESC
            LIMIT 1
            """
        )
        return cur.fetchone()


def abrir_caixa_db(saldo_inicial: float, operador: str = "", obs: str = "") -> int:
    atual = get_sessao_aberta()
    if atual:
        raise RuntimeError(f"Já existe um caixa ABERTO (Sessão #{atual[0]}). Feche antes de abrir outro.")
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO caixa_sessoes (aberto_em, status, saldo_inicial, operador, obs_abertura)
            VALUES (?, 'ABERTO', ?, ?, ?)
            """,
            (agora_iso(), float(saldo_inicial or 0.0), (operador or "").strip(), (obs or "").strip()),
        )
        conn.commit()
        return int(cur.lastrowid)


def totais_sessao(sessao_id: int):
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COALESCE(SUM(total), 0)
            FROM vendas_cabecalho
            WHERE sessao_id = ? AND status='FINALIZADA'
            """,
            (int(sessao_id),),
        )
        total_vendas = float(cur.fetchone()[0] or 0.0)

        cur.execute(
            """
            SELECT saldo_inicial, aberto_em
            FROM caixa_sessoes
            WHERE id = ?
            """,
            (int(sessao_id),),
        )
        row = cur.fetchone()
        saldo_inicial = float(row[0] or 0.0) if row else 0.0
        aberto_em = row[1] if row else ""
        saldo_final_sistema = saldo_inicial + total_vendas
        return saldo_inicial, total_vendas, saldo_final_sistema, aberto_em


def relatorio_pagamentos_sessao(sessao_id: int):
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT forma_pagamento, COALESCE(SUM(total), 0)
            FROM vendas_cabecalho
            WHERE sessao_id = ? AND status='FINALIZADA'
            GROUP BY forma_pagamento
            ORDER BY forma_pagamento
            """,
            (int(sessao_id),),
        )
        return [(str(fp), float(t or 0.0)) for fp, t in cur.fetchall()]


def fechar_caixa_db(sessao_id: int, saldo_informado: float, obs: str = ""):
    saldo_inicial, total_vendas, saldo_final_sistema, _ = totais_sessao(sessao_id)
    saldo_informado = float(saldo_informado or 0.0)
    diferenca = saldo_informado - saldo_final_sistema
    agora = agora_iso()

    with conectar() as conn:
        cur = conn.cursor()
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
            WHERE id = ? AND status='ABERTO'
            """,
            (agora, float(saldo_final_sistema), float(saldo_informado), float(diferenca),
             (obs or "").strip(), int(sessao_id)),
        )
        if cur.rowcount == 0:
            raise RuntimeError("Não foi possível fechar: sessão não está ABERTA (ou não existe).")
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
# Vendas
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
    with conectar() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO vendas_cabecalho
                (datahora, sessao_id, subtotal, desconto, total, forma_pagamento, recebido, troco, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                agora_iso(),
                int(sessao_id),
                float(subtotal),
                float(desconto),
                float(total),
                str(forma_pagamento),
                float(recebido),
                float(troco),
                str(status),
            ),
        )
        venda_id = int(cur.lastrowid)

        for it in itens:
            cur.execute(
                """
                INSERT INTO vendas_itens
                    (venda_id, codigo, produto, preco_unit, preco_custo, qtd, total_item)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    venda_id,
                    str(it.get("codigo") or ""),
                    str(it.get("produto") or ""),
                    float(it.get("preco_unit") or 0.0),
                    float(it.get("preco_custo") or 0.0),
                    int(it.get("qtd") or 0),
                    float(it.get("total_item") or 0.0),
                ),
            )

        conn.commit()
        return venda_id


def listar_vendas_itens_df(filtro_produto: str = ""):
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
            WHERE c.status='FINALIZADA'
            ORDER BY c.id DESC, i.id ASC
            """,
            conn,
        )
    if filtro and not df.empty:
        df = df[df["produto"].astype(str).str.lower().str.contains(filtro, na=False)]
    return df


def listar_vendas_por_periodo_df(data_ini: datetime, data_fim: datetime):
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
            LEFT JOIN vendas_itens i ON i.venda_id = c.id
            WHERE c.status='FINALIZADA'
              AND datetime(c.datahora) BETWEEN datetime(?) AND datetime(?)
            GROUP BY c.id, c.datahora, c.total
            ORDER BY datetime(c.datahora) DESC
            """,
            conn,
            params=(ini, fim),
        )
    return df


def totais_por_dia_do_mes(ano: int, mes: int):
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
            WHERE status='FINALIZADA'
              AND datetime(datahora) BETWEEN datetime(?) AND datetime(?)
            GROUP BY date(datahora)
            ORDER BY date(datahora)
            """,
            conn,
            params=(ini, fim),
        )
    total_mes = float(df["total"].sum()) if not df.empty else 0.0
    return df, total_mes


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
# App (UI)
# =========================
st.set_page_config(page_title=APP_TITLE, layout="wide")

# inicializa tabelas
inicializar_banco()
inicializar_usuarios()

if "cart" not in st.session_state:
    st.session_state.cart = []

st.title(APP_TITLE)
st.caption(f"DB: {DB_PATH}")

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
            }
            st.rerun()

    st.info("Faça login para usar o sistema.")
    st.stop()
else:
    st.sidebar.success(f"Logado: {auth['username']} ({auth['role']})")
    if st.sidebar.button("Sair"):
        st.session_state.auth = None
        st.rerun()

# Navegação por perfil
role = st.session_state.auth["role"]

paginas = ["🧾 Caixa (PDV)", "📈 Histórico"]
if role == "ADMIN":
    paginas.insert(1, "📦 Estoque")
    paginas.append("📅 Relatórios")
    paginas.append("👤 Usuários (Admin)")

pagina = st.sidebar.radio("Navegação", paginas, index=0)

# =========================
# Caixa (sidebar abrir/fechar sempre visível)
# =========================
st.sidebar.divider()
st.sidebar.header("Caixa (Abertura/Fechamento)")
sess = get_sessao_aberta()

if not sess:
    st.sidebar.error("CAIXA FECHADO")
    with st.sidebar.form("abrir_caixa"):
        operador = st.text_input("Operador (opcional)", value=auth.get("username", ""))
        saldo_ini = st.number_input("Saldo inicial (fundo)", min_value=0.0, step=10.0, format="%.2f")
        obs = st.text_input("Observação (opcional)", value="")
        ok = st.form_submit_button("🔓 Abrir Caixa")
    if ok:
        try:
            sid = abrir_caixa_db(saldo_ini, operador, obs)
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

    saldo_inicial, total_vendas, saldo_final_sistema, _ = totais_sessao(int(sid))
    st.sidebar.write(f"Vendas (sistema): **R$ {brl(total_vendas)}**")
    st.sidebar.write(f"Final (sistema): **R$ {brl(saldo_final_sistema)}**")

    rel = relatorio_pagamentos_sessao(int(sid))
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
            res = fechar_caixa_db(int(sid), float(contado), obs_f)
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
        st.subheader("Lançar item (por código)")

        if not sess:
            st.info("Abra o caixa na barra lateral para vender.")
        else:
            with st.form("add_item", clear_on_submit=True):
                codigo = st.text_input("Código", placeholder="Bipe o código / digite e Enter")
                qtd = st.number_input("Quantidade", min_value=1, step=1, value=1)
                add = st.form_submit_button("Adicionar")
            if add:
                prod = buscar_produto_por_codigo(codigo)
                if not prod:
                    st.error("Produto não encontrado pelo código.")
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

            try:
                desconto = to_float(desconto_txt)
            except Exception:
                desconto = 0.0
            total_liq = max(0.0, subtotal - float(desconto))

            try:
                recebido = to_float(recebido_txt) if forma_ui == "Dinheiro" else 0.0
            except Exception:
                recebido = 0.0

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
                    # valida estoque
                    for it in st.session_state.cart:
                        prod = buscar_produto_por_codigo(it["codigo"])
                        if not prod:
                            st.error(f"Produto {it['codigo']} não encontrado no estoque.")
                            st.stop()
                        if int(it["qtd"]) > int(prod["quantidade"]):
                            st.error(f"Estoque insuficiente para {it['produto']}.")
                            st.stop()

                    if desconto < 0:
                        st.error("Desconto não pode ser negativo.")
                        st.stop()

                    if forma_ui == "Dinheiro" and recebido < total_liq:
                        st.error("Valor recebido menor que o total com desconto.")
                        st.stop()

                    # baixar estoque
                    try:
                        for it in st.session_state.cart:
                            baixar_estoque_por_codigo(it["codigo"], int(it["qtd"]))
                    except Exception as e:
                        st.error(f"Erro ao baixar estoque: {e}")
                        st.stop()

                    forma_db = map_forma_pagamento(forma_ui)

                    # gravar venda
                    try:
                        venda_id = registrar_venda_completa_db(
                            sessao_id=int(sid),
                            itens=st.session_state.cart,
                            forma_pagamento=forma_db,
                            subtotal=subtotal,
                            desconto=float(desconto),
                            total=total_liq,
                            recebido=float(recebido),
                            troco=float(troco),
                            status="FINALIZADA",
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
# Página: Estoque (ADMIN)
# =========================
elif pagina.startswith("📦"):
    if role != "ADMIN":
        st.error("Acesso negado. Apenas ADMIN pode acessar o Estoque.")
        st.stop()

    st.subheader("📦 Estoque — Produtos")

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

                upsert_produto(codigo, nome, float(custo), float(venda), int(qtd))
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
                excluir_produto(cod_del.strip())
                st.success("Excluído (se existia).")
                st.rerun()

    with cB:
        st.markdown("### Buscar por código")
        cod_busca = st.text_input("Código", value="", key="busca_code")
        if st.button("🔎 Buscar"):
            prod = buscar_produto_por_codigo(cod_busca)
            if not prod:
                st.warning("Não encontrado.")
            else:
                st.info(
                    f"**{prod['nome']}**\n\n"
                    f"Custo: R$ {brl(prod['preco_custo'])} | Venda: R$ {brl(prod['preco_venda'])} | Qtd: {prod['quantidade']}"
                )

    st.divider()
    st.markdown("### Lista de produtos")
    df = listar_produtos_df()
    if df.empty:
        st.info("Sem produtos cadastrados.")
    else:
        df_show = df.copy()
        df_show["preco_custo"] = df_show["preco_custo"].map(lambda x: f"R$ {brl(x)}")
        df_show["preco_venda"] = df_show["preco_venda"].map(lambda x: f"R$ {brl(x)}")
        st.dataframe(df_show, use_container_width=True, hide_index=True)


# =========================
# Página: Histórico
# =========================
elif pagina.startswith("📈"):
    st.subheader("📈 Histórico de Vendas (itens)")

    filtro = st.text_input("Filtrar por produto (contém)", value="")
    df = listar_vendas_itens_df(filtro_produto=filtro)

    if df.empty:
        st.info("Sem vendas (ou filtro sem resultados).")
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

    st.markdown("### Criar novo usuário")
    with st.form("criar_usuario_form"):
        nu = st.text_input("Username (ex: joao)", value="").strip().lower()
        nn = st.text_input("Nome", value="")
        nr = st.selectbox("Perfil", ["OPERADOR", "ADMIN"], index=0)
        ns = st.text_input("Senha", value="", type="password")
        nativo = st.checkbox("Ativo", value=True)
        criar = st.form_submit_button("Criar")
    if criar:
        try:
            criar_usuario(nu, nn, nr, ns, 1 if nativo else 0)
            st.success("Usuário criado!")
            st.rerun()
        except Exception as e:
            st.error(str(e))

    st.divider()
    st.markdown("### Lista de usuários")
    dfu = listar_usuarios_df()
    st.dataframe(dfu, use_container_width=True, hide_index=True)

    st.markdown("### Alterar perfil / ativar/desativar")
    with st.form("editar_usuario_form"):
        eu = st.text_input("Username para editar", value="").strip().lower()
        er = st.selectbox("Novo perfil", ["OPERADOR", "ADMIN"], index=0)
        ea = st.checkbox("Ativo", value=True)
        salvar = st.form_submit_button("Salvar alterações")
    if salvar:
        try:
            atualizar_usuario_role_ativo(eu, er, 1 if ea else 0)
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
# Página: Relatórios (ADMIN)
# =========================
else:
    if role != "ADMIN":
        st.error("Acesso negado. Apenas ADMIN pode acessar Relatórios.")
        st.stop()

    st.subheader("📅 Relatórios")

    st.markdown("### Vendas por período (por cupom)")
    c1, c2 = st.columns(2)
    with c1:
        d_ini = st.date_input("Data inicial", value=date.today().replace(day=1))
    with c2:
        d_fim = st.date_input("Data final", value=date.today())

    dt_ini = datetime(d_ini.year, d_ini.month, d_ini.day, 0, 0, 0)
    dt_fim = datetime(d_fim.year, d_fim.month, d_fim.day, 23, 59, 59)

    dfp = listar_vendas_por_periodo_df(dt_ini, dt_fim)
    if dfp.empty:
        st.info("Sem vendas no período.")
    else:
        total_periodo = float(dfp["total"].sum())
        st.metric("Total do período", f"R$ {brl(total_periodo)}")
        st.dataframe(dfp, use_container_width=True, hide_index=True)

    st.divider()
    st.markdown("### Total por dia do mês")
    ano = st.number_input("Ano", min_value=2000, max_value=2100, value=date.today().year, step=1)
    mes = st.number_input("Mês", min_value=1, max_value=12, value=date.today().month, step=1)
    dfd, total_mes = totais_por_dia_do_mes(int(ano), int(mes))
    st.metric("Total do mês", f"R$ {brl(total_mes)}")
    if dfd.empty:
        st.info("Sem vendas no mês.")
    else:
        dfd_show = dfd.copy()
        dfd_show["total"] = dfd_show["total"].map(lambda x: f"R$ {brl(x)}")
        st.dataframe(dfd_show, use_container_width=True, hide_index=True)
