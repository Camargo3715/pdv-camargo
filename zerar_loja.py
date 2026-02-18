import sqlite3

DB = "pdv.db"
LOJA_ID = 1  # mude para 2 ou 3 se quiser

def c(cur, q, p=()):
    cur.execute(q, p)
    return int(cur.fetchone()[0] or 0)

conn = sqlite3.connect(DB)
conn.execute("PRAGMA foreign_keys=ON;")
cur = conn.cursor()

antes = {
    "produtos": c(cur, "SELECT COUNT(*) FROM produtos WHERE loja_id=?", (LOJA_ID,)),
    "vendas_cabecalho": c(cur, "SELECT COUNT(*) FROM vendas_cabecalho WHERE loja_id=?", (LOJA_ID,)),
    "vendas_itens": c(cur, "SELECT COUNT(*) FROM vendas_itens WHERE loja_id=?", (LOJA_ID,)),
    "caixa_sessoes": c(cur, "SELECT COUNT(*) FROM caixa_sessoes WHERE loja_id=?", (LOJA_ID,)),
    "vendas_legado": c(cur, "SELECT COUNT(*) FROM vendas WHERE loja_id=?", (LOJA_ID,)),
}
print("ANTES:", antes)

aberto = c(cur, "SELECT COUNT(*) FROM caixa_sessoes WHERE loja_id=? AND status='ABERTO'", (LOJA_ID,))
print("CAIXA_ABERTO:", aberto)

if aberto != 0:
    print("FECHA O CAIXA DA LOJA ANTES DE ZERAR.")
else:
    cur.execute("DELETE FROM vendas_itens WHERE loja_id=?", (LOJA_ID,))
    cur.execute("DELETE FROM vendas_cabecalho WHERE loja_id=?", (LOJA_ID,))
    cur.execute("DELETE FROM caixa_sessoes WHERE loja_id=?", (LOJA_ID,))
    cur.execute("DELETE FROM vendas WHERE loja_id=?", (LOJA_ID,))
    cur.execute("DELETE FROM produtos WHERE loja_id=?", (LOJA_ID,))
    conn.commit()

    depois = {
        "produtos": c(cur, "SELECT COUNT(*) FROM produtos WHERE loja_id=?", (LOJA_ID,)),
        "vendas_cabecalho": c(cur, "SELECT COUNT(*) FROM vendas_cabecalho WHERE loja_id=?", (LOJA_ID,)),
        "vendas_itens": c(cur, "SELECT COUNT(*) FROM vendas_itens WHERE loja_id=?", (LOJA_ID,)),
        "caixa_sessoes": c(cur, "SELECT COUNT(*) FROM caixa_sessoes WHERE loja_id=?", (LOJA_ID,)),
        "vendas_legado": c(cur, "SELECT COUNT(*) FROM vendas WHERE loja_id=?", (LOJA_ID,)),
    }
    print("ZERADO!")
    print("DEPOIS:", depois)

conn.close()
