import sqlite3

conexao = sqlite3.connect("pdv.db")
cursor = conexao.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS produtos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nome TEXT NOT NULL,
    preco REAL NOT NULL,
    quantidade INTEGER NOT NULL
)
""")

conexao.commit()
conexao.close()

print("Banco de dados criado com sucesso!")
