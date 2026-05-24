from modulos.assistencia.assistencia_db import *

inicializar_assistencia()

os_id = criar_os(
    cliente_nome="Camila",
    telefone="11999999999",
    aparelho="Samsung A15",
    marca="Samsung",
    modelo="A15",
    defeito="Tela quebrada",
    senha_aparelho="1990"
)

print(f"OS criada com sucesso: #{os_id}")

dados = listar_os()

for os in dados:
    print(dict(os))