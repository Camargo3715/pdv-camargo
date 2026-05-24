import os
import qrcode

PASTA_QRCODES = "qrcodes_os"


def gerar_qrcode_os(token_publico: str, os_id: int):

    os.makedirs(PASTA_QRCODES, exist_ok=True)

    # depois trocamos pelo link do Render
    base_url = "https://assistencia.camargotech.com.br"

    link = f"{base_url}/?token={token_publico}"

    caminho = os.path.join(
        PASTA_QRCODES,
        f"os_{os_id}.png"
    )

    img = qrcode.make(link)

    img.save(caminho)

    return caminho, link