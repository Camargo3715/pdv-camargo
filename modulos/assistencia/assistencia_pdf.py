import os
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

PASTA_PDFS = "pdfs_os"


def gerar_pdf_os(
    os_id,
    cliente,
    cpf_rg,
    telefone,
    rua,
    cep,
    aparelho,
    marca,
    modelo,
    defeito,
    senha,
    valor_servico,
    qr_path,
    loja_nome="CAMARGO CELULARES",
    loja_subtitulo="Assistência Técnica Especializada",
    loja_whatsapp="(11) 99999-9999",
    loja_rua="",
    loja_numero="",
    loja_bairro="",
    loja_cidade="",
    loja_cep=""
):
    os.makedirs(PASTA_PDFS, exist_ok=True)

    caminho_pdf = os.path.join(PASTA_PDFS, f"os_{os_id}.pdf")

    c = canvas.Canvas(caminho_pdf, pagesize=A4)
    largura, altura = A4

    def texto_seguro(valor):
        return str(valor or "").strip()

    def valor_formatado(valor):
        try:
            return f"R$ {float(valor or 0):.2f}"
        except Exception:
            return "R$ 0.00"

    def desenhar_via(titulo_via, y_inicio):
        y = y_inicio

        endereco_loja = ""
        if texto_seguro(loja_rua) or texto_seguro(loja_numero):
            endereco_loja = f"{texto_seguro(loja_rua)}, {texto_seguro(loja_numero)}".strip(", ")

        complemento_loja = ""
        partes = []

        if texto_seguro(loja_bairro):
            partes.append(texto_seguro(loja_bairro))

        if texto_seguro(loja_cidade):
            partes.append(texto_seguro(loja_cidade))

        if texto_seguro(loja_cep):
            partes.append(f"CEP: {texto_seguro(loja_cep)}")

        complemento_loja = " - ".join(partes)

        c.setFont("Helvetica-Bold", 14)
        c.drawString(50, y, texto_seguro(loja_nome) or "CAMARGO CELULARES")

        c.setFont("Helvetica-Bold", 10)
        c.drawRightString(largura - 50, y, titulo_via)
        y -= 16

        c.setFont("Helvetica", 8)

        if texto_seguro(loja_subtitulo):
            c.drawString(50, y, texto_seguro(loja_subtitulo))
            y -= 12

        if texto_seguro(loja_whatsapp):
            c.drawString(50, y, f"WhatsApp: {texto_seguro(loja_whatsapp)}")
            y -= 12

        if endereco_loja:
            c.drawString(50, y, endereco_loja)
            y -= 12

        if complemento_loja:
            c.drawString(50, y, complemento_loja)
            y -= 12

        y -= 2
        c.line(50, y, largura - 50, y)
        y -= 18

        c.setFont("Helvetica-Bold", 11)
        c.drawString(50, y, f"ORDEM DE SERVIÇO #{os_id}")
        y -= 18

        campos = [
            ("Cliente", cliente),
            ("CPF/RG", cpf_rg),
            ("Telefone", telefone),
            ("Rua", rua),
            ("CEP", cep),
            ("Aparelho", aparelho),
            ("Marca", marca),
            ("Modelo", modelo),
            ("Defeito", defeito),
            ("Senha", senha),
            ("Valor", valor_formatado(valor_servico))
        ]

        for titulo, valor in campos:
            c.setFont("Helvetica-Bold", 8)
            c.drawString(50, y, f"{titulo}:")
            c.setFont("Helvetica", 8)
            c.drawString(115, y, texto_seguro(valor))
            y -= 10

        c.setFont("Helvetica-Bold", 8)
        c.drawString(50, y, "Garantia:")
        y -= 10

        c.setFont("Helvetica", 7)
        garantia = (
            "Garantia de 30 dias sobre o serviço executado. "
            "Não cobre queda, mau uso, oxidação ou defeitos diferentes do informado nesta OS."
        )

        c.drawString(50, y, garantia[:115])
        y -= 9
        c.drawString(50, y, garantia[115:])
        y -= 15

        if qr_path and os.path.exists(qr_path):
            c.drawImage(qr_path, largura - 150, y - 20, width=85, height=85)
            c.setFont("Helvetica-Bold", 7)
            c.drawString(largura - 155, y - 30, "Acompanhe pelo QR Code")

        c.setFont("Helvetica", 8)
        c.drawString(50, y, "Declaro estar ciente das condições da assistência técnica.")
        y -= 32

        c.line(50, y, 240, y)
        c.drawString(85, y - 12, "Assinatura do Cliente")

        c.line(320, y, 530, y)
        c.drawString(365, y - 12, "Responsável Técnico")

    desenhar_via("VIA DO CLIENTE", altura - 35)

    meio = altura / 2
    c.setDash(4, 4)
    c.line(30, meio, largura - 30, meio)
    c.setDash()
    c.setFont("Helvetica", 7)
    c.drawCentredString(largura / 2, meio + 5, "CORTAR AQUI")

    desenhar_via("VIA DA ASSISTÊNCIA", meio - 25)

    c.save()

    return caminho_pdf