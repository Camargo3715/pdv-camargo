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
    qr_path
):
    os.makedirs(PASTA_PDFS, exist_ok=True)

    caminho_pdf = os.path.join(PASTA_PDFS, f"os_{os_id}.pdf")

    c = canvas.Canvas(caminho_pdf, pagesize=A4)
    largura, altura = A4

    def desenhar_via(titulo_via, y_inicio):
        y = y_inicio

        c.setFont("Helvetica-Bold", 14)
        c.drawString(50, y, "CAMARGO CELULARES")
        c.setFont("Helvetica-Bold", 10)
        c.drawRightString(largura - 50, y, titulo_via)
        y -= 16

        c.setFont("Helvetica", 8)
        c.drawString(50, y, "Assistência Técnica Especializada")
        y -= 12
        c.drawString(50, y, "WhatsApp: (11) 99999-9999")
        y -= 14

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
            ("Valor", f"R$ {float(valor_servico or 0):.2f}")
        ]

        for titulo, valor in campos:
            c.setFont("Helvetica-Bold", 8)
            c.drawString(50, y, f"{titulo}:")
            c.setFont("Helvetica", 8)
            c.drawString(115, y, str(valor or ""))
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