import os
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

PASTA_PDFS = "pdfs_os"


def gerar_pdf_os(
    os_id,
    cliente,
    telefone,
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

    y = altura - 45

    # =========================
    # CABEÇALHO
    # =========================

    c.setFont("Helvetica-Bold", 18)
    c.drawString(50, y, "CAMARGO CELULARES")
    y -= 22

    c.setFont("Helvetica", 10)
    c.drawString(50, y, "Assistência Técnica Especializada")
    y -= 15

    c.drawString(50, y, "WhatsApp: (11) 99999-9999")
    y -= 25

    c.line(50, y, largura - 50, y)
    y -= 25

    # =========================
    # ORDEM DE SERVIÇO
    # =========================

    c.setFont("Helvetica-Bold", 14)
    c.drawString(50, y, f"ORDEM DE SERVIÇO #{os_id}")
    y -= 30

    campos = [
        ("Cliente", cliente),
        ("Telefone", telefone),
        ("Aparelho", aparelho),
        ("Marca", marca),
        ("Modelo", modelo),
        ("Defeito", defeito),
        ("Senha", senha),
        ("Valor do serviço", f"R$ {float(valor_servico or 0):.2f}")
    ]

    for titulo, valor in campos:

        c.setFont("Helvetica-Bold", 10)
        c.drawString(50, y, f"{titulo}:")

        c.setFont("Helvetica", 10)
        c.drawString(180, y, str(valor or ""))

        y -= 22

    # =========================
    # GARANTIA
    # =========================

    y -= 10

    c.line(50, y, largura - 50, y)

    y -= 25

    c.setFont("Helvetica-Bold", 11)
    c.drawString(50, y, "GARANTIA")
    y -= 20

    c.setFont("Helvetica", 9)

    garantia = [
        "• Garantia de 30 dias sobre o serviço executado.",
        "• A garantia cobre apenas o defeito informado nesta OS.",
        "• Aparelhos com sinais de queda ou oxidação perdem a garantia.",
        "• É obrigatória a apresentação deste comprovante."
    ]

    for linha in garantia:
        c.drawString(60, y, linha)
        y -= 16

    # =========================
    # QR CODE
    # =========================

    y -= 10

    c.line(50, y, largura - 50, y)

    y -= 30

    c.setFont("Helvetica-Bold", 11)
    c.drawString(50, y, "Acompanhe sua OS pelo QR Code")
    y -= 140

    if qr_path and os.path.exists(qr_path):
        c.drawImage(qr_path, 50, y, width=120, height=120)

    # =========================
    # ASSINATURAS
    # =========================

    c.setFont("Helvetica", 9)

    c.drawString(
        50,
        120,
        "Declaro estar ciente das condições da assistência técnica."
    )

    c.line(50, 80, 250, 80)
    c.drawString(95, 65, "Assinatura do Cliente")

    c.line(330, 80, 530, 80)
    c.drawString(375, 65, "Responsável Técnico")

    c.save()

    return caminho_pdf