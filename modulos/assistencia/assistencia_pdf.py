import os
from datetime import datetime

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
    loja_cep="",
    data_emissao=None,
    data_atualizacao=None
):
    """
    Gera o PDF da Ordem de Serviço com duas vias:
    - Via do cliente
    - Via da assistência

    data_emissao:
        Deve receber a data original de criação da OS.
        Caso não seja informada, será usada a data atual.

    data_atualizacao:
        Deve ser informada quando uma OS existente for editada.
        Caso esteja vazia, não aparecerá no PDF.
    """

    os.makedirs(PASTA_PDFS, exist_ok=True)

    caminho_pdf = os.path.join(PASTA_PDFS, f"os_{os_id}.pdf")

    c = canvas.Canvas(caminho_pdf, pagesize=A4)
    largura, altura = A4

    def texto_seguro(valor):
        return str(valor or "").strip()

    def valor_formatado(valor):
        try:
            return f"R$ {float(valor or 0):.2f}".replace(".", ",")
        except Exception:
            return "R$ 0,00"

    def formatar_data(valor):
        """
        Aceita datas como:
        2026-08-05 20:16:00
        2026-08-05T20:16:00
        datetime
        05/08/2026 às 20:16
        """

        if not valor:
            return ""

        if isinstance(valor, datetime):
            return valor.strftime("%d/%m/%Y às %H:%M")

        texto = texto_seguro(valor)

        formatos = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M",
            "%d/%m/%Y %H:%M:%S",
            "%d/%m/%Y %H:%M"
        ]

        for formato in formatos:
            try:
                data_convertida = datetime.strptime(texto, formato)
                return data_convertida.strftime("%d/%m/%Y às %H:%M")
            except ValueError:
                continue

        # Caso já esteja formatada ou venha em formato desconhecido
        return texto

    # Se o sistema não enviar a data original, usa a data atual.
    data_emissao_formatada = formatar_data(data_emissao)

    if not data_emissao_formatada:
        data_emissao_formatada = datetime.now().strftime(
            "%d/%m/%Y às %H:%M"
        )

    data_atualizacao_formatada = formatar_data(data_atualizacao)

    def desenhar_via(titulo_via, y_inicio):
        y = y_inicio

        endereco_loja = ""

        if texto_seguro(loja_rua) or texto_seguro(loja_numero):
            endereco_loja = (
                f"{texto_seguro(loja_rua)}, "
                f"{texto_seguro(loja_numero)}"
            ).strip(", ")

        partes_complemento = []

        if texto_seguro(loja_bairro):
            partes_complemento.append(texto_seguro(loja_bairro))

        if texto_seguro(loja_cidade):
            partes_complemento.append(texto_seguro(loja_cidade))

        if texto_seguro(loja_cep):
            partes_complemento.append(
                f"CEP: {texto_seguro(loja_cep)}"
            )

        complemento_loja = " - ".join(partes_complemento)

        # =========================
        # Cabeçalho
        # =========================
        c.setFont("Helvetica-Bold", 14)
        c.drawString(
            50,
            y,
            texto_seguro(loja_nome) or "CAMARGO CELULARES"
        )

        c.setFont("Helvetica-Bold", 10)
        c.drawRightString(
            largura - 50,
            y,
            titulo_via
        )

        y -= 16

        c.setFont("Helvetica", 8)

        if texto_seguro(loja_subtitulo):
            c.drawString(
                50,
                y,
                texto_seguro(loja_subtitulo)
            )
            y -= 12

        if texto_seguro(loja_whatsapp):
            c.drawString(
                50,
                y,
                f"WhatsApp: {texto_seguro(loja_whatsapp)}"
            )
            y -= 12

        if endereco_loja:
            c.drawString(
                50,
                y,
                endereco_loja
            )
            y -= 12

        if complemento_loja:
            c.drawString(
                50,
                y,
                complemento_loja
            )
            y -= 12

        y -= 2

        c.line(
            50,
            y,
            largura - 50,
            y
        )

        y -= 18

        # =========================
        # Número e datas da OS
        # =========================
        c.setFont("Helvetica-Bold", 11)
        c.drawString(
            50,
            y,
            f"ORDEM DE SERVIÇO #{os_id}"
        )

        c.setFont("Helvetica-Bold", 8)
        c.drawRightString(
            largura - 50,
            y,
            f"Emissão: {data_emissao_formatada}"
        )

        y -= 13

        if data_atualizacao_formatada:
            c.setFont("Helvetica", 7)
            c.drawRightString(
                largura - 50,
                y,
                f"Última atualização: {data_atualizacao_formatada}"
            )
            y -= 12
        else:
            y -= 5

        # =========================
        # Dados da OS
        # =========================
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
            c.drawString(
                50,
                y,
                f"{titulo}:"
            )

            c.setFont("Helvetica", 8)
            c.drawString(
                115,
                y,
                texto_seguro(valor)
            )

            y -= 10

        # =========================
        # Garantia
        # =========================
        c.setFont("Helvetica-Bold", 8)
        c.drawString(
            50,
            y,
            "Garantia:"
        )

        y -= 10

        c.setFont("Helvetica", 7)

        garantia_linha_1 = (
            "Garantia de 30 dias sobre o serviço executado. "
            "Não cobre queda, mau uso, oxidação"
        )

        garantia_linha_2 = (
            "ou defeitos diferentes do informado nesta "
            "Ordem de Serviço."
        )

        c.drawString(
            50,
            y,
            garantia_linha_1
        )

        y -= 9

        c.drawString(
            50,
            y,
            garantia_linha_2
        )

        y -= 15

        # =========================
        # QR Code
        # =========================
        if qr_path and os.path.exists(qr_path):
            try:
                c.drawImage(
                    qr_path,
                    largura - 150,
                    y - 20,
                    width=85,
                    height=85,
                    preserveAspectRatio=True,
                    mask="auto"
                )

                c.setFont("Helvetica-Bold", 7)
                c.drawString(
                    largura - 155,
                    y - 30,
                    "Acompanhe pelo QR Code"
                )

            except Exception:
                # Não impede a geração do PDF caso o QR apresente erro.
                pass

        # =========================
        # Declaração e assinaturas
        # =========================
        c.setFont("Helvetica", 8)
        c.drawString(
            50,
            y,
            "Declaro estar ciente das condições da assistência técnica."
        )

        y -= 32

        c.line(
            50,
            y,
            240,
            y
        )

        c.drawString(
            85,
            y - 12,
            "Assinatura do Cliente"
        )

        c.line(
            320,
            y,
            530,
            y
        )

        c.drawString(
            365,
            y - 12,
            "Responsável Técnico"
        )

    # =========================
    # Via do cliente
    # =========================
    desenhar_via(
        "VIA DO CLIENTE",
        altura - 35
    )

    # =========================
    # Linha de corte
    # =========================
    meio = altura / 2

    c.setDash(4, 4)
    c.line(
        30,
        meio,
        largura - 30,
        meio
    )
    c.setDash()

    c.setFont("Helvetica", 7)
    c.drawCentredString(
        largura / 2,
        meio + 5,
        "CORTAR AQUI"
    )

    # =========================
    # Via da assistência
    # =========================
    desenhar_via(
        "VIA DA ASSISTÊNCIA",
        meio - 25
    )

    c.save()

    return caminho_pdf