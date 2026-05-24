import os
import streamlit as st
import pandas as pd

from modulos.assistencia.assistencia_db import *
from modulos.assistencia.assistencia_qrcode import gerar_qrcode_os
from modulos.assistencia.assistencia_pdf import gerar_pdf_os


def tela_assistencia():

    st.title("🔧 Assistência Técnica")

    if "ultimo_pdf_os" not in st.session_state:
        st.session_state.ultimo_pdf_os = None
    if "ultimo_os_id" not in st.session_state:
        st.session_state.ultimo_os_id = None
    if "ultimo_qr_os" not in st.session_state:
        st.session_state.ultimo_qr_os = None
    if "ultimo_link_os" not in st.session_state:
        st.session_state.ultimo_link_os = None

    st.subheader("Nova Ordem de Serviço")

    with st.form("nova_os"):

        cliente = st.text_input("Nome do Cliente")
        cpf_rg = st.text_input("CPF ou RG")
        telefone = st.text_input("Telefone")
        rua = st.text_input("Rua")
        cep = st.text_input("CEP")

        aparelho = st.text_input("Aparelho")
        marca = st.text_input("Marca")
        modelo = st.text_input("Modelo")
        defeito = st.text_area("Defeito Informado")

        valor_servico = st.number_input(
            "Valor do serviço",
            min_value=0.0,
            step=10.0,
            format="%.2f"
        )

        senha = st.text_input("Senha do aparelho")

        salvar = st.form_submit_button("Criar OS")

        if salvar:

            endereco = f"Rua: {rua} | CEP: {cep}"

            os_id, token_publico = criar_os(
                cliente_nome=cliente,
                cpf=cpf_rg,
                telefone=telefone,
                endereco=endereco,
                aparelho=aparelho,
                marca=marca,
                modelo=modelo,
                defeito=defeito,
                senha_aparelho=senha,
                valor_servico=valor_servico
            )

            caminho_qr, link_os = gerar_qrcode_os(token_publico, os_id)

            caminho_pdf = gerar_pdf_os(
                os_id=os_id,
                cliente=cliente,
                cpf_rg=cpf_rg,
                telefone=telefone,
                rua=rua,
                cep=cep,
                aparelho=aparelho,
                marca=marca,
                modelo=modelo,
                defeito=defeito,
                senha=senha,
                valor_servico=valor_servico,
                qr_path=caminho_qr
            )

            st.session_state.ultimo_pdf_os = caminho_pdf
            st.session_state.ultimo_os_id = os_id
            st.session_state.ultimo_qr_os = caminho_qr
            st.session_state.ultimo_link_os = link_os

    if st.session_state.ultimo_os_id:
        st.success(f"OS criada com sucesso: #{st.session_state.ultimo_os_id}")
        st.image(st.session_state.ultimo_qr_os, width=200)
        st.code(st.session_state.ultimo_link_os)

        with open(st.session_state.ultimo_pdf_os, "rb") as f:
            st.download_button(
                "📄 Baixar comprovante PDF",
                f,
                file_name=f"os_{st.session_state.ultimo_os_id}.pdf",
                mime="application/pdf"
            )

    st.divider()

    st.subheader("Ordens de Serviço")

    dados = listar_os()
    lista = []

    for os_item in dados:
        lista.append({
            "OS": os_item["id"],
            "Cliente": os_item["cliente_nome"],
            "CPF/RG": os_item["cpf"],
            "Telefone": os_item["telefone"],
            "Endereço": os_item["endereco"],
            "Aparelho": os_item["aparelho"],
            "Marca": os_item["marca"],
            "Modelo": os_item["modelo"],
            "Valor": f"R$ {float(os_item['orcamento'] or 0):.2f}".replace(".", ","),
            "Status": os_item["status"],
            "Entrada": os_item["data_entrada"]
        })

    if lista:
        df = pd.DataFrame(lista)
        st.dataframe(df, use_container_width=True)
    else:
        st.info("Nenhuma OS cadastrada.")

    st.divider()

    st.subheader("Alterar Status da OS")

    if lista:
        os_ids = [item["OS"] for item in lista]

        os_selecionada = st.selectbox("Selecione a OS", os_ids)

        novo_status = st.selectbox(
            "Novo status",
            [
                "📥 RECEBIDO",
                "🔍 EM ANALISE",
                "🛠️ EM REPARO",
                "📦 AGUARDANDO PEÇA",
                "✅ PRONTO PARA RETIRADA",
                "❌ SEM CONSERTO",
                "📤 RETIRADO"
            ]
        )

        if st.button("Atualizar Status"):
            atualizar_status_os(os_selecionada, novo_status)
            st.success("Status atualizado com sucesso!")
            st.rerun()
    else:
        st.info("Crie uma OS primeiro para alterar o status.")

    st.divider()

    st.subheader("Excluir OS")

    if lista:
        os_ids_excluir = [item["OS"] for item in lista]

        os_para_excluir = st.selectbox(
            "Selecione a OS para excluir",
            os_ids_excluir,
            key="select_excluir_os"
        )

        confirmar_exclusao = st.checkbox(
            f"Confirmo que quero excluir a OS #{os_para_excluir}",
            key="confirmar_exclusao_os"
        )

        if st.button("🗑️ Excluir OS", type="secondary"):

            if not confirmar_exclusao:
                st.warning("Marque a confirmação antes de excluir.")
            else:
                excluir_os(os_para_excluir)

                pdf_path = os.path.join("pdfs_os", f"os_{os_para_excluir}.pdf")
                qr_path = os.path.join("qrcodes_os", f"os_{os_para_excluir}.png")

                if os.path.exists(pdf_path):
                    os.remove(pdf_path)

                if os.path.exists(qr_path):
                    os.remove(qr_path)

                if st.session_state.ultimo_os_id == os_para_excluir:
                    st.session_state.ultimo_pdf_os = None
                    st.session_state.ultimo_os_id = None
                    st.session_state.ultimo_qr_os = None
                    st.session_state.ultimo_link_os = None

                st.success(f"OS #{os_para_excluir} excluída com sucesso!")
                st.rerun()
    else:
        st.info("Nenhuma OS cadastrada para excluir.")