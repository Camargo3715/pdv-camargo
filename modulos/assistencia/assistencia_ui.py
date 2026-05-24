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

    st.subheader("🏪 Configuração das Lojas")

    lojas = listar_lojas()

    if not lojas:
        criar_loja(nome="CAMARGO CELULARES")
        lojas = listar_lojas()

    loja_ids = [loja["id"] for loja in lojas]

    loja_config_id = st.selectbox(
        "Selecione a loja para editar",
        loja_ids,
        format_func=lambda loja_id: buscar_loja_por_id(loja_id)["nome"],
        key="config_loja_id"
    )

    loja_dados = buscar_loja_por_id(loja_config_id)

    with st.expander("Editar dados da loja"):

        nome_loja = st.text_input("Nome da loja", value=loja_dados["nome"] or "", key=f"nome_loja_{loja_config_id}")
        subtitulo_loja = st.text_input("Subtítulo", value=loja_dados["subtitulo"] or "", key=f"subtitulo_loja_{loja_config_id}")
        whatsapp_loja = st.text_input("WhatsApp", value=loja_dados["whatsapp"] or "", key=f"whatsapp_loja_{loja_config_id}")
        rua_loja = st.text_input("Rua", value=loja_dados["rua"] or "", key=f"rua_loja_{loja_config_id}")
        numero_loja = st.text_input("Número", value=loja_dados["numero"] or "", key=f"numero_loja_{loja_config_id}")
        bairro_loja = st.text_input("Bairro", value=loja_dados["bairro"] or "", key=f"bairro_loja_{loja_config_id}")
        cidade_loja = st.text_input("Cidade", value=loja_dados["cidade"] or "", key=f"cidade_loja_{loja_config_id}")
        cep_loja = st.text_input("CEP", value=loja_dados["cep"] or "", key=f"cep_loja_{loja_config_id}")

        if st.button("Salvar dados da loja", key=f"salvar_loja_{loja_config_id}"):

            atualizar_loja(
                loja_id=loja_config_id,
                nome=nome_loja,
                subtitulo=subtitulo_loja,
                whatsapp=whatsapp_loja,
                rua=rua_loja,
                numero=numero_loja,
                bairro=bairro_loja,
                cidade=cidade_loja,
                cep=cep_loja
            )

            st.success("Dados da loja atualizados!")
            st.rerun()

    st.divider()

    st.subheader("Nova Ordem de Serviço")

    lojas = listar_lojas()

    nomes_lojas = {
        f"{loja['nome']} - Loja {loja['id']}": loja["id"]
        for loja in lojas
    }

    with st.form("nova_os"):

        loja_escolhida_nome = st.selectbox(
            "Loja",
            list(nomes_lojas.keys())
        )

        loja_id = nomes_lojas[loja_escolhida_nome]

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

            endereco = rua

            os_id, token_publico = criar_os(
                cliente_nome=cliente,
                cpf=cpf_rg,
                telefone=telefone,
                endereco=endereco,
                cep=cep,
                aparelho=aparelho,
                marca=marca,
                modelo=modelo,
                defeito=defeito,
                senha_aparelho=senha,
                valor_servico=valor_servico,
                loja_id=loja_id
            )

            caminho_qr, link_os = gerar_qrcode_os(token_publico, os_id)

            loja_pdf = buscar_loja_por_id(loja_id)

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
                qr_path=caminho_qr,
                loja_nome=loja_pdf["nome"],
                loja_subtitulo=loja_pdf["subtitulo"],
                loja_whatsapp=loja_pdf["whatsapp"],
                loja_rua=loja_pdf["rua"],
                loja_numero=loja_pdf["numero"],
                loja_bairro=loja_pdf["bairro"],
                loja_cidade=loja_pdf["cidade"],
                loja_cep=loja_pdf["cep"]
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
            "Loja": os_item["loja_nome"],
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

        os_selecionada = st.selectbox(
            "Selecione a OS",
            os_ids
        )

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

            atualizar_status_os(
                os_selecionada,
                novo_status
            )

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

                pdf_path = os.path.join(
                    "pdfs_os",
                    f"os_{os_para_excluir}.pdf"
                )

                qr_path = os.path.join(
                    "qrcodes_os",
                    f"os_{os_para_excluir}.png"
                )

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