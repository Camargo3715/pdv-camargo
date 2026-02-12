import streamlit as st
from core_db import (
    inicializar_banco,
    get_sessao_aberta, abrir_caixa, fechar_caixa,
    relatorio_pagamentos_sessao, totais_sessao,
    buscar_produto, listar_produtos,
    registrar_venda, listar_vendas_itens
)

def map_forma(ui: str) -> str:
    s = (ui or "").strip().lower()
    if s == "pix": return "PIX"
    if s == "dinheiro": return "DINHEIRO"
    if "crédito" in s or "credito" in s: return "CARTAO_CREDITO"
    if "débito" in s or "debito" in s: return "CARTAO_DEBITO"
    return "OUTRO"

def ensure_state():
    if "carrinho" not in st.session_state:
        st.session_state.carrinho = {}
    if "codigo" not in st.session_state:
        st.session_state.codigo = ""
    if "qtd" not in st.session_state:
        st.session_state.qtd = 1
    if "forma" not in st.session_state:
        st.session_state.forma = "Pix"
    if "desconto" not in st.session_state:
        st.session_state.desconto = 0.0
    if "recebido" not in st.session_state:
        st.session_state.recebido = 0.0

ensure_state()
inicializar_banco()

st.set_page_config(page_title="Camargo Celulares — PDV Web", layout="wide")
st.title("🧾 Camargo Celulares — PDV Web")

sess = get_sessao_aberta()

tab_pdv, tab_estoque, tab_hist, tab_caixa = st.tabs(["🧾 PDV", "📦 Estoque", "📈 Histórico", "🔐 Caixa"])

# -------------------------
# PDV
# -------------------------
with tab_pdv:
    if not sess:
        st.warning("Caixa FECHADO. Abra o caixa na aba **Caixa** para vender.")
    else:
        sid, aberto_em, saldo_inicial, operador = sess
        st.success(f"CAIXA ABERTO — Sessão #{sid} (aberto em {aberto_em})")

        colA, colB = st.columns([2, 1])

        with colA:
            st.subheader("🛒 Carrinho")

            with st.form("add_form"):
                codigo = st.text_input("Código do produto", value=st.session_state.codigo)
                qtd = st.number_input("Qtd", min_value=1, value=int(st.session_state.qtd), step=1)
                add = st.form_submit_button("Adicionar")

                if add:
                    prod = buscar_produto(codigo)
                    if not prod:
                        st.error("Produto não encontrado.")
                    else:
                        cod, nome, custo, venda, estoque = prod
                        estoque = int(estoque or 0)
                        ja = int(st.session_state.carrinho.get(cod, {}).get("qtd", 0))
                        if ja + int(qtd) > estoque:
                            st.error(f"Estoque insuficiente. Disponível: {estoque} | No carrinho: {ja}")
                        else:
                            if cod in st.session_state.carrinho:
                                st.session_state.carrinho[cod]["qtd"] += int(qtd)
                            else:
                                st.session_state.carrinho[cod] = {
                                    "produto": nome,
                                    "preco": float(venda),
                                    "custo": float(custo or 0.0),
                                    "qtd": int(qtd),
                                }
                            st.session_state.codigo = ""
                            st.session_state.qtd = 1
                            st.rerun()

            carr = st.session_state.carrinho
            if not carr:
                st.info("Carrinho vazio.")
            else:
                rows = []
                subtotal = 0.0
                for cod, item in carr.items():
                    sub = float(item["preco"]) * int(item["qtd"])
                    subtotal += sub
                    rows.append({
                        "Código": cod,
                        "Produto": item["produto"],
                        "Preço": f"{item['preco']:.2f}",
                        "Qtd": item["qtd"],
                        "Subtotal": f"{sub:.2f}",
                    })
                st.dataframe(rows, use_container_width=True, hide_index=True)

                c1, c2 = st.columns([2, 1])
                with c2:
                    sel = st.selectbox("Remover item", options=["(selecionar)"] + list(carr.keys()))
                    if st.button("Remover", use_container_width=True, disabled=(sel == "(selecionar)")):
                        carr.pop(sel, None)
                        st.rerun()

                if st.button("Limpar carrinho", use_container_width=True):
                    st.session_state.carrinho = {}
                    st.rerun()

        with colB:
            st.subheader("💳 Pagamento")

            forma = st.selectbox("Forma", ["Pix", "Dinheiro", "Cartão Crédito", "Cartão Débito"],
                                 index=["Pix","Dinheiro","Cartão Crédito","Cartão Débito"].index(st.session_state.forma))
            st.session_state.forma = forma

            desconto = st.number_input("Desconto (R$)", min_value=0.0, value=float(st.session_state.desconto), step=1.0)
            st.session_state.desconto = desconto

            subtotal = sum(float(i["preco"]) * int(i["qtd"]) for i in st.session_state.carrinho.values())
            total = max(0.0, subtotal - float(desconto or 0.0))

            if forma == "Dinheiro":
                recebido = st.number_input("Recebido (R$)", min_value=0.0, value=float(st.session_state.recebido), step=1.0)
                st.session_state.recebido = recebido
                troco = max(0.0, float(recebido) - total)
            else:
                st.session_state.recebido = 0.0
                recebido = 0.0
                troco = 0.0

            st.markdown("---")
            st.metric("Subtotal", f"R$ {subtotal:.2f}")
            st.metric("Total", f"R$ {total:.2f}")
            st.metric("Troco", f"R$ {troco:.2f}")

            if st.button("✅ Finalizar venda", use_container_width=True, disabled=(not st.session_state.carrinho)):
                if forma == "Dinheiro" and recebido < total:
                    st.error("Recebido menor que o total.")
                else:
                    try:
                        itens = []
                        for cod, item in st.session_state.carrinho.items():
                            itens.append({
                                "codigo": cod,
                                "produto": item["produto"],
                                "preco_unit": float(item["preco"]),
                                "preco_custo": float(item.get("custo") or 0.0),
                                "qtd": int(item["qtd"]),
                            })
                        venda_id = registrar_venda(
                            sessao_id=int(sid),
                            itens=itens,
                            forma_pagamento=map_forma(forma),
                            desconto=float(desconto or 0.0),
                            recebido=float(recebido or 0.0),
                            troco=float(troco or 0.0),
                        )
                        st.success(f"Venda registrada! Cupom #{venda_id}")
                        st.session_state.carrinho = {}
                        st.session_state.desconto = 0.0
                        st.session_state.recebido = 0.0
                        st.rerun()
                    except Exception as e:
                        st.error(str(e))

# -------------------------
# Estoque
# -------------------------
with tab_estoque:
    st.subheader("📦 Produtos (banco)")

    prods = listar_produtos()
    st.dataframe(
        [{"Código": c, "Produto": n, "Custo": float(pc), "Venda": float(pv), "Qtd": int(q)} for c, n, pc, pv, q in prods],
        use_container_width=True,
        hide_index=True
    )

# -------------------------
# Histórico
# -------------------------
with tab_hist:
    st.subheader("📈 Histórico (itens)")
    vendas = listar_vendas_itens(limit=300)
    st.dataframe(
        [{"Cupom": cupom, "DataHora": dh, "Pagamento": fp, "Código": cod, "Produto": prod, "Preço": float(pu), "Qtd": int(q), "Total": float(t)}
         for cupom, dh, fp, cod, prod, pu, q, t in vendas],
        use_container_width=True,
        hide_index=True
    )

# -------------------------
# Caixa
# -------------------------
with tab_caixa:
    st.subheader("🔐 Caixa")

    col1, col2 = st.columns([1, 1])

    with col1:
        st.markdown("### Abertura")
        if sess:
            sid, aberto_em, saldo_inicial, operador = sess
            st.success(f"ABERTO — Sessão #{sid}")
            st.write(f"Aberto em: {aberto_em}")
            st.write(f"Saldo inicial: R$ {float(saldo_inicial):.2f}")
            if operador:
                st.write(f"Operador: {operador}")
        else:
            saldo = st.number_input("Saldo inicial", min_value=0.0, value=0.0, step=10.0)
            operador = st.text_input("Operador (opcional)")
            obs = st.text_input("Obs (opcional)")
            if st.button("Abrir Caixa", use_container_width=True):
                try:
                    sid = abrir_caixa(saldo, operador, obs)
                    st.success(f"Caixa aberto! Sessão #{sid}")
                    st.rerun()
                except Exception as e:
                    st.error(str(e))

    with col2:
        st.markdown("### Fechamento / Relatório")
        if not sess:
            st.info("Caixa fechado.")
        else:
            sid = int(sess[0])
            rel = relatorio_pagamentos_sessao(sid)
            saldo_ini, total_vendas, saldo_final_sistema, _ = totais_sessao(sid)

            st.write("**Relatório por pagamento (sessão aberta):**")
            if not rel:
                st.write("Sem vendas nesta sessão.")
            else:
                st.table([{"Forma": f, "Total": float(t)} for f, t in rel])

            st.markdown("---")
            st.metric("Total vendas", f"R$ {total_vendas:.2f}")
            st.metric("Saldo final (sistema)", f"R$ {saldo_final_sistema:.2f}")

            contado = st.number_input("Valor contado", min_value=0.0, value=float(saldo_final_sistema), step=10.0)
            obs_f = st.text_input("Obs fechamento (opcional)")

            if st.button("Fechar Caixa", use_container_width=True):
                try:
                    r = fechar_caixa(sid, contado, obs_f)
                    st.success(
                        f"Caixa fechado!\n\n"
                        f"Total vendas: R$ {r['total_vendas']:.2f}\n"
                        f"Sistema: R$ {r['saldo_final_sistema']:.2f}\n"
                        f"Contado: R$ {r['saldo_informado']:.2f}\n"
                        f"Diferença: R$ {r['diferenca']:.2f}\n"
                        f"Fechado em: {r['fechado_em']}"
                    )
                    st.rerun()
                except Exception as e:
                    st.error(str(e))
