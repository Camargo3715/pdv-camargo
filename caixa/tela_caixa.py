# caixa/tela_caixa.py
# Layout estilo PDV: itens/carrinho à esquerda + pagamento (sidebar) à direita
# UPDATE: Cupom estilo mercado real + preview antes de imprimir
# UPDATE2: Lê config_pdv.json (dados da loja) automaticamente via config_pdv.py
# UPDATE3: Remover item do carrinho SEM confirmação (sem popup)

import tkinter as tk
from tkinter import messagebox, ttk, filedialog
from datetime import datetime
import os
import tempfile

# Lê o config_pdv.json (na pasta do projeto)
try:
    from config_pdv import carregar_config_pdv
except Exception:
    carregar_config_pdv = None


def montar_caixa(parent, ler_estoque, escrever_estoque, registrar_venda, carregar_na_tabela):
    caixa = tk.Frame(parent)
    caixa.pack(fill="both", expand=True)

    # ===== Layout 2 colunas (Esquerda: itens | Direita: pagamento) =====
    root = tk.Frame(caixa)
    root.pack(fill="both", expand=True, padx=10, pady=10)

    root.grid_columnconfigure(0, weight=4)
    root.grid_columnconfigure(1, weight=1)
    root.grid_rowconfigure(0, weight=1)

    left = tk.Frame(root)
    left.grid(row=0, column=0, sticky="nsew", padx=(0, 10))
    left.grid_rowconfigure(3, weight=1)

    right = tk.Frame(root)
    right.grid(row=0, column=1, sticky="nsew")
    right.grid_rowconfigure(2, weight=1)
    right.grid_columnconfigure(0, weight=1)

    # ====== Estado ======
    carrinho = {}      # cod -> {"produto": str, "preco": float, "custo": float, "qtd": int}
    estoque_cache = {} # cod -> [Codigo, Produto, Custo, Venda, Qtd]

    # ===================== Config Cupom =====================
    # fallback (caso config_pdv.json não exista / esteja inválido)
    CUPOM_LARGURA = 40  # 40 bom p/ 80mm; 32~36 bom p/ 58mm
    DADOS_LOJA = {
        "nome": "MERCADO EXEMPLO LTDA",
        "cnpj": "00.000.000/0001-00",
        "ie": "ISENTO",
        "endereco": "Rua Exemplo, 123 - Centro",
        "cidade": "Sua Cidade - UF",
        "telefone": "(11) 99999-9999",
        "mensagem": "OBRIGADO! VOLTE SEMPRE :)",
        "mostrar_cupom_nao_fiscal": True,
    }

    def carregar_cfg_loja():
        """Carrega dados do config_pdv.json e atualiza CUPOM_LARGURA / DADOS_LOJA (fallback seguro)."""
        nonlocal CUPOM_LARGURA, DADOS_LOJA
        if carregar_config_pdv is None:
            return
        try:
            cfg = carregar_config_pdv()
            if isinstance(cfg, dict):
                CUPOM_LARGURA = int(cfg.get("cupom_largura", CUPOM_LARGURA))
                loja = cfg.get("loja", {})
                if isinstance(loja, dict):
                    for k, v in loja.items():
                        DADOS_LOJA[k] = v
        except Exception:
            pass

    
    def gerar_texto_cupom(numero_venda, pagamento, desconto, recebido, troco, documento):
        agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        subtotal = total_carrinho()
        total_pagar = max(0.0, subtotal - desconto)

        carregar_cfg_loja()

        linhas = []

        if DADOS_LOJA.get("mostrar_cupom_nao_fiscal", True):
            linhas.append(centralizar("CUPOM NAO FISCAL", CUPOM_LARGURA))

        linhas.append(centralizar(DADOS_LOJA.get("nome", ""), CUPOM_LARGURA))
        linhas.append(centralizar(f"CNPJ: {DADOS_LOJA.get('cnpj', '')}", CUPOM_LARGURA))
        linhas.append(centralizar(f"IE: {DADOS_LOJA.get('ie', '')}", CUPOM_LARGURA))
        linhas.append(centralizar(DADOS_LOJA.get("endereco", ""), CUPOM_LARGURA))
        linhas.append(centralizar(DADOS_LOJA.get("cidade", ""), CUPOM_LARGURA))
        linhas.append(centralizar(f"Fone: {DADOS_LOJA.get('telefone', '')}", CUPOM_LARGURA))

        linhas.append(linha_sep("="))
        linhas.append(f"DATA: {agora}")
        linhas.append(f"VENDA: {numero_venda}")

        documento = (documento or "").strip()
        if documento:
            linhas.append(f"CPF/CNPJ CLIENTE: {documento}")

        linhas.append(linha_sep("-"))
        linhas.append("ITENS")
        linhas.append(linha_sep("-"))

        for cod, item in carrinho.items():
            nome = f"{item['produto']} ({cod})"
            qtd = item["qtd"]
            unit = item["preco"]
            total_item = unit * qtd

            linhas.append(fmt_item_linha1(nome))
            linhas.append(fmt_item_linha2(qtd, unit, total_item))

        linhas.append(linha_sep("-"))
        linhas.append(linha_valor("SUBTOTAL", subtotal))

        if desconto > 0:
            linhas.append(linha_valor("DESCONTO", desconto))

        linhas.append(linha_valor("TOTAL", total_pagar))
        linhas.append(linha_sep("-"))
        linhas.append(f"PAGAMENTO: {pagamento}")

        if pagamento == "Dinheiro":
            linhas.append(linha_valor("RECEBIDO", recebido))
            linhas.append(linha_valor("TROCO", troco))

        linhas.append(linha_sep("="))
        linhas.append(centralizar(DADOS_LOJA.get("mensagem", ""), CUPOM_LARGURA))
        linhas.append(linha_sep("="))

        return "\n".join(linhas)



    # ===================== Preview / impressão =====================
    def imprimir_comprovante_windows(texto: str):
        if os.name != "nt":
            raise RuntimeError("Impressão direta (print) está configurada apenas para Windows.")
        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt", mode="w", encoding="utf-8") as f:
            f.write(texto)
            caminho = f.name
        os.startfile(caminho, "print")

    def salvar_comprovante_dialog(texto: str, sugestao_nome="cupom.txt"):
        caminho = filedialog.asksaveasfilename(
            defaultextension=".txt",
            initialfile=sugestao_nome,
            filetypes=[("Arquivo de texto", "*.txt")],
        )
        if not caminho:
            return
        with open(caminho, "w", encoding="utf-8") as f:
            f.write(texto)
        messagebox.showinfo("Salvo", f"Cupom salvo em:\n{caminho}")

    def abrir_preview_comprovante(parent_widget, texto: str, numero_venda=None):
        win = tk.Toplevel(parent_widget)
        win.title("Pré-visualização do cupom")
        win.transient(parent_widget)
        win.grab_set()
        win.geometry("560x640")
        win.minsize(520, 520)

        topo = ttk.Frame(win, padding=10)
        topo.pack(fill="x")
        ttk.Label(topo, text="Cupom (estilo mercado)", font=("Segoe UI", 12, "bold")).pack(anchor="w")
        if numero_venda is not None:
            ttk.Label(topo, text=f"Venda Nº: {numero_venda}", font=("Segoe UI", 9)).pack(anchor="w", pady=(2, 0))

        centro = ttk.Frame(win, padding=(10, 0, 10, 10))
        centro.pack(fill="both", expand=True)

        txt = tk.Text(centro, wrap="none")
        txt.pack(side="left", fill="both", expand=True)
        txt.configure(font=("Consolas", 10))
        txt.insert("1.0", texto)
        txt.config(state="disabled")

        scroll_y = ttk.Scrollbar(centro, orient="vertical", command=txt.yview)
        scroll_y.pack(side="right", fill="y")
        txt.configure(yscrollcommand=scroll_y.set)

        rodape = ttk.Frame(win, padding=10)
        rodape.pack(fill="x")

        def on_imprimir():
            try:
                imprimir_comprovante_windows(texto)
            except Exception as e:
                messagebox.showerror("Erro ao imprimir", str(e))

        def on_salvar():
            nome = f"cupom_{numero_venda}.txt" if numero_venda else "cupom.txt"
            salvar_comprovante_dialog(texto, sugestao_nome=nome)

        ttk.Button(rodape, text="Salvar TXT", command=on_salvar).pack(side="left")
        ttk.Button(rodape, text="Imprimir", command=on_imprimir).pack(side="left", padx=8)
        ttk.Button(rodape, text="Fechar", command=win.destroy).pack(side="right")

        win.bind("<Escape>", lambda e: win.destroy())
        win.focus_set()

    # ===================== Ações do carrinho =====================
    def adicionar_ao_carrinho():
        codigo = entry_codigo.get().strip()
        qtd_txt = entry_qtd.get().strip()

        if not codigo:
            messagebox.showerror("Erro", "Informe o código do produto.")
            entry_codigo.focus()
            return

        qtd = 1
        if qtd_txt:
            try:
                qtd = int(qtd_txt)
                if qtd <= 0:
                    raise ValueError
            except ValueError:
                messagebox.showerror("Erro", "Quantidade deve ser um inteiro > 0.")
                entry_qtd.focus()
                return

        recarregar_estoque_cache()
        produto = buscar_produto_por_codigo(codigo)
        if not produto:
            messagebox.showerror("Erro", "Produto não encontrado pelo código.")
            entry_codigo.focus()
            return

        cod, nome, custo_txt, venda_txt, estoque_txt = produto
        estoque_atual = int(estoque_txt)
        preco_venda = parse_preco(venda_txt)
        custo = parse_preco(custo_txt)

        qtd_no_carrinho = carrinho.get(cod, {}).get("qtd", 0)
        if qtd_no_carrinho + qtd > estoque_atual:
            messagebox.showerror("Erro", "Quantidade no carrinho excede o estoque disponível.")
            return

        if cod in carrinho:
            carrinho[cod]["qtd"] += qtd
        else:
            carrinho[cod] = {"produto": nome, "preco": preco_venda, "custo": custo, "qtd": qtd}

        recarregar_carrinho()
        entry_codigo.delete(0, tk.END)
        entry_qtd.delete(0, tk.END)
        entry_codigo.focus()

    # ✅ Remover SEM confirmação
    def remover_item_por_codigo(cod):
        if cod not in carrinho:
            return
        carrinho.pop(cod, None)
        recarregar_carrinho()

    def remover_item_selecionado():
        sel = tabela_car.selection()
        if not sel:
            messagebox.showwarning("Atenção", "Selecione um item no carrinho.")
            return
        cod = tabela_car.item(sel[0], "values")[0]
        remover_item_por_codigo(cod)

    def limpar_carrinho(confirmar=True):
        if confirmar and carrinho:
            if not messagebox.askyesno("Confirmar", "Deseja limpar o carrinho?"):
                return
        carrinho.clear()
        recarregar_carrinho()
        entry_codigo.focus()

    def aumentar_qtd():
        sel = tabela_car.selection()
        if not sel:
            messagebox.showwarning("Atenção", "Selecione um item no carrinho.")
            return
        cod = tabela_car.item(sel[0], "values")[0]
        recarregar_estoque_cache()
        produto = buscar_produto_por_codigo(cod)
        if not produto:
            return
        estoque_atual = int(produto[4])
        if carrinho[cod]["qtd"] + 1 > estoque_atual:
            messagebox.showerror("Erro", "Não há estoque suficiente.")
            return
        carrinho[cod]["qtd"] += 1
        recarregar_carrinho()

    def diminuir_qtd():
        sel = tabela_car.selection()
        if not sel:
            messagebox.showwarning("Atenção", "Selecione um item no carrinho.")
            return
        cod = tabela_car.item(sel[0], "values")[0]
        carrinho[cod]["qtd"] -= 1
        if carrinho[cod]["qtd"] <= 0:
            carrinho.pop(cod, None)
        recarregar_carrinho()

    def editar_qtd_popup(cod):
        top = tk.Toplevel(caixa)
        top.title("Editar quantidade")
        top.geometry("300x150")
        top.resizable(False, False)

        tk.Label(top, text=f"{carrinho[cod]['produto']}", font=("Arial", 10)).pack(pady=(10, 6))
        frame = tk.Frame(top)
        frame.pack(pady=6)

        tk.Label(frame, text="Nova qtd:").grid(row=0, column=0, padx=6, pady=6)
        entry = tk.Entry(frame, width=10)
        entry.grid(row=0, column=1, padx=6, pady=6)
        entry.insert(0, str(carrinho[cod]["qtd"]))
        entry.focus()

        def salvar_nova_qtd():
            try:
                nova = int(entry.get().strip())
                if nova <= 0:
                    raise ValueError
            except ValueError:
                messagebox.showerror("Erro", "Digite um inteiro > 0.")
                return

            recarregar_estoque_cache()
            produto = buscar_produto_por_codigo(cod)
            if produto:
                estoque_atual = int(produto[4])
                if nova > estoque_atual:
                    messagebox.showerror("Erro", "Quantidade maior que o estoque disponível.")
                    return

            carrinho[cod]["qtd"] = nova
            recarregar_carrinho()
            top.destroy()

        tk.Button(top, text="Salvar", width=10, command=salvar_nova_qtd).pack(pady=8)
        top.bind("<Return>", lambda e: salvar_nova_qtd())
        top.grab_set()

    def duplo_clique_carrinho(event=None):
        sel = tabela_car.selection()
        if not sel:
            return
        cod = tabela_car.item(sel[0], "values")[0]
        if cod in carrinho:
            editar_qtd_popup(cod)

    def clique_carrinho(event):
        item_id = tabela_car.identify_row(event.y)
        col = tabela_car.identify_column(event.x)
        if not item_id:
            return
        if col == "#6":  # coluna Ação
            cod = tabela_car.item(item_id, "values")[0]
            remover_item_por_codigo(cod)

    
    # ===================== Finalizar venda (gera cupom) =====================
    def finalizar_venda():
        if not carrinho:
            messagebox.showwarning("Atenção", "Carrinho vazio.")
            return

        recarregar_estoque_cache()

        for cod, item in carrinho.items():
            prod = buscar_produto_por_codigo(cod)

            if not prod:
                messagebox.showerror(
                    "Erro",
                    f"Produto {cod} não existe mais no estoque."
                )
                return

            qtd_disp = int(prod[4])

            if item["qtd"] > qtd_disp:
                messagebox.showerror(
                    "Erro",
                    f"Estoque insuficiente para {item['produto']}."
                )
                return

        pagamento = cb_pag.get()

        try:
            desconto = parse_preco(entry_desc.get())
        except Exception:
            messagebox.showerror("Erro", "Desconto inválido.")
            return

        if desconto < 0:
            messagebox.showerror("Erro", "Desconto não pode ser negativo.")
            return

        total_bruto = total_carrinho()
        total_liq = max(0.0, total_bruto - desconto)

        try:
            recebido = parse_preco(entry_rec.get())
        except Exception:
            recebido = 0.0

        if pagamento == "Dinheiro":
            if recebido < total_liq:
                messagebox.showerror(
                    "Erro",
                    "Valor recebido menor que o total com desconto."
                )
                return

            troco = recebido - total_liq
        else:
            troco = 0.0

        # Captura o CPF/CNPJ informado pelo cliente
        documento = entry_documento.get().strip()

        numero_venda = gerar_numero_venda()

        # Atualiza o estoque e registra os itens vendidos
        for cod, item in carrinho.items():
            linha = estoque_cache[cod]
            qtd_disp = int(linha[4])
            linha[4] = str(qtd_disp - item["qtd"])

            total_item = item["preco"] * item["qtd"]

            registrar_venda(
                cod,
                item["produto"],
                brl(item["preco"]),
                str(item["qtd"]),
                brl(total_item)
            )

        escrever_estoque(list(estoque_cache.values()))

        # Gera o comprovante com o CPF/CNPJ do cliente
        texto = gerar_texto_cupom(
            numero_venda,
            pagamento,
            desconto,
            recebido,
            troco,
            documento
        )

        abrir_preview_comprovante(
            caixa,
            texto,
            numero_venda=numero_venda
        )

        # Limpa o carrinho após finalizar
        carrinho.clear()
        recarregar_carrinho()

        # Limpa o desconto
        entry_desc.delete(0, tk.END)
        entry_desc.insert(0, "0")

        # Limpa o CPF/CNPJ do cliente
        entry_documento.delete(0, tk.END)

        # Restaura a forma de pagamento
        cb_pag.set("Pix")

        entry_rec.config(state="normal")
        entry_rec.delete(0, tk.END)
        entry_rec.insert(0, "0")
        entry_rec.config(state="disabled")

        entry_codigo.focus()

    # ===================== ESQUERDA (input + carrinho) =====================
    header = tk.Label(left, text="Caixa (PDV)", font=("Arial", 14))
    header.grid(row=0, column=0, sticky="w", pady=(0, 8))

    frame_busca = tk.Frame(left)
    frame_busca.grid(row=1, column=0, sticky="ew", pady=(0, 8))
    frame_busca.grid_columnconfigure(1, weight=1)

    tk.Label(frame_busca, text="Código:").grid(row=0, column=0, padx=6, pady=4, sticky="e")
    entry_codigo = tk.Entry(frame_busca, width=28, font=("Arial", 12))
    entry_codigo.grid(row=0, column=1, padx=6, pady=4, sticky="w")

    tk.Label(frame_busca, text="Qtd:").grid(row=0, column=2, padx=6, pady=4, sticky="e")
    entry_qtd = tk.Entry(frame_busca, width=8, font=("Arial", 12))
    entry_qtd.grid(row=0, column=3, padx=6, pady=4, sticky="w")

    btn_add = tk.Button(frame_busca, text="Adicionar (Enter)", width=16, command=adicionar_ao_carrinho)
    btn_add.grid(row=0, column=4, padx=6, pady=4)

    tk.Label(left, text="Carrinho").grid(row=2, column=0, sticky="w", pady=(4, 4))

    colunas_car = ("Codigo", "Produto", "Preço Unit.", "Qtd", "Subtotal", "Ação")
    tabela_car = ttk.Treeview(left, columns=colunas_car, show="headings", height=16)

    for col in colunas_car:
        tabela_car.heading(col, text=col)
        tabela_car.column(col, anchor="center", width=130)

    tabela_car.column("Produto", width=320)
    tabela_car.column("Ação", width=60)
    tabela_car.grid(row=3, column=0, sticky="nsew")

    frame_car_btn = tk.Frame(left)
    frame_car_btn.grid(row=4, column=0, sticky="ew", pady=8)

    tk.Button(frame_car_btn, text="+ Qtd", width=10, command=aumentar_qtd).pack(side="left", padx=6)
    tk.Button(frame_car_btn, text="- Qtd", width=10, command=diminuir_qtd).pack(side="left", padx=6)
    tk.Button(frame_car_btn, text="Remover (Del)", width=14, command=remover_item_selecionado).pack(side="left", padx=6)
    tk.Button(frame_car_btn, text="Limpar", width=12, command=lambda: limpar_carrinho(confirmar=True)).pack(side="left", padx=6)

    
    # ===================== DIREITA (sidebar pagamento) =====================
    box_pag = tk.LabelFrame(right, text="Pagamento", padx=10, pady=10)
    box_pag.grid(row=0, column=0, sticky="new", pady=(0, 10))

    # CPF/CNPJ do consumidor
    tk.Label(box_pag, text="CPF/CNPJ:").grid(
        row=0, column=0, sticky="e", padx=6, pady=6
    )
    entry_documento = tk.Entry(box_pag, width=20)
    entry_documento.grid(row=0, column=1, sticky="w", padx=6, pady=6)

    # Forma de pagamento
    tk.Label(box_pag, text="Forma:").grid(
        row=1, column=0, sticky="e", padx=6, pady=6
    )
    cb_pag = ttk.Combobox(
        box_pag,
        values=["Pix", "Dinheiro", "Cartão Crédito", "Cartão Débito"],
        state="readonly",
        width=18
    )
    cb_pag.grid(row=1, column=1, sticky="w", padx=6, pady=6)
    cb_pag.set("Pix")

    # Desconto
    tk.Label(box_pag, text="Desconto (R$):").grid(
        row=2, column=0, sticky="e", padx=6, pady=6
    )
    entry_desc = tk.Entry(box_pag, width=12)
    entry_desc.grid(row=2, column=1, sticky="w", padx=6, pady=6)
    entry_desc.insert(0, "0")

    # Recebido
    tk.Label(box_pag, text="Recebido (dinheiro):").grid(
        row=3, column=0, sticky="e", padx=6, pady=6
    )
    entry_rec = tk.Entry(box_pag, width=12)
    entry_rec.grid(row=3, column=1, sticky="w", padx=6, pady=6)
    entry_rec.insert(0, "0")
    entry_rec.config(state="disabled")
