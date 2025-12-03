import re
from datetime import datetime
from decimal import Decimal

# ============================================================
# 1. DETECTAR LAYOUT
# ============================================================

def detectar_layout_ci_gfip(texto: str) -> str:
    """
    Detecta automaticamente o layout do CI GFIP.
    Retorna: "modelo_1", "modelo_2" ou "layout_nao_identificado".
    """

    if "COMPETÊNCIA" in texto and "FPAS" in texto:
        return "modelo_1"

    if "tomador" in texto.lower() and "categoria" in texto.lower():
        return "modelo_2"

    return "layout_nao_identificado"

# ============================================================
# 2. FUNÇÕES AUXILIARES
# ============================================================

def so_numeros(valor: str | None) -> str:
    return re.sub(r"\D", "", valor or "")

def parse_valor(valor: str | None):
    """
    Converte valores como '1.234,56' para Decimal.
    """
    if not valor:
        return None
    v = valor.replace(".", "").replace(",", ".")
    try:
        return Decimal(v)
    except:
        return None

# ============================================================
# 3. PARSE DO CABEÇALHO
# ============================================================

def parse_cabecalho(texto: str) -> dict:
    """
    Extrai NIT, Nome, Nome da Mãe, Data de Nascimento e CPF.
    Esses são os campos padrão DO RELATÓRIO.
    🚨 Profissão e Estado NÃO são preenchidos aqui.
       Eles vêm do Lovable e serão adicionados pelo main.py
    """
    cab = {}

    # --- NIT ---
    m = re.search(r"NIT[:\s]*([\d\.\-]+)", texto, re.IGNORECASE)
    if m:
        cab["nit"] = so_numeros(m.group(1))

    # --- Nome ---
    m = re.search(r"Nome[:\s]*([A-ZÇÃÕÂÉÊÍÓÚ ]+)", texto)
    if m:
        cab["nome"] = m.group(1).strip()

    # --- Nome da Mãe ---
    m = re.search(r"Mãe[:\s]*([A-ZÇÃÕÂÉÊÍÓÚ ]+)", texto)
    if m:
        cab["nome_mae"] = m.group(1).strip()

    # --- Data de Nascimento ---
    m = re.search(r"Nasc[:\s]*(\d{2}/\d{2}/\d{4})", texto)
    if m:
        nasc = datetime.strptime(m.group(1), "%d/%m/%Y").date()
        cab["data_nascimento"] = str(nasc)

    # --- CPF ---
    m = re.search(r"CPF[:\s]*([\d\.\-]+)", texto)
    if m:
        cab["cpf"] = so_numeros(m.group(1))

    # ⚠️ NÃO adicionamos profissão e estado aqui!
    # Eles serão aplicados no main.py após o parser.
    # cab["profissao"] = ""
    # cab["estado"] = ""

    return cab

# ============================================================
# 4. PARSE DAS LINHAS
# ============================================================

def parse_linhas(texto: str) -> list:
    """
    Parser universal simplificado — extrai blocos de linhas do CI GFIP.
    Cada linha do modelo 2 geralmente já está estruturada.
    """
    linhas = []
    padrao = re.compile(
        r"(\d{2}/\d{4}).*?(\d{7,14}).*?(\d{3}).*?(\d{3}).*?([0-9\.\,]+).*?([0-9\.\,]+)",
        re.MULTILINE
    )

    for m in padrao.finditer(texto):
        comp = m.group(1)
        comp_dt = None
        ano = None
        mes = None

        try:
            mes, ano = comp.split("/")
            comp_dt = f"{ano}-{mes.zfill(2)}-01"
        except:
            pass

        linhas.append({
            "fonte": "GFIP",
            "competencia_literal": comp,
            "competencia_date": comp_dt,
            "competencia_ano": int(ano) if ano else None,
            "competencia_mes": int(mes) if mes else None,
            "documento_tomador": m.group(2),
            "documento_tomador_tipo": "CNPJ_RAIZ",
            "categoria_codigo": m.group(3),
            "fpas": m.group(4),
            "remuneracao_literal": m.group(5),
            "remuneracao": parse_valor(m.group(5)),
            "valor_retido_literal": m.group(6),
            "valor_retido": parse_valor(m.group(6)),
            "extemporaneo_literal": None,
            "extemporaneo": None,
        })

    return linhas

# ============================================================
# 5. PARSER PRINCIPAL
# ============================================================

def parse_ci_gfip(texto: str) -> dict:
    """
    Parser principal que retorna:
    - cabecalho (SEM profissão/estado)
    - linhas
    - erro, se houver
    """

    layout = detectar_layout_ci_gfip(texto)
    if layout == "layout_nao_identificado":
        return {"erro": "layout_nao_identificado"}

    cab = parse_cabecalho(texto)
    linhas = parse_linhas(texto)

    return {
        "cabecalho": cab,
        "linhas": linhas,
        "layout_detectado": layout,
    }
