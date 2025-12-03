import re
from datetime import datetime
from decimal import Decimal

# =====================================================================
#  NORMALIZAÇÕES
# =====================================================================

def so_numeros(valor: str | None) -> str:
    if not valor:
        return ""
    return re.sub(r"\D", "", valor)

def normalizar_competencia(comp_str: str | None):
    if not comp_str:
        return None, ""
    try:
        dt = datetime.strptime(comp_str.strip(), "%m/%Y")
        return dt.strftime("%Y-%m-01"), comp_str
    except:
        return None, comp_str

def normalizar_data(ddmmaaaa: str | None):
    if not ddmmaaaa:
        return None, ""
    try:
        dt = datetime.strptime(ddmmaaaa.strip(), "%d/%m/%Y")
        return dt.strftime("%Y-%m-%d"), ddmmaaaa
    except:
        return None, ddmmaaaa

def normalizar_moeda(valor: str | None):
    if not valor:
        return None, ""
    bruto = valor.strip()
    txt = bruto.replace("R$", "").replace(".", "").replace(",", ".")
    try:
        return float(Decimal(txt)), bruto
    except:
        return None, bruto

def normalizar_documento_tomador(valor: str | None):
    if not valor:
        return "", "DESCONHECIDO"
    numeros = so_numeros(valor)

    if len(numeros) == 14:
        return numeros, "CNPJ_COMPLETO"
    if len(numeros) == 12:
        return numeros, "CEI"
    if len(numeros) == 11:
        return numeros, "CPF"
    if len(numeros) <= 8:
        return numeros.zfill(8), "CNPJ_RAIZ"
    if 9 <= len(numeros) <= 13:
        return numeros[:8].zfill(8), "CNPJ_RAIZ"

    return numeros, "DESCONHECIDO"

# =====================================================================
#     PARSER UNIVERSAL HÍBRIDO – MODELO 2 (MÁXIMA ROBUSTEZ)
# =====================================================================

def parse_ci_gfip_modelo_2(texto: str) -> dict:
    """
    PARSER HÍBRIDO – aceita qualquer tabela CI GFIP/eSocial/INSS.
    Funciona mesmo quando pdfplumber separa ou cola colunas.
    """

    # --------------------------------------------------------
    # EXTRAIR CABEÇALHO DO FILIADO
    # --------------------------------------------------------

    cabecalho = {
        "nit": None,
        "nome": None,
        "nome_mae": None,
        "data_nascimento": None,
        "cpf": None,
    }

    m_nit = re.search(r"Nit[: ]+([\d\.\-]+)", texto, re.IGNORECASE)
    if m_nit:
        cabecalho["nit"] = so_numeros(m_nit.group(1))

    m_nome = re.search(
        r"Nome[: ]+(.+?)\s+Data de Nascimento[: ]+[0-9]{2}/[0-9]{2}/[0-9]{4}",
        texto,
        re.IGNORECASE | re.DOTALL,
    )
    if m_nome:
        cabecalho["nome"] = m_nome.group(1).strip()

    m_dn = re.search(
        r"Data de Nascimento[: ]+([0-9]{2}/[0-9]{2}/[0-9]{4})",
        texto,
        re.IGNORECASE,
    )
    if m_dn:
        cabecalho["data_nascimento"] = normalizar_data(m_dn.group(1))[0]

    m_mae = re.search(
        r"Nome da M[ãa]e[: ]+(.+?)(?:\s+CPF[: ]|Página\s+\d+ de \d+)",
        texto,
        re.IGNORECASE | re.DOTALL,
    )
    if m_mae:
        cabecalho["nome_mae"] = m_mae.group(1).strip()

    m_cpf = re.search(r"CPF[: ]+([\d\.\-]+)", texto, re.IGNORECASE)
    if m_cpf:
        cabecalho["cpf"] = m_cpf.group(1).strip()

    # --------------------------------------------------------
    # ETAPA 1 – CAPTURAR TODAS AS LINHAS QUE CONTÊM GFIP/ESOCIAL
    # --------------------------------------------------------

    linhas_brutas = []
    for line in texto.splitlines():
        l = line.strip()
        if l.startswith("GFIP") or l.upper().startswith("ESOCIAL"):
            linhas_brutas.append(l)

    # --------------------------------------------------------
    # ETAPA 2 – RECONSTRUIR LINHAS QUE ESTÃO QUEBRADAS
    # --------------------------------------------------------

    linhas_unificadas = []
    buffer = ""

    for l in linhas_brutas:
        if buffer == "":
            buffer = l
            continue

        if re.match(r"^\S+$", l) and not re.search(r"\d{2}/\d{4}", l):
            buffer += " " + l
        elif re.match(r"^\d{2}/\d{4}$", l):
            buffer += " " + l
        else:
            linhas_unificadas.append(buffer)
            buffer = l

    if buffer:
        linhas_unificadas.append(buffer)

    # --------------------------------------------------------
    # ETAPA 3 – PROCESSAR CADA LINHA (AGORA UNIFICADA)
    # --------------------------------------------------------

    linhas = []

    for raw in linhas_unificadas:
        partes = raw.split()
        if len(partes) < 10:
            continue

        # Mapear campos
        fonte = partes[0]
        numero_documento = partes[1]
        nit_linha = partes[2]
        competencia_lit = partes[3]
        documento_raw = partes[4]
        fpas = partes[5]
        categoria = partes[6]
        codigo_gfip = partes[7]
        data_envio_lit = partes[8]

        tipo_tokens = partes[9:-3]
        remuneracao_txt = partes[-3]
        valor_retido_txt = partes[-2]
        extemporaneo_txt = partes[-1]

        tipo_remuneracao = " ".join(tipo_tokens).strip()

        # Normalização
        comp_date, comp_literal = normalizar_competencia(competencia_lit)
        data_envio_date, data_envio_literal = normalizar_data(data_envio_lit)
        remuneracao, remuneracao_literal = normalizar_moeda(remuneracao_txt)
        valor_retido, valor_retido_literal = normalizar_moeda(valor_retido_txt)

        doc_tomador, doc_tomador_tipo = normalizar_documento_tomador(documento_raw)

        extemp_literal = extemporaneo_txt.strip()
        extemporaneo_bool = extemp_literal.lower().startswith("s")

        linhas.append(
            {
                "fonte": fonte.upper(),
                "numero_documento": numero_documento,
                "nit": so_numeros(nit_linha) or cabecalho.get("nit"),
                "competencia_literal": comp_literal,
                "competencia_date": comp_date,
                "competencia_ano": int(comp_date.split("-")[0]) if comp_date else None,
                "competencia_mes": int(comp_date.split("-")[1]) if comp_date else None,
                "documento_tomador": doc_tomador,
                "documento_tomador_tipo": doc_tomador_tipo,
                "fpas": fpas,
                "categoria_codigo": categoria,
                "codigo_gfip": codigo_gfip,
                "data_envio_literal": data_envio_literal,
                "data_envio_date": data_envio_date,
                "tipo_remuneracao": tipo_remuneracao or None,
                "remuneracao_literal": remuneracao_literal,
                "remuneracao": remuneracao,
                "valor_retido_literal": valor_retido_literal,
                "valor_retido": valor_retido,
                "extemporaneo_literal": extemp_literal,
                "extemporaneo": extemporaneo_bool,
            }
        )

    return {
        "cabecalho": cabecalho,
        "linhas": linhas,
        "total_linhas": len(linhas),
        "layout": "modelo_2",
    }
