import re
from datetime import datetime
from decimal import Decimal

# =====================================================================
#  FUNÇÕES DE NORMALIZAÇÃO
# =====================================================================

def so_numeros(valor: str | None) -> str:
    return re.sub(r"\D", "", valor or "")


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
    texto = bruto.replace("R$", "").replace(".", "").replace(",", ".")
    try:
        return float(Decimal(texto)), bruto
    except:
        return None, bruto


def normalizar_documento_tomador(valor: str | None):
    if not valor:
        return "", "DESCONHECIDO"

    numeros = so_numeros(valor)

    # CNPJ COMPLETO
    if len(numeros) == 14:
        return numeros, "CNPJ_COMPLETO"

    # CEI
    if len(numeros) == 12:
        return numeros, "CEI"

    # CPF
    if len(numeros) == 11:
        return numeros, "CPF"

    # CNPJ RAIZ (até 8 dígitos)
    if len(numeros) <= 8:
        return numeros.zfill(8), "CNPJ_RAIZ"

    # CNPJ raiz + filial sem DV
    if 9 <= len(numeros) <= 13:
        return numeros[:8].zfill(8), "CNPJ_RAIZ"

    return numeros, "DESCONHECIDO"


# =====================================================================
#  PARSER UNIVERSAL HÍBRIDO – CI GFIP / eSocial / INSS
# =====================================================================

def parse_ci_gfip_modelo_2(texto: str) -> dict:
    """
    Parser definitivo, robusto e tolerante a qualquer PDF de CI GFIP/eSocial/INSS.
    """

    # --------------------------------------------------------
    # CABEÇALHO – robusto, sem capturar a tabela
    # --------------------------------------------------------

    cabecalho = {
        "nit": None,
        "nome": None,
        "nome_mae": None,
        "data_nascimento": None,
        "cpf": None,
    }

    # Nit
    m_nit = re.search(r"Nit[: ]+([\d\.\-]+)", texto, re.IGNORECASE)
    if m_nit:
        cabecalho["nit"] = so_numeros(m_nit.group(1))

    # Nome
    m_nome = re.search(
        r"Nome[: ]+([A-ZÇÃÂÉÊÍÓÔÚà-ú ]+?)\s+Data de Nascimento",
        texto,
        re.IGNORECASE,
    )
    if m_nome:
        cabecalho["nome"] = m_nome.group(1).strip()

    # Data de nascimento
    m_dn = re.search(r"Data de Nascimento[: ]+(\d{2}/\d{2}/\d{4})", texto)
    if m_dn:
        cabecalho["data_nascimento"] = normalizar_data(m_dn.group(1))[0]

    # Nome da mãe – agora PARA antes de qualquer cabeçalho de tabela
    m_mae = re.search(
        r"Nome da M[ãa]e[: ]+(.+?)(?:\n\s*Fonte|\n\s*GFIP|\n\s*ESOCIAL|\nPágina)",
        texto,
        re.IGNORECASE | re.DOTALL,
    )
    if m_mae:
        cabecalho["nome_mae"] = m_mae.group(1).strip()

    # CPF
    m_cpf = re.search(r"CPF[: ]+([\d\.\-]+)", texto, re.IGNORECASE)
    if m_cpf:
        cabecalho["cpf"] = m_cpf.group(1).strip()

    # --------------------------------------------------------
    # CAPTURA DAS LINHAS DA TABELA
    # --------------------------------------------------------

    linhas_brutas = []
    for l in texto.splitlines():
        s = l.strip()
        if s.startswith("GFIP") or s.upper().startswith("ESOCIAL"):
            linhas_brutas.append(s)

    # --------------------------------------------------------
    # RECONSTRUÇÃO DE LINHAS QUEBRADAS
    # --------------------------------------------------------

    linhas_unificadas = []
    buffer = ""

    for l in linhas_brutas:
        if not buffer:
            buffer = l
            continue

        if not re.search(r"\d{2}/\d{4}", buffer):
            buffer += " " + l
            continue

        if re.match(r"^[A-Z]{2,10}$", l):
            buffer += " " + l
            continue

        linhas_unificadas.append(buffer)
        buffer = l

    if buffer:
        linhas_unificadas.append(buffer)

    # --------------------------------------------------------
    # PROCESSAMENTO DO REGISTRO
    # --------------------------------------------------------

    linhas = []

    for raw_line in linhas_unificadas:
        partes = raw_line.split()
        if len(partes) < 10:
            continue

        fonte = partes[0]

        if fonte.upper() == "GFIP":
            # GFIP segue o layout clássico
            numero_documento = partes[1]
            nit_linha = partes[2]
            competencia_lit = partes[3]
            doc_raw = partes[4]
            fpas = partes[5]
            categoria = partes[6]
            codigo_gfip = partes[7]
            data_envio_lit = partes[8]

            tipo_tokens = partes[9:-3]
            remuneracao_txt = partes[-3]
            valor_retido_txt = partes[-2]
            extemp_txt = partes[-1]

        else:
            # ----------------------------------------
            # ESOCIAL – layout diferente
            # ----------------------------------------
            numero_documento = partes[1]
            nit_linha = partes[2]
            competencia_lit = partes[3]
            doc_raw = partes[4]
            fpas = partes[5]
            categoria = partes[6]
            # No eSocial, o próximo campo é SIEMPRE a DATA DE ENVIO
            data_envio_lit = partes[7]
            # Depois o tipo de remuneração literal (NORMAL)
            tipo_tokens = [partes[8]]
            remuneracao_txt = partes[-3]
            valor_retido_txt = partes[-2]
            extemp_txt = partes[-1]
            codigo_gfip = categoria  # eSocial não usa código GFIP clássico

        # Normalizações
        comp_date, comp_literal = normalizar_competencia(competencia_lit)
        data_envio_date, data_envio_literal = normalizar_data(data_envio_lit)
        remuneracao, remuneracao_literal = normalizar_moeda(remuneracao_txt)
        valor_retido, valor_retido_literal = normalizar_moeda(valor_retido_txt)

        doc_tomador, doc_tomador_tipo = normalizar_documento_tomador(doc_raw)

        tipo_remuneracao = " ".join(tipo_tokens).strip()
        extemp_literal = extemp_txt.strip()
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
