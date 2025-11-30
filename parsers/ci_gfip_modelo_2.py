import re
from datetime import datetime
from decimal import Decimal

# =====================================================================
#  FUNÇÕES DE NORMALIZAÇÃO
# =====================================================================

def so_numeros(valor: str | None) -> str:
    if not valor:
        return ""
    return re.sub(r"\D", "", valor)


def normalizar_competencia(comp_str: str | None) -> tuple[str | None, str]:
    """
    Recebe um literal como '07/2023' e devolve:
    - data ISO (ex: "2023-07-01")
    - literal original
    """
    if not comp_str:
        return None, ""
    try:
        dt = datetime.strptime(comp_str.strip(), "%m/%Y")
        return dt.strftime("%Y-%m-01"), comp_str
    except:
        return None, comp_str


def normalizar_data(ddmmaaaa: str | None) -> tuple[str | None, str]:
    if not ddmmaaaa:
        return None, ""
    try:
        dt = datetime.strptime(ddmmaaaa.strip(), "%d/%m/%Y")
        return dt.strftime("%Y-%m-%d"), ddmmaaaa
    except:
        return None, ddmmaaaa


def normalizar_moeda(valor: str | None) -> tuple[float | None, str]:
    if not valor:
        return None, ""
    bruto = valor.strip()
    txt = bruto.replace("R$", "").replace(".", "").replace(",", ".")
    try:
        return float(Decimal(txt)), bruto
    except:
        return None, bruto


def normalizar_documento_tomador(valor: str | None) -> tuple[str, str]:
    """
    Retorna (documento, tipo):
      - CNPJ_COMPLETO (14 dígitos)
      - CEI (12)
      - CPF (11)
      - CNPJ_RAIZ (≤ 8 ou truncado)
    """
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
#  PARSER DO MODELO 2 – CONDENSADO / eSOCIAL / INSS
# =====================================================================

def parse_ci_gfip_modelo_2(texto: str) -> dict:
    # --------------------------------------------------------
    # CABEÇALHO – super simples para este layout
    # --------------------------------------------------------

    cabecalho = {
        "nit": None,
        "nome": None,
        "nome_mae": None,
        "data_nascimento": None,
        "cpf": None,
    }

    m_nome = re.search(r"Nome[: ]+(.+)", texto)
    if m_nome:
        cabecalho["nome"] = m_nome.group(1).strip()

    m_cpf = re.search(r"CPF[: ]+([\d\.\-]+)", texto)
    if m_cpf:
        cabecalho["cpf"] = m_cpf.group(1).strip()

    m_dn = re.search(r"Data de Nascimento[: ]+([0-9]{2}/[0-9]{2}/[0-9]{4})", texto)
    if m_dn:
        cabecalho["data_nascimento"] = normalizar_data(m_dn.group(1))[0]

    m_nit = re.search(r"NIT[: ]+([\d\.]+)", texto)
    if m_nit:
        cabecalho["nit"] = so_numeros(m_nit.group(1))

    # --------------------------------------------------------
    # TABELA – linhas em formato condensado
    # Cada PDF tem pequenas variações, então regex bem robusta
    # --------------------------------------------------------

    linhas = []

    padrao = re.compile(
        r"(?P<competencia>\d{2}/\d{4})\s+"
        r"(?P<documento>[\d\.\-\/]+)\s+"
        r"(?P<fpas>\d{3})\s+"
        r"(?P<categoria>\d+)\s+"
        r"(?P<codigo>\d+)\s+"
        r"(?P<data_envio>\d{2}/\d{2}/\d{4})\s+"
        r"(?P<remuneracao>[\d\.\,]+)\s+"
        r"(?P<retido>[\d\.\,]+)\s+"
        r"(?P<extemporaneo>Sim|Não)",
        re.IGNORECASE
    )

    for m in padrao.finditer(texto):
        comp_date, comp_lit = normalizar_competencia(m.group("competencia"))
        data_envio_date, data_envio_lit = normalizar_data(m.group("data_envio"))
        remuneracao, remuneracao_lit = normalizar_moeda(m.group("remuneracao"))
        retido, retido_lit = normalizar_moeda(m.group("retido"))
        doc, doc_tipo = normalizar_documento_tomador(m.group("documento"))

        linhas.append({
            "fonte": "CONDENSADO",
            "numero_documento": None,
            "nit": cabecalho.get("nit"),
            "competencia_literal": comp_lit,
            "competencia_date": comp_date,
            "documento_tomador": doc,
            "documento_tomador_tipo": doc_tipo,
            "fpas": m.group("fpas"),
            "categoria_codigo": m.group("categoria"),
            "codigo_gfip": m.group("codigo"),
            "data_envio_literal": data_envio_lit,
            "data_envio_date": data_envio_date,
            "tipo_remuneracao": None,
            "remuneracao_literal": remuneracao_lit,
            "remuneracao": remuneracao,
            "valor_retido_literal": retido_lit,
            "valor_retido": retido,
            "extemporaneo_literal": m.group("extemporaneo"),
            "extemporaneo": 1 if m.group("extemporaneo").lower() == "sim" else 0,
        })

    return {
        "cabecalho": cabecalho,
        "linhas": linhas,
        "total_linhas": len(linhas),
        "layout": "modelo_2",
    }
