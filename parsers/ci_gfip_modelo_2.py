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


def normalizar_competencia(comp_str: str | None):
    """
    Aceita '07/2023' ou '07-2023' e devolve:
    - data ISO (ex: "2023-07-01")
    - literal original
    """
    if not comp_str:
        return None, ""
    comp_str = comp_str.strip()
    for fmt in ("%m/%Y", "%m-%Y"):
        try:
            dt = datetime.strptime(comp_str, fmt)
            return dt.strftime("%Y-%m-01"), comp_str
        except Exception:
            continue
    return None, comp_str


def normalizar_data(ddmmaaaa: str | None):
    """
    Aceita '31/12/2020' ou '31-12-2020' e devolve:
    - data ISO (ex: "2020-12-31")
    - literal original
    """
    if not ddmmaaaa:
        return None, ""
    ddmmaaaa = ddmmaaaa.strip()
    for fmt in ("%d/%m/%Y", "%d-%m-%Y"):
        try:
            dt = datetime.strptime(ddmmaaaa, fmt)
            return dt.strftime("%Y-%m-%d"), ddmmaaaa
        except Exception:
            continue
    return None, ddmmaaaa


def normalizar_moeda(valor: str | None):
    """
    Converte '1.234,56' → (1234.56, '1.234,56')
    """
    if not valor:
        return None, ""
    bruto = valor.strip()
    txt = bruto.replace("R$", "").replace(".", "").replace(",", ".")
    try:
        return float(Decimal(txt)), bruto
    except Exception:
        return None, bruto


def normalizar_documento_tomador(valor: str | None):
    """
    Retorna (documento, tipo):
      - CNPJ_COMPLETO (14 dígitos)
      - CEI (12)
      - CPF (11)
      - CNPJ_RAIZ (<= 8 ou truncado)
      - DESCONHECIDO
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
        # raiz de CNPJ (preenche à esquerda se for menor)
        return numeros.zfill(8), "CNPJ_RAIZ"

    if 9 <= len(numeros) <= 13:
        # muito comum vir raiz + filial sem DV
        return numeros[:8].zfill(8), "CNPJ_RAIZ"

    return numeros, "DESCONHECIDO"


# =====================================================================
#  PARSER DO MODELO 2 – CONSULTA VALORES CI GFIP/eSocial/INSS
# =====================================================================

def parse_ci_gfip_modelo_2(texto: str) -> dict:
    """
    Parser específico para os layouts:
    'CONSULTA VALORES CI GFIP/eSocial/INSS'

    Estrutura da tabela (conforme cabeçalho do relatório):

        Fonte | NIT | Remuneração | Competência | CNPJ/CPF/CEI | FPAS |
        Categoria | Código GFIP | Data de Envio | Valor Retido | Extemp.

    Lê:
      - Cabeçalho do filiado (Nit, Nome, Nome da Mãe, Data de Nascimento, CPF)
      - Todas as linhas da tabela (GFIP + eSOCIAL) em múltiplas páginas

    Retorna dict com:
      - cabecalho
      - linhas
      - total_linhas
      - layout = 'modelo_2'
    """

    # --------------------------------------------------------
    # CABEÇALHO – extração robusta
    # --------------------------------------------------------
    cabecalho: dict[str, str | None] = {
        "nit": None,
        "nome": None,
        "nome_mae": None,
        "data_nascimento": None,
        "cpf": None,
    }

    # Nit: 1.688.946.939-0
    m_nit = re.search(r"Nit[: ]+([\d\.\-]+)", texto, re.IGNORECASE)
    if m_nit:
        cabecalho["nit"] = so_numeros(m_nit.group(1))

    # Nome: TALITA ...  Data de Nascimento: 06/01/1990
    m_nome = re.search(
        r"Nome[: ]+(.+?)\s+Data de Nascimento[: ]+[0-9]{2}/[0-9]{2}/[0-9]{4}",
        texto,
        re.IGNORECASE | re.DOTALL,
    )
    if m_nome:
        cabecalho["nome"] = m_nome.group(1).strip()

    # Data de Nascimento: 06/01/1990
    m_dn = re.search(
        r"Data de Nascimento[: ]+([0-9]{2}/[0-9]{2}/[0-9]{4})",
        texto,
        re.IGNORECASE,
    )
    if m_dn:
        cabecalho["data_nascimento"] = normalizar_data(m_dn.group(1))[0]

    # Nome da Mãe: ...
    m_mae = re.search(
        r"Nome da M[ãa]e[: ]+(.+?)(?:\s+CPF[: ]|Página\s+\d+ de \d+)",
        texto,
        re.IGNORECASE | re.DOTALL,
    )
    if m_mae:
        cabecalho["nome_mae"] = m_mae.group(1).strip()

    # CPF: 101.951.366-75
    m_cpf = re.search(r"CPF[: ]+([\d\.\-]+)", texto, re.IGNORECASE)
    if m_cpf:
        cabecalho["cpf"] = m_cpf.group(1).strip()

    # --------------------------------------------------------
    # TABELA – uma linha por registro (GFIP ou eSOCIAL)
    # --------------------------------------------------------
    linhas: list[dict] = []

    for raw_line in texto.splitlines():
        linha = raw_line.strip()

        # Ignora cabeçalhos e linhas vazias
        if not linha:
            continue
        if linha.startswith("Fonte NIT") or linha.startswith("Fonte  NIT"):
            continue

        partes = linha.split()
        if not partes:
            continue

        fonte_token = partes[0].upper()
        if not (fonte_token.startswith("GFIP") or fonte_token.startswith("ESOCIAL")):
            continue

        # ----------------------------------------------------
        # GFIP – linhas completas (11 colunas)
        # ----------------------------------------------------
        if fonte_token.startswith("GFIP") and len(partes) >= 11:
            # Fonte NIT Remuneração Competência CNPJ/CPF/CEI FPAS Categoria CódigoGFIP DataEnvio ValorRetido Extemp
            fonte = partes[0]
            nit_linha = partes[1]
            remuneracao_txt = partes[2]
            competencia_lit = partes[3]
            documento_tomador_raw = partes[4]
            fpas = partes[5]
            categoria = partes[6]
            codigo_gfip = partes[7]
            data_envio_lit = partes[8]
            valor_retido_txt = partes[9]
            extemporaneo_txt = partes[10]
        else:
            # ------------------------------------------------
            # eSOCIAL ou linhas GFIP "reduzidas"
            # eSOCIAL  NIT  Remuneração  Competência  DocTomador  FPAS/Outro  DataEnvio  ValorRetido  Extemp
            # ------------------------------------------------
            if len(partes) < 8:
                # Linha muito estranha, ignora
                continue

            fonte = partes[0]
            nit_linha = partes[1]
            remuneracao_txt = partes[2]
            competencia_lit = partes[3]
            documento_tomador_raw = partes[4] if len(partes) >= 5 else ""
            fpas = partes[5] if len(partes) >= 6 else ""
            categoria = None
            codigo_gfip = None
            data_envio_lit = partes[-3]
            valor_retido_txt = partes[-2]
            extemporaneo_txt = partes[-1]

        # Normalizações
        comp_date, comp_literal = normalizar_competencia(competencia_lit)
        data_envio_date, data_envio_literal = normalizar_data(data_envio_lit)
        remuneracao, remuneracao_literal = normalizar_moeda(remuneracao_txt)
        valor_retido, valor_retido_literal = normalizar_moeda(valor_retido_txt)
        doc_tomador, doc_tomador_tipo = normalizar_documento_tomador(
            documento_tomador_raw
        )

        extemp_literal = extemporaneo_txt.strip()
        extemporaneo_bool = (
            extemp_literal.lower().startswith("s")  # "Sim"
            if extemp_literal
            else False
        )

        linhas.append(
            {
                "fonte": fonte_token,  # GFIP ou ESOCIAL
                "numero_documento": None,  # não há número próprio neste layout
                "nit": so_numeros(nit_linha) or cabecalho.get("nit"),
                "competencia_literal": comp_literal,
                "competencia_date": comp_date,
                "competencia_ano": int(comp_date.split("-")[0])
                if comp_date
                else None,
                "competencia_mes": int(comp_date.split("-")[1])
                if comp_date
                else None,
                "documento_tomador": doc_tomador,
                "documento_tomador_tipo": doc_tomador_tipo,
                "fpas": fpas,
                "categoria_codigo": categoria,
                "codigo_gfip": codigo_gfip,
                "data_envio_literal": data_envio_literal,
                "data_envio_date": data_envio_date,
                "tipo_remuneracao": None,
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
