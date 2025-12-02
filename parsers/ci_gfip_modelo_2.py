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
    - literal original (sempre devolvido)
    """
    if not comp_str:
        return None, ""
    try:
        dt = datetime.strptime(comp_str.strip(), "%m/%Y")
        return dt.strftime("%Y-%m-01"), comp_str
    except Exception:
        return None, comp_str


def normalizar_data(ddmmaaaa: str | None) -> tuple[str | None, str]:
    """
    Recebe '31/12/2020' e devolve:
    - data ISO (ex: "2020-12-31")
    - literal original
    """
    if not ddmmaaaa:
        return None, ""
    try:
        dt = datetime.strptime(ddmmaaaa.strip(), "%d/%m/%Y")
        return dt.strftime("%Y-%m-%d"), ddmmaaaa
    except Exception:
        return None, ddmmaaaa


def normalizar_moeda(valor: str | None) -> tuple[float | None, str]:
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


def normalizar_documento_tomador(valor: str | None) -> tuple[str, str]:
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
#  PARSER DO MODELO 2 – TABELA HORIZONTAL CI GFIP/eSocial/INSS
# =====================================================================

def parse_ci_gfip_modelo_2(texto: str) -> dict:
    """
    Parser específico para os layouts:
    'CONSULTA VALORES CI GFIP/eSocial/INSS'

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

    # Vamos trabalhar linha a linha do texto
    for raw_line in texto.splitlines():
        linha = raw_line.strip()

        # Considera apenas linhas que começam com 'GFIP' ou 'eSOCIAL'
        if not (linha.startswith("GFIP") or linha.upper().startswith("ESOCIAL")):
            continue

        # Quebra por espaços (colunas separadas por espaço)
        partes = linha.split()
        if len(partes) < 10:
            # Linha estranha ou quebrada, ignora por segurança
            continue

        # Estrutura básica:
        # 0  = Fonte (GFIP / eSOCIAL)
        # 1  = Número do Documento
        # 2  = NIT
        # 3  = Competência (MM/AAAA)
        # 4  = CNPJ/CPF/CEI (tomador)
        # 5  = FPAS
        # 6  = Categoria GFIP/eSocial
        # 7  = Código GFIP
        # 8  = Data de Envio
        # 9..-4 = Tipo de Remuneração (pode ter 1 ou mais palavras)
        # -3 = Remuneração
        # -2 = Valor Retido
        # -1 = Extemp. (Sim/Não)

        fonte = partes[0]
        numero_documento = partes[1]
        nit_linha = partes[2]
        competencia_lit = partes[3]
        documento_tomador_raw = partes[4]
        fpas = partes[5]
        categoria = partes[6]
        codigo_gfip = partes[7]
        data_envio_lit = partes[8]

        if len(partes) >= 12:
            tipo_remuneracao_tokens = partes[9:-3]
            remuneracao_txt = partes[-3]
            valor_retido_txt = partes[-2]
            extemporaneo_txt = partes[-1]
        else:
            # fallback: assume estrutura mínima
            tipo_remuneracao_tokens = [partes[9]]
            remuneracao_txt = partes[10] if len(partes) > 10 else ""
            valor_retido_txt = partes[11] if len(partes) > 11 else ""
            extemporaneo_txt = partes[12] if len(partes) > 12 else ""

        tipo_remuneracao = " ".join(tipo_remuneracao_tokens).strip()

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
                "fonte": fonte.upper(),  # GFIP ou ESOCIAL
                "numero_documento": numero_documento,
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
