import re
from datetime import datetime
from decimal import Decimal
from typing import List, Dict, Optional

# ============================================================
#  FUNÇÕES GENÉRICAS
# ============================================================

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


# ============================================================
#  DETECTAR LAYOUT
# ============================================================

def detectar_layout_ci_gfip(texto: str) -> str:
    """
    Detecta automaticamente o layout do CI GFIP.
    Retorna: "modelo_1", "modelo_2" ou "layout_nao_identificado".
    """
    up = texto.upper()

    # CONSULTA VALORES CI GFIP/eSocial/INSS (tabela Fonte / Número do Documento / NIT...)
    if "CONSULTA VALORES CI GFIP/ESOCIAL/INSS" in up:
        return "modelo_2"

    # Layout mais antigo (tabela COMPET / FPAS / CATEG...)
    if "COMPET" in up and "FPAS" in up and "CATEG" in up:
        return "modelo_1"

    return "layout_nao_identificado"


# ============================================================
#  PARSER MODELO 1  (SEFIP TRADICIONAL SIMPLIFICADO)
# ============================================================

def parse_modelo_1(texto: str) -> dict:
    """
    Parser simples para relatórios antigos onde a tabela
    tem cabeçalho COMPET / FPAS / CATEG / REMUNERAÇÃO etc.
    (É basicamente o código que você já tinha.)
    """

    linhas_txt = texto.splitlines()

    cabecalho = {
        "nit": "",
        "nome": "",
        "nome_mae": "",
        "data_nascimento": "",
        "cpf": "",
    }

    registros: List[Dict] = []

    # -------------------------------
    # CABEÇALHO
    # -------------------------------
    for linha in linhas_txt:
        if "NIT" in linha.upper():
            m = re.search(r"NIT[:\s]+([\d\.\-]+)", linha, re.IGNORECASE)
            if m:
                cabecalho["nit"] = so_numeros(m.group(1))

        if "NOME" in linha.upper() and "MAE" not in linha.upper():
            m = re.search(r"NOME[:\s]+(.+)", linha, re.IGNORECASE)
            if m:
                cabecalho["nome"] = m.group(1).strip()

        if "MAE" in linha.upper():
            m = re.search(r"MAE[:\s]+(.+)", linha, re.IGNORECASE)
            if m:
                cabecalho["nome_mae"] = m.group(1).strip()

        if "NASCTO" in linha.upper() or "NASCIMENTO" in linha.upper():
            m = re.search(r"(\d{2}/\d{2}/\d{4})", linha)
            if m:
                cabecalho["data_nascimento"] = m.group(1)

        if "CPF" in linha.upper():
            m = re.search(r"CPF[:\s]+([\d\. -]+)", linha)
            if m:
                cabecalho["cpf"] = m.group(1).strip()

    # -------------------------------
    # LINHAS / TABELA
    # -------------------------------
    dentro_tabela = False

    for linha in linhas_txt:
        if (
            "COMPET" in linha.upper()
            and "FPAS" in linha.upper()
            and "CATEG" in linha.upper()
        ):
            dentro_tabela = True
            continue

        if not dentro_tabela:
            continue

        partes = re.split(r"\s{2,}", linha.strip())
        if len(partes) < 5:
            continue

        try:
            competencia = partes[0]
            fpas = partes[1]
            categoria = partes[2]
            remuneracao = partes[3] if len(partes) > 3 else "0"
            valor_retido = partes[4] if len(partes) > 4 else "0"

            remun_num, remun_lit = normalizar_moeda(remuneracao)
            retido_num, retido_lit = normalizar_moeda(valor_retido)

            comp_date, comp_lit = normalizar_competencia(competencia)

            registros.append(
                {
                    "fonte": "GFIP",
                    "numero_documento": None,
                    "nit": cabecalho["nit"],
                    "competencia_literal": comp_lit,
                    "competencia_date": comp_date,
                    "competencia_ano": int(comp_date.split("-")[0]) if comp_date else None,
                    "competencia_mes": int(comp_date.split("-")[1]) if comp_date else None,
                    "documento_tomador": None,
                    "documento_tomador_tipo": None,
                    "fpas": fpas,
                    "categoria_codigo": categoria,
                    "codigo_gfip": None,
                    "data_envio_literal": None,
                    "data_envio_date": None,
                    "tipo_remuneracao": None,
                    "remuneracao_literal": remun_lit,
                    "remuneracao": remun_num,
                    "valor_retido_literal": retido_lit,
                    "valor_retido": retido_num,
                    "extemporaneo_literal": None,
                    "extemporaneo": None,
                }
            )
        except Exception:
            continue

    return {
        "cabecalho": cabecalho,
        "linhas": registros,
        "total_linhas": len(registros),
        "layout": "modelo_1",
    }


# ============================================================
#  PARSER MODELO 2 – CONSULTA VALORES CI GFIP/eSocial/INSS
# ============================================================

def _cabecalho_modelo_2(texto: str) -> Dict[str, Optional[str]]:
    """
    Lê o bloco "Identificação do Filiado" dos relatórios
    CONSULTA VALORES CI GFIP/eSocial/INSS.
    """

    cab: Dict[str, Optional[str]] = {
        "nit": None,
        "nome": None,
        "nome_mae": None,
        "data_nascimento": None,
        "cpf": None,
    }

    m_nit = re.search(r"Nit[: ]+([\d\.\-]+)", texto, re.IGNORECASE)
    if m_nit:
        cab["nit"] = so_numeros(m_nit.group(1))

    m_nome = re.search(
        r"Nome[: ]+(.+?)\s+Data de Nascimento[: ]+(\d{2}/\d{2}/\d{4})",
        texto,
        re.IGNORECASE | re.DOTALL,
    )
    if m_nome:
        cab["nome"] = m_nome.group(1).strip()
        dn_lit = m_nome.group(2).strip()
        dn_iso, _ = normalizar_data(dn_lit)
        cab["data_nascimento"] = dn_iso

    m_mae = re.search(
        r"Nome da M[ãa]e[: ]+(.+?)\s+CPF[: ]",
        texto,
        re.IGNORECASE | re.DOTALL,
    )
    if m_mae:
        cab["nome_mae"] = m_mae.group(1).strip()

    m_cpf = re.search(r"CPF[: ]+([\d\.\-]+)", texto, re.IGNORECASE)
    if m_cpf:
        cab["cpf"] = m_cpf.group(1).strip()

    return cab


def _linhas_modelo_2(texto: str) -> List[Dict]:
    """
    Lê todas as linhas da tabela CONSULTA VALORES CI GFIP/eSocial/INSS,
    tratando corretamente GFIP e eSOCIAL.
    """

    linhas: List[Dict] = []
    in_table = False

    for raw in texto.splitlines():
        linha = raw.strip()
        if not linha:
            continue

        # Identifica o cabeçalho da tabela
        up = linha.upper()
        if (
            "FONTE" in up
            and "NIT" in up
            and "COMPET" in up
            and "VALOR RETIDO" in up
        ):
            in_table = True
            continue

        if not in_table:
            continue

        # ignora rodapé
        if linha.startswith("Página "):
            continue

        partes = linha.split()
        if not partes:
            continue

        fonte = partes[0].upper()
        if fonte not in ("GFIP", "ESOCIAL"):
            continue

        try:
            if fonte == "GFIP" and len(partes) >= 13:
                # Fonte  NºDoc  NIT  Comp  CNPJ/CPF/CEI  FPAS  Categ  CodGFIP  DtEnvio  TipoRem  Remun  ValRet  Extemp
                numero_documento = partes[1]
                nit_raw = partes[2]
                competencia = partes[3]
                doc_tomador_raw = partes[4]
                fpas = partes[5]
                categoria = partes[6]
                codigo_gfip = partes[7]
                data_envio_lit = partes[8]
                tipo_rem = partes[9]
                remun_txt = partes[10]
                valor_retido_txt = partes[11]
                extemp_txt = partes[12] if len(partes) > 12 else ""
            elif fonte == "ESOCIAL" and len(partes) >= 12:
                # eSOCIAL não tem Código GFIP (coluna vazia)
                # Fonte  NºDoc  NIT  Comp  CNPJ/CPF/CEI  FPAS  Categ  DtEnvio  TipoRem  Remun  ValRet  Extemp
                numero_documento = partes[1]
                nit_raw = partes[2]
                competencia = partes[3]
                doc_tomador_raw = partes[4]
                fpas = partes[5]
                categoria = partes[6]
                codigo_gfip = None
                data_envio_lit = partes[7]
                tipo_rem = partes[8]
                remun_txt = partes[9]
                valor_retido_txt = partes[10]
                extemp_txt = partes[11]
            else:
                # Linha estranha, ignora
                continue

            # Normalizações
            comp_date, comp_literal = normalizar_competencia(competencia)
            data_envio_date, data_envio_literal = normalizar_data(data_envio_lit)
            remuneracao, remuneracao_literal = normalizar_moeda(remun_txt)
            valor_retido, valor_retido_literal = normalizar_moeda(valor_retido_txt)
            doc_tomador, doc_tomador_tipo = normalizar_documento_tomador(
                doc_tomador_raw
            )

            extemp_literal = extemp_txt.strip()
            extemporaneo = (
                extemp_literal.lower().startswith("s") if extemp_literal else False
            )

            linhas.append(
                {
                    "fonte": fonte,
                    "numero_documento": numero_documento,
                    "nit": so_numeros(nit_raw),
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
                    "tipo_remuneracao": tipo_rem,
                    "remuneracao_literal": remuneracao_literal,
                    "remuneracao": remuneracao,
                    "valor_retido_literal": valor_retido_literal,
                    "valor_retido": valor_retido,
                    "extemporaneo_literal": extemp_literal,
                    "extemporaneo": extemporaneo,
                }
            )
        except Exception:
            # Qualquer problema na linha: ignora só aquela
            continue

    return linhas


def parse_ci_gfip_modelo_2(texto: str) -> dict:
    """
    Parser específico para os layouts:
    'CONSULTA VALORES CI GFIP/eSocial/INSS'
    """

    cabecalho = _cabecalho_modelo_2(texto)
    linhas = _linhas_modelo_2(texto)

    return {
        "cabecalho": cabecalho,
        "linhas": linhas,
        "total_linhas": len(linhas),
        "layout": "modelo_2",
    }


# ============================================================
#  PARSER UNIVERSAL
# ============================================================

def parse_ci_gfip(texto: str) -> dict:
    """
    Parser universal:
      - Detecta o layout
      - Encaminha para o parser correto (modelo_1 ou modelo_2)
      - Retorna sempre { cabecalho, linhas, total_linhas, layout } ou
        {"erro": "layout_nao_identificado"}.
    """
    layout = detectar_layout_ci_gfip(texto)

    if layout == "modelo_1":
        return parse_modelo_1(texto)

    if layout == "modelo_2":
        return parse_ci_gfip_modelo_2(texto)

    return {"erro": "layout_nao_identificado"}
