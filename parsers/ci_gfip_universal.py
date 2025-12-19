import re
from datetime import datetime
from decimal import Decimal
from typing import List, Dict

# ============================================================
# 1. DETECTAR LAYOUT AUTOMATICAMENTE
# ============================================================

def detectar_layout_ci_gfip(texto: str) -> str:
    """
    Detecta automaticamente o layout do CI GFIP.
    Retorna: "modelo_1", "modelo_2" ou "layout_nao_identificado".
    """
    up = texto.upper()

    # PRIORIDADE ABSOLUTA: CONSULTA VALORES CI GFIP/eSocial/INSS → modelo_2
    if "CONSULTA VALORES" in up or "CI GFIP/ESOCIAL/INSS" in up:
        return "modelo_2"

    # Cabeçalho típico do modelo_2
    if "FONTE" in up and "NIT" in up and "COMPET" in up:
        return "modelo_2"

    # Modelo 1 (SEFIP tradicional)
    if "COMPETÊNCIA" in up and "FPAS" in up:
        return "modelo_1"

    return "layout_nao_identificado"


# ============================================================
# 2. FUNÇÕES DE NORMALIZAÇÃO
# ============================================================

def so_numeros(valor: str | None) -> str:
    return re.sub(r"\D", "", valor or "")


def normalizar_competencia(comp_str: str | None):
    if not comp_str:
        return None, ""
    comp_str = comp_str.strip()
    for fmt in ("%m/%Y", "%m-%Y"):
        try:
            dt = datetime.strptime(comp_str, fmt)
            return dt.strftime("%Y-%m-01"), comp_str
        except:
            continue
    return None, comp_str


def normalizar_data(ddmmaaaa: str | None):
    if not ddmmaaaa:
        return None, ""
    ddmmaaaa = ddmmaaaa.strip()
    for fmt in ("%d/%m/%Y", "%d-%m-%Y"):
        try:
            dt = datetime.strptime(ddmmaaaa, fmt)
            return dt.strftime("%Y-%m-%d"), ddmmaaaa
        except:
            continue
    return None, ddmmaaaa


def normalizar_moeda(valor: str | None):
    if not valor:
        return None, ""
    bruto = valor.strip()
    # aceita "R$ 1.234,56" ou "1.234,56" ou "-"
    if bruto == "-":
        return None, bruto
    txt = bruto.replace("R$", "").replace(" ", "").replace(".", "").replace(",", ".")
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


def _merge_moeda_tokens(partes: List[str]) -> List[str]:
    """
    Junta tokens quando vier no formato: ['R$', '2.948,38'] -> ['R$ 2.948,38']
    Isso evita deslocamento de colunas.
    """
    out: List[str] = []
    i = 0
    while i < len(partes):
        if partes[i] == "R$" and i + 1 < len(partes):
            out.append(f"R$ {partes[i + 1]}")
            i += 2
            continue
        out.append(partes[i])
        i += 1
    return out


# ============================================================
# 3. CABEÇALHO — CAPTURA COMPLETA E CORRIGIDA
# ============================================================

def parse_cabecalho(texto: str) -> dict:
    cab = {
        "nit": None,
        "nome": None,
        "nome_mae": None,
        "data_nascimento": None,
        "cpf": None,
    }

    # NIT
    m = re.search(r"NIT[:\s]*([\d\.\-]+)", texto, re.IGNORECASE)
    if m:
        cab["nit"] = so_numeros(m.group(1))

    # NOME
    m = re.search(r"Nome[:\s]*([A-ZÁÉÍÓÚÀÂÊÔÃÕÇ ]+)", texto)
    if m:
        cab["nome"] = m.group(1).strip()

    # NOME DA MÃE — CORRIGIDO
    m = re.search(
        r"(NOME\s+DA\s+M[ÃA]E|M[ÃA]E)[:\s]*([A-ZÁÉÍÓÚÀÂÊÔÃÕÇ ]+)",
        texto,
        re.IGNORECASE
    )
    if m:
        cab["nome_mae"] = m.group(2).strip()

    # DATA DE NASCIMENTO — CORRIGIDO
    m = re.search(
        r"(DATA DE NASCIMENTO|NASCIMENTO|NASC\.?|NASC|DT\.? NASC)[\s:]*"
        r"(\d{2}/\d{2}/\d{4})",
        texto,
        re.IGNORECASE
    )
    if m:
        data = m.group(2)
        cab["data_nascimento"] = str(datetime.strptime(data, "%d/%m/%Y").date())

    # CPF — se existir
    m = re.search(r"CPF[:\s]*([\d\.\-]+)", texto)
    if m:
        cab["cpf"] = so_numeros(m.group(1))

    return cab


# ============================================================
# 4. PARSER UNIVERSAL – MODELO 2
# ============================================================

def _linhas_modelo_2(texto: str) -> List[Dict]:
    linhas: List[Dict] = []
    in_table = False

    for raw in texto.splitlines():
        linha = raw.strip()
        if not linha:
            continue

        up = linha.upper()

        # INÍCIO DA TABELA
        if "FONTE" in up and "NIT" in up and "COMPET" in up:
            in_table = True
            continue

        if not in_table:
            continue

        if linha.startswith("PÁG") or linha.startswith("PAG") or linha.startswith("Página"):
            continue

        partes = linha.split()
        if not partes:
            continue

        partes = _merge_moeda_tokens(partes)

        fonte = partes[0].upper()
        if fonte not in ("GFIP", "ESOCIAL"):
            continue

        try:
            numero_documento = None
            nit_raw = ""
            competencia = ""
            doc_tomador_raw = ""
            fpas = ""
            categoria = None
            codigo_gfip = None
            data_envio_lit = ""
            tipo_rem = None
            remun_txt = ""
            valor_retido_txt = ""
            extemp_txt = ""

            # ----------------------------
            # GFIP (pode vir com 13 ou 11)
            # ----------------------------
            if fonte == "GFIP" and len(partes) >= 13:
                # GFIP novo (13 colunas)
                numero_documento = partes[1]
                nit_raw          = partes[2]
                competencia      = partes[3]
                doc_tomador_raw  = partes[4]
                fpas             = partes[5]
                categoria        = partes[6]
                codigo_gfip      = partes[7]
                data_envio_lit   = partes[8]
                tipo_rem         = partes[9]
                remun_txt        = partes[10]
                valor_retido_txt = partes[11]
                extemp_txt       = partes[12]

            elif fonte == "GFIP" and len(partes) == 11:
                # GFIP antigo (11 colunas)
                nit_raw          = partes[1]
                competencia      = partes[2]
                doc_tomador_raw  = partes[3]
                fpas             = partes[4]
                categoria        = partes[5]
                codigo_gfip      = partes[6]
                data_envio_lit   = partes[7]
                remun_txt        = partes[8]
                valor_retido_txt = partes[9]
                extemp_txt       = partes[10]

            # -----------------------------------------
            # ESOCIAL (NÃO TEM categoria/código GFIP)
            # Corrigido: data de envio vai para data_envio
            # -----------------------------------------
            elif fonte == "ESOCIAL":
                # Formato mais comum após merge de "R$ 76,71":
                # ESOCIAL <num_doc> <nit> <compet> <doc> <fpas> <data_envio> <tipo_rem> <remun> <valor_retido> <extemp>
                # ou sem valor_retido:
                # ESOCIAL <num_doc> <nit> <compet> <doc> <fpas> <data_envio> <tipo_rem> <remun> <extemp>
                if len(partes) >= 11:
                    numero_documento = partes[1]
                    nit_raw          = partes[2]
                    competencia      = partes[3]
                    doc_tomador_raw  = partes[4]
                    fpas             = partes[5]
                    data_envio_lit   = partes[6]
                    tipo_rem         = partes[7]
                    remun_txt        = partes[8]
                    valor_retido_txt = partes[9]
                    extemp_txt       = partes[10]
                elif len(partes) == 10:
                    numero_documento = partes[1]
                    nit_raw          = partes[2]
                    competencia      = partes[3]
                    doc_tomador_raw  = partes[4]
                    fpas             = partes[5]
                    data_envio_lit   = partes[6]
                    tipo_rem         = partes[7]
                    remun_txt        = partes[8]
                    extemp_txt       = partes[9]
                else:
                    continue

                # Regras ESOCIAL
                categoria = None
                codigo_gfip = None
                valor_retido_txt = ""  # ESOCIAL não deve preencher valor_retido

            # ----------------------------
            # Fallback (evita travar)
            # ----------------------------
            elif len(partes) >= 9:
                nit_raw          = partes[1]
                competencia      = partes[2]
                doc_tomador_raw  = partes[3]
                fpas             = partes[4]
                categoria        = partes[5]
                data_envio_lit   = partes[6]
                remun_txt        = partes[7]
                valor_retido_txt = partes[8]
                extemp_txt       = partes[9] if len(partes) > 9 else ""
            else:
                continue

            comp_date, comp_literal = normalizar_competencia(competencia)
            data_envio_date, data_envio_literal = normalizar_data(data_envio_lit)
            remuneracao, remuneracao_literal = normalizar_moeda(remun_txt)

            # Valor retido só para GFIP
            if fonte == "GFIP":
                valor_retido, valor_retido_literal = normalizar_moeda(valor_retido_txt)
            else:
                valor_retido, valor_retido_literal = None, ""

            doc_tomador, doc_tomador_tipo = normalizar_documento_tomador(doc_tomador_raw)

            extemp_literal = extemp_txt.strip()
            extemporaneo = extemp_literal.lower().startswith("s") if extemp_literal else False

            # REGRA FINAL: ESOCIAL não tem categoria/cod GFIP
            if fonte != "GFIP":
                categoria = None
                codigo_gfip = None

            linhas.append({
                "fonte": fonte,
                "numero_documento": numero_documento,
                "nit": so_numeros(nit_raw),
                "competencia_literal": comp_literal,
                "competencia_date": comp_date,
                "competencia_ano": int(comp_date.split("-")[0]) if comp_date else None,
                "competencia_mes": int(comp_date.split("-")[1]) if comp_date else None,
                "documento_tomador": doc_tomador,
                "documento_tomador_tipo": doc_tomador_tipo,
                "fpas": fpas,
                "categoria_codigo": categoria,
                "codigo_gfip": codigo_gfip,

                # data de envio (especialmente importante para ESOCIAL)
                "data_envio_literal": data_envio_literal if fonte == "ESOCIAL" else data_envio_literal,
                "data_envio_date": data_envio_date if fonte == "ESOCIAL" else data_envio_date,

                "tipo_remuneracao": tipo_rem,
                "remuneracao_literal": remuneracao_literal,
                "remuneracao": remuneracao,

                "valor_retido_literal": valor_retido_literal,
                "valor_retido": valor_retido,

                "extemporaneo_literal": extemp_literal,
                "extemporaneo": extemporaneo,
            })

        except:
            continue

    return linhas


# ============================================================
# 5. PARSER PRINCIPAL
# ============================================================

def parse_ci_gfip(texto: str) -> dict:
    layout = detectar_layout_ci_gfip(texto)

    if layout == "modelo_2":
        cab = parse_cabecalho(texto)
        linhas = _linhas_modelo_2(texto)
        return {
            "cabecalho": cab,
            "linhas": linhas,
            "layout_detectado": layout,
        }

    if layout == "modelo_1":
        # Implementaremos depois
        return {"erro": "modelo_1_ainda_nao_implementado"}

    return {"erro": "layout_nao_identificado"}
