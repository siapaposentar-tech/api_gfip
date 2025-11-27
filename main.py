import re
import tempfile
from datetime import datetime
from decimal import Decimal

import pdfplumber
from fastapi import FastAPI, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware


# ============================================
# CONFIGURAÇÃO DO FASTAPI
# ============================================

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================
# FUNÇÕES DE APOIO
# ============================================

def so_numeros(valor: str) -> str:
    if not valor:
        return ""
    return re.sub(r"\D", "", valor)


def normalizar_documento_tomador(doc: str) -> tuple[str, str]:
    """
    Retorna (documento_numerico, tipo)
    tipo ∈ { 'CNPJ_COMPLETO', 'CNPJ_RAIZ', 'CPF', 'CEI', 'DESCONHECIDO' }
    """
    if not doc:
        return "", "DESCONHECIDO"

    bruto = doc.strip()
    numeros = so_numeros(bruto)
    tem_barra = "/" in bruto

    # CNPJ completo – 14 dígitos (com ou sem barra)
    if len(numeros) == 14:
        return numeros, "CNPJ_COMPLETO"

    # CEI – 12 dígitos
    if len(numeros) == 12:
        return numeros, "CEI"

    # CPF – 11 dígitos
    if len(numeros) == 11:
        return numeros, "CPF"

    # CNPJ RAIZ – até 8 dígitos
    if len(numeros) <= 8:
        raiz = numeros.zfill(8)
        return raiz, "CNPJ_RAIZ"

    # CNPJ truncado 9–13 dígitos → virar raiz
    if 9 <= len(numeros) <= 13:
        raiz = numeros[:8].zfill(8)
        return raiz, "CNPJ_RAIZ"

    return numeros, "DESCONHECIDO"


def normalizar_competencia(comp_str: str) -> str | None:
    if not comp_str:
        return None
    comp_str = comp_str.strip()
    try:
        dt = datetime.strptime(comp_str, "%m/%Y")
        return dt.strftime("%Y-%m-01")
    except ValueError:
        return None


def normalizar_data(ddmmaaaa: str) -> str | None:
    if not ddmmaaaa:
        return None
    ddmmaaaa = ddmmaaaa.strip()
    try:
        dt = datetime.strptime(ddmmaaaa, "%d/%m/%Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


def normalizar_moeda(valor: str) -> float | None:
    if not valor:
        return None
    txt = (
        valor.replace("R$", "")
        .replace(".", "")
        .replace(",", ".")
        .strip()
    )
    try:
        return float(Decimal(txt))
    except Exception:
        return None


# ============================================
# PARSE DO CABEÇALHO
# ============================================

def parse_cabecalho(texto: str) -> dict:
    cab = {
        "nit": None,
        "nome": None,
        "data_nascimento": None,
        "nome_mae": None,
    }

    m_nit = re.search(r"Nit:\s*([\d\.]+)", texto, re.IGNORECASE)
    if m_nit:
        cab["nit"] = so_numeros(m_nit.group(1))

    m_nome = re.search(r"Nome:\s*(.+)", texto, re.IGNORECASE)
    if m_nome:
        cab["nome"] = m_nome.group(1).strip()

    m_dn = re.search(r"Data de Nascimento:\s*([0-9]{2}/[0-9]{2}/[0-9]{4})", texto, re.IGNORECASE)
    if m_dn:
        cab["data_nascimento"] = normalizar_data(m_dn.group(1))

    m_mae = re.search(r"Nome da M[aã]e:\s*(.+)", texto, re.IGNORECASE)
    if m_mae:
        cab["nome_mae"] = m_mae.group(1).strip()

    return cab


# ============================================
# PARSER DO CI GFIP MODELO 1
# ============================================

def parse_ci_gfip_modelo_1(texto: str) -> dict:
    cabecalho = parse_cabecalho(texto)
    linhas_resultado = []

    padrao_linha = re.compile(
        r"^(GFIP|eSOCIAL)\s+"
        r"(\d+)\s+"
        r"(\d{2}/\d{4})\s+"
        r"([0-9\.\-\/ ]+)\s+"
        r"(\d{3})\s+"
        r"(\d+)\s+"
        r"(\d+)\s+"
        r"([0-9]{2}/[0-9]{2}/[0-9]{4})\s+"
        r"([\d\.\,]+)\s+"
        r"([\d\.\,]+)\s+"
        r"(Sim|Não)$",
        re.IGNORECASE
    )

    em_tabela = False

    for linha in texto.splitlines():
        linha = linha.strip()
        if not linha:
            continue

        if ("Fonte" in linha and "Competência" in linha
                and "CNPJ/CPF/CEI" in linha and "Data de Envio" in linha):
            em_tabela = True
            continue

        if not em_tabela:
            continue

        if linha.startswith("Página "):
            continue

        m = padrao_linha.match(linha)
        if not m:
            continue

        fonte = m.group(1).upper()
        nit_linha = so_numeros(m.group(2))
        competencia_str = m.group(3)
        doc_bruto = m.group(4).strip()
        fpas = m.group(5)
        categoria = m.group(6)
        codigo_gfip = m.group(7)
        data_envio_str = m.group(8)
        remun_str = m.group(9)
        retido_str = m.group(10)
        extemporaneo = m.group(11).capitalize()

        competencia_norm = normalizar_competencia(competencia_str)
        data_envio_norm = normalizar_data(data_envio_str)
        remun_val = normalizar_moeda(remun_str)
        retido_val = normalizar_moeda(retido_str)
        doc_norm, tipo_doc = normalizar_documento_tomador(doc_bruto)

        linhas_resultado.append({
            "fonte": fonte,
            "nit": nit_linha,
            "competencia": competencia_norm,
            "competencia_bruto": competencia_str,
            "documento_tomador": doc_norm,
            "documento_tomador_bruto": doc_bruto,
            "documento_tomador_tipo": tipo_doc,
            "fpas": fpas,
            "categoria_seg": categoria,
            "codigo_gfip": codigo_gfip,
            "data_envio": data_envio_norm,
            "data_envio_bruto": data_envio_str,
            "remuneracao": remun_val,
            "remuneracao_bruto": remun_str,
            "valor_retido": retido_val,
            "valor_retido_bruto": retido_str,
            "extemporaneo": extemporaneo,
        })

    return {
        "status": "sucesso",
        "mensagem": "Parser CI GFIP Modelo 1 executado com sucesso.",
        "cabecalho": cabecalho,
        "total_linhas": len(linhas_resultado),
        "linhas": linhas_resultado,
    }


# ============================================
# ROTA PRINCIPAL → RECEBE PDF → PROCESSA → DEVOLVE JSON
# ============================================

@app.post("/ci-gfip/modelo-1")
async def processar_ci_gfip_modelo_1(arquivo: UploadFile = File(...)):
    conteudo = await arquivo.read()

    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        tmp.write(conteudo)
        caminho_pdf = tmp.name

    texto = ""
    with pdfplumber.open(caminho_pdf) as pdf:
        for pagina in pdf.pages:
            texto += (pagina.extract_text() or "") + "\n"

    resultado = parse_ci_gfip_modelo_1(texto)

    resultado["arquivo_recebido"] = arquivo.filename
    resultado["tamanho_bytes"] = len(conteudo)

    return resultado
