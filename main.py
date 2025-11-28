import re
import os
import hashlib
import tempfile
from datetime import datetime
from decimal import Decimal

import pdfplumber
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from supabase import create_client, Client

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
# CONFIGURAÇÃO DO SUPABASE
# ============================================

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client | None = None
if SUPABASE_URL and SUPABASE_KEY:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


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

    # CNPJ completo – 14 dígitos
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
        return numeros.zfill(8), "CNPJ_RAIZ"

    # CNPJ truncado 9–13 dígitos → virar raiz
    if 9 <= len(numeros) <= 13:
        return numeros[:8].zfill(8), "CNPJ_RAIZ"

    return numeros, "DESCONHECIDO"


def normalizar_competencia(comp_str: str) -> str | None:
    if not comp_str:
        return None
    try:
        dt = datetime.strptime(comp_str, "%m/%Y")
        return dt.strftime("%Y-%m-01")
    except ValueError:
        return None


def normalizar_data(ddmmaaaa: str) -> str | None:
    if not ddmmaaaa:
        return None
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
    cab = {"nit": None, "nome": None, "data_nascimento": None, "nome_mae": None}

    m_nit = re.search(r"Nit:\s*([\d\.]+)", texto, re.IGNORECASE)
    if m_nit:
        cab["nit"] = so_numeros(m_nit.group(1))

    m_nome = re.search(r"Nome:\s*(.+)", texto, re.IGNORECASE)
    if m_nome:
        cab["nome"] = m_nome.group(1).strip()

    m_dn = re.search(r"Data de Nascimento:\s*([0-9]{2}/[0-9]{2}/[0-9]{4})", texto)
    if m_dn:
        cab["data_nascimento"] = normalizar_data(m_dn.group(1))

    m_mae = re.search(r"Nome da M[aã]e:\s*(.+)", texto, re.IGNORECASE)
    if m_mae:
        cab["nome_mae"] = m_mae.group(1).strip()

    return cab


# ============================================
# PARSE DO CI GFIP MODELO 1
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

        linhas_resultado.append({
            "fonte": fonte,
            "nit": nit_linha,
            "competencia": normalizar_competencia(competencia_str),
            "competencia_bruto": competencia_str,
            "documento_tomador": normalizar_documento_tomador(doc_bruto)[0],
            "documento_tomador_bruto": doc_bruto,
            "documento_tomador_tipo": normalizar_documento_tomador(doc_bruto)[1],
            "fpas": fpas,
            "categoria_seg": categoria,
            "codigo_gfip": codigo_gfip,
            "data_envio": normalizar_data(data_envio_str),
            "data_envio_bruto": data_envio_str,
            "remuneracao": normalizar_moeda(remun_str),
            "remuneracao_bruto": remun_str,
            "valor_retido": normalizar_moeda(retido_str),
            "valor_retido_bruto": retido_str,
            "extemporaneo": extemporaneo,
        })

    return {
        "cabecalho": cabecalho,
        "linhas": linhas_resultado,
    }


# ============================================
# FUNÇÕES DE BANCO (SUPABASE)
# ============================================

def calcular_hash_arquivo(conteudo: bytes) -> str:
    return hashlib.sha256(conteudo).hexdigest()


def get_or_create_segurado(cab: dict) -> str | None:
    """Localiza ou cria o segurado."""
    if supabase is None:
        return None

    nit = cab.get("nit")
    nome = cab.get("nome")

    if not nome:
        return None

    # 1) Procurar NIT na tabela segurado_nits
    if nit:
        r = supabase.table("segurado_nits").select("segurado_id").eq("nit", nit).execute()
        if r.data:
            return r.data[0]["segurado_id"]

    # 2) Criar segurado novo
    resp = supabase.table("segurados").insert({
        "nome": nome,
        "data_nascimento": cab.get("data_nascimento"),
        "nome_mae": cab.get("nome_mae"),
        "nit_principal": nit,
    }).execute()

    segurado_id = resp.data[0]["id"]

    # 3) Registrar NIT principal
    if nit:
        supabase.table("segurado_nits").insert({
            "segurado_id": segurado_id,
            "nit": nit
        }).execute()

    return segurado_id


def salvar_ci_gfip_no_supabase(parser: dict, arquivo_nome: str, arquivo_bytes: bytes):
    """Salva segurado, relatório e linhas."""
    if supabase is None:
        return None

    cab = parser["cabecalho"]
    linhas = parser["linhas"]

    segurado_id = get_or_create_segurado(cab)
    if not segurado_id:
        return None

    hash_doc = calcular_hash_arquivo(arquivo_bytes)

    # salva relatório
    resp_rel = supabase.table("ci_gfip_relatorios").insert({
        "segurado_id": segurado_id,
        "arquivo_storage_path": arquivo_nome,
        "hash_documento": hash_doc,
        "modelo_relatorio": "modelo_1"
    }).execute()
    relatorio_id = resp_rel.data[0]["id"]

    # salva linhas
    linhas_insert = []
    for l in linhas:
        comp_date = l.get("competencia")
        ano = int(comp_date[:4]) if comp_date else None
        mes = int(comp_date[5:7]) if comp_date else None

        linhas_insert.append({
            "relatorio_id": relatorio_id,
            "fonte": l["fonte"],
            "nit": l["nit"],
            "competencia_date": comp_date,
            "competencia_literal": l["competencia_bruto"],
            "competencia_ano": ano,
            "competencia_mes": mes,
            "documento_tomador": l["documento_tomador"],
            "documento_tomador_tipo": l["documento_tomador_tipo"],
            "fpas": l["fpas"],
            "categoria_codigo": l["categoria_seg"],
            "codigo_gfip": l["codigo_gfip"],
            "data_envio_date": l["data_envio"],
            "data_envio_literal": l["data_envio_bruto"],
            "remuneracao": l["remuneracao"],
            "remuneracao_literal": l["remuneracao_bruto"],
            "valor_retido": l["valor_retido"],
            "valor_retido_literal": l["valor_retido_bruto"],
            "extemporaneo_literal": l["extemporaneo"]
        })

    if linhas_insert:
        supabase.table("ci_gfip_linhas").insert(linhas_insert).execute()

    return {
        "segurado_id": segurado_id,
        "relatorio_id": relatorio_id,
        "linhas_salvas": len(linhas_insert)
    }


# ============================================
# ROTA PRINCIPAL
# ============================================

@app.post("/ci-gfip/modelo-1")
async def processar_ci_gfip_modelo_1(arquivo: UploadFile = File(...)):
    conteudo = await arquivo.read()

    if not conteudo:
        raise HTTPException(status_code=400, detail="Arquivo PDF vazio.")

    # extrair texto
    texto = ""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        tmp.write(conteudo)
        caminho_pdf = tmp.name

    with pdfplumber.open(caminho_pdf) as pdf:
        for pagina in pdf.pages:
            texto += (pagina.extract_text() or "") + "\n"

    # aplicar parser
    resultado = parse_ci_gfip_modelo_1(texto)

    # salvar no Supabase
    info_supabase = salvar_ci_gfip_no_supabase(
        parser=resultado,
        arquivo_nome=arquivo.filename,
        arquivo_bytes=conteudo,
    )

    return {
        "status": "sucesso",
        "mensagem": "Extração e gravação concluídas.",
        "cabecalho": resultado["cabecalho"],
        "total_linhas": len(resultado["linhas"]),
        "linhas": resultado["linhas"],
        "arquivo": arquivo.filename,
        "supabase": info_supabase,
    }
