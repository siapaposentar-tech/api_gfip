import re
import os
import hashlib
import tempfile
import requests

import pdfplumber
from fastapi import FastAPI, File, UploadFile, Form
from fastapi.middleware.cors import CORSMiddleware

from supabase import create_client, Client
from parsers.ci_gfip_universal import (
    parse_ci_gfip,
    detectar_layout_ci_gfip
)

# ============================================
# FASTAPI CONFIG
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
# SUPABASE CONFIG
# ============================================

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client | None = None
if SUPABASE_URL and SUPABASE_KEY:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ============================================
# HELPERS
# ============================================

def so_numeros(valor: str | None) -> str:
    return re.sub(r"\D", "", valor or "")

def calcular_hash_arquivo(conteudo: bytes) -> str:
    return hashlib.sha256(conteudo).hexdigest()

# ============================================
# BRASILAPI (ENRIQUECIMENTO OPCIONAL)
# ============================================

def consultar_brasilapi_cnpj(cnpj: str) -> dict | None:
    try:
        url = f"https://brasilapi.com.br/api/cnpj/v1/{cnpj}"
        resp = requests.get(url, timeout=10)

        if resp.status_code != 200:
            return None

        data = resp.json()
        return {
            "nome": data.get("razao_social") or data.get("nome_fantasia")
        }
    except:
        return None

# ============================================
# SEGURADO
# ============================================

def get_or_create_segurado(cab: dict) -> str | None:
    if supabase is None:
        return None

    nit = so_numeros(cab.get("nit"))
    nome = (cab.get("nome") or "").strip()

    if not nome:
        return None

    # Busca segurado por NIT
    if nit:
        r = (
            supabase.table("ci_gfip_segurado_nits")
            .select("segurado_id")
            .eq("nit", nit)
            .execute()
        )
        if r.data:
            return r.data[0]["segurado_id"]

    # Criação do segurado
    resp = (
        supabase.table("ci_gfip_segurados")
        .insert({
            "nome": nome,
            "data_nascimento": cab.get("data_nascimento"),
            "nome_mae": cab.get("nome_mae"),
            "nit_principal": nit or None,
            "profissao": cab.get("profissao"),
            "estado": cab.get("estado"),
        })
        .execute()
    )

    segurado_id = resp.data[0]["id"]

    if nit:
        supabase.table("ci_gfip_segurado_nits").insert(
            {"segurado_id": segurado_id, "nit": nit}
        ).execute()

    return segurado_id

# ============================================
# EMPRESA (TABELA CENTRAL)
# ============================================

def get_or_create_empresa(doc_tomador: str | None) -> str | None:
    if supabase is None:
        return None

    doc = so_numeros(doc_tomador)
    if not doc:
        return None

    if len(doc) >= 14:
        raiz = doc[:8]
        cnpj = doc[:14]
    else:
        raiz = doc.zfill(8)
        cnpj = None

    # Busca empresa pela raiz do CNPJ
    resp = (
        supabase.table("empresas")
        .select("id, cnpj, nome")
        .eq("raiz_cnpj", raiz)
        .limit(1)
        .execute()
    )

    if resp.data:
        empresa = resp.data[0]
        updates = {}

        if cnpj and not empresa.get("cnpj"):
            updates["cnpj"] = cnpj

        if cnpj and not empresa.get("nome"):
            enriched = consultar_brasilapi_cnpj(cnpj)
            if enriched and enriched.get("nome"):
                updates["nome"] = enriched["nome"]

        if updates:
            updates["atualizado_em"] = "now()"
            supabase.table("empresas").update(updates).eq("id", empresa["id"]).execute()

        return empresa["id"]

    # Criação da empresa
    insert_data = {
        "raiz_cnpj": raiz,
        "cnpj": cnpj,
        "origem_inicial": "CI_GFIP",
    }

    if cnpj:
        enriched = consultar_brasilapi_cnpj(cnpj)
        if enriched and enriched.get("nome"):
            insert_data["nome"] = enriched["nome"]

    resp_new = supabase.table("empresas").insert(insert_data).execute()
    return resp_new.data[0]["id"]

# ============================================
# SALVAR RELATÓRIO COMPLETO
# ============================================

def salvar_relatorio_completo(parser: dict, arquivo_nome: str, arquivo_bytes: bytes, modelo: str):

    cab = parser.get("cabecalho", {}) or {}
    linhas = parser.get("linhas", []) or []

    segurado_id = get_or_create_segurado(cab)
    hash_doc = calcular_hash_arquivo(arquivo_bytes)

    resp_rel = (
        supabase.table("ci_gfip_relatorios")
        .insert({
            "segurado_id": segurado_id,
            "tipo_relatorio": "ci_gfip",
            "modelo_relatorio": modelo,
            "arquivo_storage_path": arquivo_nome,
            "hash_documento": hash_doc,
            "profissao": cab.get("profissao"),
            "estado": cab.get("estado"),
        })
        .execute()
    )

    relatorio_id = resp_rel.data[0]["id"]

    linhas_insert = []
    vinculos_insert = []

    for l in linhas:
        linhas_insert.append({
            "relatorio_id": relatorio_id,
            "fonte": l.get("fonte"),
            "nit": l.get("nit"),
            "competencia_literal": l.get("competencia_literal"),
            "competencia_date": l.get("competencia_date"),
            "competencia_ano": l.get("competencia_ano"),
            "competencia_mes": l.get("competencia_mes"),
            "documento_tomador": l.get("documento_tomador"),
            "documento_tomador_tipo": l.get("documento_tomador_tipo"),
            "fpas": l.get("fpas"),
            "categoria_codigo": l.get("categoria_codigo"),
            "remuneracao": l.get("remuneracao"),
            "extemporaneo": l.get("extemporaneo"),
        })

        empresa_id = get_or_create_empresa(l.get("documento_tomador"))

        if empresa_id:
            vinculos_insert.append({
                "empresa_id": empresa_id,
                "segurado_id": segurado_id,
                "relatorio_id": relatorio_id,
                "competencia_ano": l.get("competencia_ano"),
                "competencia_mes": l.get("competencia_mes"),
                "categoria_codigo": l.get("categoria_codigo"),
                "fpas": l.get("fpas"),
                "remuneracao": l.get("remuneracao"),
                "extemporaneo": l.get("extemporaneo"),
            })

    if linhas_insert:
        supabase.table("ci_gfip_linhas").insert(linhas_insert).execute()

    if vinculos_insert:
        supabase.table("ci_gfip_empresas_vinculos").insert(vinculos_insert).execute()

    return {
        "success": True,
        "status": "sucesso",
        "relatorio_id": relatorio_id,
        "linhas_salvas": len(linhas_insert),
        "vinculos_salvos": len(vinculos_insert),
    }

# ============================================
# ENDPOINT – PROCESSAR CI GFIP
# ============================================

@app.post("/ci-gfip/processar")
async def processar_ci_gfip(
    arquivo: UploadFile = File(...),
    profissao: str = Form(""),
    estado: str = Form(""),
):
    conteudo = await arquivo.read()
    if not conteudo:
        return {
            "success": False,
            "status": "erro",
            "mensagem": "Arquivo PDF vazio."
        }

    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        tmp.write(conteudo)
        caminho_pdf = tmp.name

    texto = ""
    with pdfplumber.open(caminho_pdf) as pdf:
        for pagina in pdf.pages:
            texto += (pagina.extract_text() or "") + "\n"

    layout = detectar_layout_ci_gfip(texto)
    resultado = parse_ci_gfip(texto)

    if resultado.get("erro"):
        return {
            "success": False,
            "status": "erro",
            "mensagem": resultado["erro"]
        }

    # Injeta profissão e estado no cabeçalho
    cab = resultado.get("cabecalho", {}) or {}
    cab["profissao"] = profissao.strip()
    cab["estado"] = estado.strip()
    resultado["cabecalho"] = cab

    return salvar_relatorio_completo(
        parser=resultado,
        arquivo_nome=arquivo.filename,
        arquivo_bytes=conteudo,
        modelo=layout,
    )
