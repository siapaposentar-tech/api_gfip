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

# ==============================
# APP
# ==============================

app = FastAPI()

# 👉 CORS CORRETO PARA O LOVABLE
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://lovable.dev"],
    allow_credentials=False,
    allow_methods=["POST", "OPTIONS"],
    allow_headers=["*"],
)

# ==============================
# SUPABASE
# ==============================

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client | None = None
if SUPABASE_URL and SUPABASE_KEY:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ==============================
# HELPERS
# ==============================

def so_numeros(valor: str | None) -> str:
    return re.sub(r"\D", "", valor or "")

def calcular_hash_arquivo(conteudo: bytes) -> str:
    return hashlib.sha256(conteudo).hexdigest()

# ==============================
# BRASILAPI (opcional)
# ==============================

def consultar_brasilapi_cnpj(cnpj: str) -> dict | None:
    try:
        resp = requests.get(
            f"https://brasilapi.com.br/api/cnpj/v1/{cnpj}",
            timeout=10
        )
        if resp.status_code != 200:
            return None

        data = resp.json()
        return {
            "nome": data.get("razao_social") or data.get("nome_fantasia")
        }
    except:
        return None

# ==============================
# SEGURADO
# ==============================

def get_or_create_segurado(cab: dict) -> str | None:
    nit = so_numeros(cab.get("nit"))
    nome = (cab.get("nome") or "").strip()

    if not nome or supabase is None:
        return None

    if nit:
        r = (
            supabase.table("ci_gfip_segurado_nits")
            .select("segurado_id")
            .eq("nit", nit)
            .execute()
        )
        if r.data:
            return r.data[0]["segurado_id"]

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

# ==============================
# EMPRESAS (tabela central)
# ==============================

def get_or_create_empresa(doc: str | None) -> str | None:
    if supabase is None:
        return None

    numeros = so_numeros(doc)
    if not numeros:
        return None

    if len(numeros) >= 14:
        raiz = numeros[:8]
        cnpj = numeros[:14]
    else:
        raiz = numeros.zfill(8)
        cnpj = None

    r = (
        supabase.table("empresas")
        .select("id, cnpj, nome")
        .eq("raiz_cnpj", raiz)
        .limit(1)
        .execute()
    )

    if r.data:
        empresa = r.data[0]
        updates = {}

        if cnpj and not empresa.get("cnpj"):
            updates["cnpj"] = cnpj

        if cnpj and not empresa.get("nome"):
            info = consultar_brasilapi_cnpj(cnpj)
            if info and info.get("nome"):
                updates["nome"] = info["nome"]

        if updates:
            updates["atualizado_em"] = "now()"
            supabase.table("empresas").update(updates).eq(
                "id", empresa["id"]
            ).execute()

        return empresa["id"]

    data = {
        "raiz_cnpj": raiz,
        "cnpj": cnpj,
        "origem_inicial": "CI_GFIP",
    }

    if cnpj:
        info = consultar_brasilapi_cnpj(cnpj)
        if info and info.get("nome"):
            data["nome"] = info["nome"]

    r2 = supabase.table("empresas").insert(data).execute()
    return r2.data[0]["id"]

# ==============================
# SALVAR RELATÓRIO
# ==============================

def salvar_relatorio(parser: dict, nome_arquivo: str, conteudo: bytes, modelo: str):
    cab = parser.get("cabecalho", {})
    linhas = parser.get("linhas", [])

    segurado_id = get_or_create_segurado(cab)
    hash_doc = calcular_hash_arquivo(conteudo)

    r = (
        supabase.table("ci_gfip_relatorios")
        .insert({
            "segurado_id": segurado_id,
            "tipo_relatorio": "ci_gfip",
            "modelo_relatorio": modelo,
            "arquivo_storage_path": nome_arquivo,
            "hash_documento": hash_doc,
            "profissao": cab.get("profissao"),
            "estado": cab.get("estado"),
        })
        .execute()
    )

    relatorio_id = r.data[0]["id"]

    for l in linhas:
        empresa_id = get_or_create_empresa(l.get("documento_tomador"))
        if empresa_id:
            supabase.table("ci_gfip_empresas_vinculos").insert({
                "empresa_id": empresa_id,
                "segurado_id": segurado_id,
                "relatorio_id": relatorio_id,
                "competencia_ano": l.get("competencia_ano"),
                "competencia_mes": l.get("competencia_mes"),
            }).execute()

    return {
        "status": "sucesso"
    }

# ==============================
# ENDPOINT
# ==============================

@app.post("/ci-gfip/processar")
async def processar_ci_gfip(
    arquivo: UploadFile = File(...),
    profissao: str = Form(""),
    estado: str = Form(""),
):
    conteudo = await arquivo.read()
    if not conteudo:
        return {"status": "erro"}

    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        tmp.write(conteudo)
        caminho = tmp.name

    texto = ""
    with pdfplumber.open(caminho) as pdf:
        for p in pdf.pages:
            texto += (p.extract_text() or "") + "\n"

    resultado = parse_ci_gfip(texto)
    if resultado.get("erro"):
        return {"status": "erro"}

    cab = resultado.get("cabecalho", {})
    cab["profissao"] = profissao
    cab["estado"] = estado
    resultado["cabecalho"] = cab

    return salvar_relatorio(
        resultado,
        arquivo.filename,
        conteudo,
        detectar_layout_ci_gfip(texto)
    )

# ==============================
# OPTIONS (preflight)
# ==============================

@app.options("/ci-gfip/processar")
async def options_ci_gfip():
    return {}
