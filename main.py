import re
import os
import hashlib
import tempfile
import requests

import pdfplumber
from fastapi import FastAPI, File, UploadFile, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from supabase import create_client, Client
from parsers.ci_gfip_universal import (
    parse_ci_gfip,
    detectar_layout_ci_gfip
)

# =====================================================
# APP
# =====================================================

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://lovable.dev"],
    allow_credentials=False,
    allow_methods=["POST", "OPTIONS"],
    allow_headers=["*"],
)

# =====================================================
# SUPABASE
# =====================================================

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client | None = None
if SUPABASE_URL and SUPABASE_KEY:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# =====================================================
# HELPERS
# =====================================================

def so_numeros(valor: str | None) -> str:
    return re.sub(r"\D", "", valor or "")

def calcular_hash(conteudo: bytes) -> str:
    return hashlib.sha256(conteudo).hexdigest()

# =====================================================
# SEGURADO
# =====================================================

def get_or_create_segurado(cab: dict) -> str | None:
    if supabase is None:
        return None

    nome = (cab.get("nome") or "").strip()
    nit = so_numeros(cab.get("nit"))

    if not nome:
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

    r = (
        supabase.table("ci_gfip_segurados")
        .insert({
            "nome": nome,
            "data_nascimento": cab.get("data_nascimento"),
            "nome_mae": cab.get("nome_mae"),
            "nit_principal": nit or None,
        })
        .execute()
    )

    segurado_id = r.data[0]["id"]

    if nit:
        supabase.table("ci_gfip_segurado_nits").insert({
            "segurado_id": segurado_id,
            "nit": nit
        }).execute()

    return segurado_id

# =====================================================
# EMPRESAS (APENAS CADASTRO CENTRAL)
# =====================================================

def get_or_create_empresa(doc: str | None):
    if supabase is None:
        return

    numeros = so_numeros(doc)
    if not numeros:
        return

    raiz = numeros[:8].zfill(8)
    cnpj = numeros[:14] if len(numeros) >= 14 else None

    r = (
        supabase.table("empresas")
        .select("id, cnpj")
        .eq("raiz_cnpj", raiz)
        .limit(1)
        .execute()
    )

    if r.data:
        empresa = r.data[0]
        if cnpj and not empresa.get("cnpj"):
            supabase.table("empresas").update({
                "cnpj": cnpj,
                "atualizado_em": "now()"
            }).eq("id", empresa["id"]).execute()
        return

    supabase.table("empresas").insert({
        "raiz_cnpj": raiz,
        "cnpj": cnpj,
        "origem_inicial": "CI_GFIP"
    }).execute()

# =====================================================
# SALVAR RELATÓRIO
# =====================================================

def salvar_relatorio(parser: dict, nome_arquivo: str, conteudo: bytes, modelo: str):

    cab = parser.get("cabecalho", {})
    linhas = parser.get("linhas", [])

    segurado_id = get_or_create_segurado(cab)
    if not segurado_id:
        raise HTTPException(
            status_code=400,
            detail="Não foi possível identificar o segurado no relatório."
        )

    rel = (
        supabase.table("ci_gfip_relatorios")
        .insert({
            "segurado_id": segurado_id,
            "tipo_relatorio": "ci_gfip",
            "modelo_relatorio": modelo,
            "arquivo_storage_path": nome_arquivo,
            "hash_documento": calcular_hash(conteudo),
            "profissao": cab.get("profissao"),
            "estado": cab.get("estado"),
        })
        .execute()
    )

    relatorio_id = rel.data[0]["id"]

    linhas_insert = []
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
            "codigo_gfip": l.get("codigo_gfip"),
            "remuneracao": l.get("remuneracao"),
            "extemporaneo": l.get("extemporaneo"),
        })

        get_or_create_empresa(l.get("documento_tomador"))

    if linhas_insert:
        supabase.table("ci_gfip_linhas").insert(linhas_insert).execute()

    return {"status": "sucesso"}

# =====================================================
# ENDPOINT
# =====================================================

@app.post("/ci-gfip/processar")
async def processar_ci_gfip(
    arquivo: UploadFile = File(...),
    profissao: str = Form(""),
    estado: str = Form(""),
):

    conteudo = await arquivo.read()
    if not conteudo:
        raise HTTPException(status_code=400, detail="Arquivo vazio.")

    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        tmp.write(conteudo)
        caminho = tmp.name

    texto = ""
    with pdfplumber.open(caminho) as pdf:
        for p in pdf.pages:
            texto += (p.extract_text() or "") + "\n"

    resultado = parse_ci_gfip(texto)
    if resultado.get("erro"):
        raise HTTPException(status_code=400, detail="Erro ao interpretar o CI GFIP.")

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
