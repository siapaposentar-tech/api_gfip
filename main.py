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


def to_bool(value: str | None):
    if not value:
        return None
    txt = value.strip().lower()
    if txt == "sim":
        return True
    if txt in ["não", "nao"]:
        return False
    return None


# ============================================
# SEGURADOS
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

    # Cria segurado
    resp = (
        supabase.table("ci_gfip_segurados")
        .insert(
            {
                "nome": nome,
                "data_nascimento": cab.get("data_nascimento"),
                "nome_mae": cab.get("nome_mae"),
                "nit_principal": nit if nit else None,
            }
        )
        .execute()
    )

    segurado_id = resp.data[0]["id"]

    if nit:
        supabase.table("ci_gfip_segurado_nits").insert(
            {"segurado_id": segurado_id, "nit": nit}
        ).execute()

    return segurado_id


# ============================================
# SALVAR NO SUPABASE
# ============================================

def salvar_ci_gfip_no_supabase(parser: dict, arquivo_nome: str, arquivo_bytes: bytes, modelo: str):

    if supabase is None:
        return None

    # Cabeçalho + Linhas
    cab = parser.get("cabecalho", {}) or {}
    linhas = parser.get("linhas", []) or []

    segurado_id = get_or_create_segurado(cab)
    if not segurado_id:
        return None

    hash_doc = calcular_hash_arquivo(arquivo_bytes)

    # Salva relatório
    resp_rel = (
        supabase.table("ci_gfip_relatorios")
        .insert(
            {
                "segurado_id": segurado_id,
                "tipo_relatorio": "ci_gfip",
                "modelo_relatorio": modelo,
                "arquivo_storage_path": arquivo_nome,
                "hash_documento": hash_doc,
                "profissao": cab.get("profissao"),
                "estado": cab.get("estado"),
            }
        )
        .execute()
    )

    relatorio_id = resp_rel.data[0]["id"]

    # Insere linhas
    linhas_insert = []

    for l in linhas:
        linhas_insert.append(
            {
                "relatorio_id": relatorio_id,

                # Campos padrão
                "fonte": l.get("fonte"),
                "nit": l.get("nit"),

                # Competência
                "competencia_literal": l.get("competencia_literal"),
                "competencia_date": l.get("competencia_date"),
                "competencia_ano": l.get("competencia_ano"),
                "competencia_mes": l.get("competencia_mes"),

                # Tomador
                "documento_tomador": l.get("documento_tomador"),
                "documento_tomador_tipo": l.get("documento_tomador_tipo") or "",

                # FPAS / Categoria / Código GFIP
                "fpas": l.get("fpas"),
                "categoria_codigo": l.get("categoria_codigo"),
                "codigo_gfip": l.get("codigo_gfip"),

                # Datas
                "data_envio_literal": l.get("data_envio_literal"),
                "data_envio_date": l.get("data_envio_date"),

                # Campos novos do parser universal
                "numero_documento": l.get("numero_documento"),
                "tipo_remuneracao": l.get("tipo_remuneracao"),

                # Valores
                "remuneracao_literal": l.get("remuneracao_literal"),
                "remuneracao": l.get("remuneracao"),
                "valor_retido_literal": l.get("valor_retido_literal"),
                "valor_retido": l.get("valor_retido"),

                # Extemporâneo
                "extemporaneo_literal": l.get("extemporaneo_literal"),
                "extemporaneo": l.get("extemporaneo"),
            }
        )

    if linhas_insert:
        supabase.table("ci_gfip_linhas").insert(linhas_insert).execute()

    return {
        "segurado_id": segurado_id,
        "relatorio_id": relatorio_id,
        "linhas_salvas": len(linhas_insert),
    }


# ============================================
# ROTA PRINCIPAL
# ============================================

@app.post("/ci-gfip/processar")
async def processar_ci_gfip(
    arquivo: UploadFile = File(...),
    profissao: str = "",
    estado: str = "",
):

    conteudo = await arquivo.read()

    if not conteudo:
        raise HTTPException(status_code=400, detail="Arquivo PDF vazio.")

    # Arquivo temporário
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        tmp.write(conteudo)
        caminho_pdf = tmp.name

    # Extração de texto
    texto = ""
    with pdfplumber.open(caminho_pdf) as pdf:
        for pagina in pdf.pages:
            texto += (pagina.extract_text() or "") + "\n"

    # Detectar layout
    layout = detectar_layout_ci_gfip(texto)

    # Parse geral
    resultado = parse_ci_gfip(texto)

    # Tratamento de erro do parser
    if resultado.get("erro"):
        raise HTTPException(400, f"Erro no parser: {resultado['erro']}")

    # Aplicar profissão / estado
    cab = resultado.get("cabecalho", {})
    cab["profissao"] = profissao.strip()
    cab["estado"] = estado.strip()
    resultado["cabecalho"] = cab

    # Salvar no Supabase
    info_supabase = salvar_ci_gfip_no_supabase(
        parser=resultado,
        arquivo_nome=arquivo.filename,
        arquivo_bytes=conteudo,
        modelo=layout,
    )

    return {
        "status": "sucesso",
        "mensagem": "CI GFIP processada com sucesso.",
        "layout_detectado": layout,
        "cabecalho": resultado.get("cabecalho"),
        "total_linhas": len(resultado.get("linhas", [])),
        "arquivo": arquivo.filename,
        "supabase": info_supabase,
    }
