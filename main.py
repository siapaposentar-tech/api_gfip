import re
import os
import hashlib
import tempfile
from datetime import datetime
from decimal import Decimal

import pdfplumber
from fastapi import FastAPI, File, UploadFile, HTTPException, Form
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


def normalizar_linha_para_comparacao(linha: dict) -> tuple:
    """
    Transforma uma linha da GFIP em tupla ordenável para comparação.
    Ignora campos que não definem a competência em si.
    """
    return (
        linha.get("competencia_ano"),
        linha.get("competencia_mes"),
        linha.get("competencia_literal"),
        linha.get("documento_tomador"),
        linha.get("categoria_codigo"),
        linha.get("codigo_gfip"),
        linha.get("remuneracao"),
        linha.get("valor_retido"),
        linha.get("extemporaneo"),
    )


def conjuntos_sao_iguais(linhas1: list, linhas2: list) -> bool:
    """
    Verifica se dois conjuntos de competências são idênticos.
    """
    set1 = {normalizar_linha_para_comparacao(x) for x in linhas1}
    set2 = {normalizar_linha_para_comparacao(x) for x in linhas2}
    return set1 == set2


# ============================================
# DUPLICIDADE POR CONTEÚDO
# ============================================

def relatorio_ja_existe_por_conteudo(segurado_id: str, novas_linhas: list) -> dict | None:
    """
    Verifica se já existe um relatório com o mesmo conteúdo (duplicado)
    ou se existe um relatório com diferenças (complemento / retificador).
    """

    if supabase is None:
        return None

    # Buscar relatórios existentes deste segurado
    resp = (
        supabase.table("ci_gfip_relatorios")
        .select("id")
        .eq("segurado_id", segurado_id)
        .order("criado_em", desc=False)
        .execute()
    )

    relatorios = resp.data or []

    for rel in relatorios:
        rel_id = rel["id"]

        # Buscar linhas deste relatório
        linhas_antigas = (
            supabase.table("ci_gfip_linhas")
            .select(
                "competencia_ano, competencia_mes, competencia_literal, "
                "documento_tomador, categoria_codigo, codigo_gfip, "
                "remuneracao, valor_retido, extemporaneo"
            )
            .eq("relatorio_id", rel_id)
            .execute()
        ).data or []

        # Comparação de duplicidade
        if conjuntos_sao_iguais(linhas_antigas, novas_linhas):
            return {
                "status": "duplicado",
                "relatorio_id": rel_id
            }

    return {"status": "novo"}


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

    # Buscar segurado por NITs adicionais
    if nit:
        r = (
            supabase.table("ci_gfip_segurado_nits")
            .select("segurado_id")
            .eq("nit", nit)
            .execute()
        )
        if r.data:
            return r.data[0]["segurado_id"]

    # Criar segurado
    resp = (
        supabase.table("ci_gfip_segurados")
        .insert(
            {
                "nome": nome,
                "data_nascimento": cab.get("data_nascimento"),
                "nome_mae": cab.get("nome_mae"),
                "nit_principal": nit if nit else None,
                "profissao": cab.get("profissao"),
                "estado": cab.get("estado"),
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
# SALVAR RELATÓRIO NO SUPABASE
# ============================================

def salvar_relatorio_completo(parser: dict, arquivo_nome: str, arquivo_bytes: bytes, modelo: str):

    cab = parser.get("cabecalho", {}) or {}
    linhas = parser.get("linhas", []) or []

    segurado_id = get_or_create_segurado(cab)
    if not segurado_id:
        return None

    # DUPLICIDADE POR CONTEÚDO
    check = relatorio_ja_existe_por_conteudo(segurado_id, linhas)

    if check and check["status"] == "duplicado":
        return {
            "status": "duplicado",
            "mensagem": "Relatório já existe — nenhuma ação necessária.",
            "relatorio_existente_id": check["relatorio_id"]
        }

    # SALVAR RELATÓRIO NOVO
    hash_doc = calcular_hash_arquivo(arquivo_bytes)

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

    # INSERIR LINHAS
    linhas_insert = []
    for l in linhas:
        linhas_insert.append(
            {
                "relatorio_id": relatorio_id,
                "fonte": l.get("fonte"),
                "nit": l.get("nit"),
                "competencia_literal": l.get("competencia_literal"),
                "competencia_date": l.get("competencia_date"),
                "competencia_ano": l.get("competencia_ano"),
                "competencia_mes": l.get("competencia_mes"),
                "documento_tomador": l.get("documento_tomador"),
                "documento_tomador_tipo": l.get("documento_tomador_tipo") or "",
                "fpas": l.get("fpas"),
                "categoria_codigo": l.get("categoria_codigo"),
                "codigo_gfip": l.get("codigo_gfip"),
                "data_envio_literal": l.get("data_envio_literal"),
                "data_envio_date": l.get("data_envio_date"),
                "numero_documento": l.get("numero_documento"),
                "tipo_remuneracao": l.get("tipo_remuneracao"),
                "remuneracao_literal": l.get("remuneracao_literal"),
                "remuneracao": l.get("remuneracao"),
                "valor_retido_literal": l.get("valor_retido_literal"),
                "valor_retido": l.get("valor_retido"),
                "extemporaneo_literal": l.get("extemporaneo_literal"),
                "extemporaneo": l.get("extemporaneo"),
            }
        )

    if linhas_insert:
        supabase.table("ci_gfip_linhas").insert(linhas_insert).execute()

    return {
        "status": "novo",
        "segurado_id": segurado_id,
        "relatorio_id": relatorio_id,
        "linhas_salvas": len(linhas_insert),
    }


# ============================================
# ENDPOINT PRINCIPAL — PROCESSAR CI GFIP
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
            "status": "erro",
            "mensagem": "Arquivo PDF vazio."
        }

    # Arquivo temporário
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        tmp.write(conteudo)
        caminho_pdf = tmp.name

    # Extrair texto
    texto = ""
    with pdfplumber.open(caminho_pdf) as pdf:
        for pagina in pdf.pages:
            texto += (pagina.extract_text() or "") + "\n"

    # Detectar layout
    layout = detectar_layout_ci_gfip(texto)

    # Parse principal
    resultado = parse_ci_gfip(texto)

    if resultado.get("erro"):
        return {
            "status": "erro",
            "mensagem": f"Erro no parser: {resultado['erro']}"
        }

    # Inserir profissão e estado no cabeçalho
    cab = resultado.get("cabecalho", {})
    cab["profissao"] = profissao.strip()
    cab["estado"] = estado.strip()
    resultado["cabecalho"] = cab

    # Salvar no Supabase
    save = salvar_relatorio_completo(
        parser=resultado,
        arquivo_nome=arquivo.filename,
        arquivo_bytes=conteudo,
        modelo=layout,
    )

    # RESPOSTA SUAVE PARA DUPLICIDADE
    if save["status"] == "duplicado":
        return {
            "status": "duplicado",
            "mensagem": "Relatório já existe — nenhuma ação necessária.",
            "relatorio_existente_id": save["relatorio_existente_id"]
        }

    return {
        "status": "sucesso",
        "mensagem": "CI GFIP processada com sucesso.",
        "layout_detectado": layout,
        "cabecalho": resultado.get("cabecalho"),
        "total_linhas": resultado.get("linhas") and len(resultado["linhas"]),
        "arquivo": arquivo.filename,
        "supabase": save,
    }
