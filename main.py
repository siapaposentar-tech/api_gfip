import re
import os
import hashlib
import tempfile
from datetime import datetime
from decimal import Decimal
import json

import pdfplumber
from fastapi import FastAPI, File, UploadFile, HTTPException, Form
from fastapi.middleware.cors import CORSMiddleware

from supabase import create_client, Client
from parsers.ci_gfip_universal import (
    parse_ci_gfip,
    detectar_layout_ci_gfip,
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


def gerar_hash_conteudo(cabecalho: dict, linhas: list[dict]) -> str:
    """
    Gera um hash baseado no CONTEÚDO previdenciário, não no PDF.
    Dois relatórios com o mesmo conteúdo previdenciário produzem o mesmo hash.
    """

    cab_norm = {
        "nome": (cabecalho.get("nome") or "").strip().upper(),
        "nit": (cabecalho.get("nit") or "").strip(),
        "data_nascimento": cabecalho.get("data_nascimento"),
        "nome_mae": (cabecalho.get("nome_mae") or "").strip().upper(),
    }

    linhas_norm = []
    for l in linhas:
        linhas_norm.append(
            {
                "ano": l.get("competencia_ano"),
                "mes": l.get("competencia_mes"),
                "documento_tomador": l.get("documento_tomador"),
                "categoria": l.get("categoria_codigo"),
                "remuneracao": float(l.get("remuneracao") or 0),
                "extemporaneo": l.get("extemporaneo"),
            }
        )

    # Ordena para garantir estabilidade do hash
    linhas_norm = sorted(
        linhas_norm,
        key=lambda x: (x["ano"], x["mes"], x["documento_tomador"]),
    )

    estrutura = {
        "cabecalho": cab_norm,
        "linhas": linhas_norm,
    }

    txt = json.dumps(estrutura, sort_keys=True)
    return hashlib.sha256(txt.encode("utf-8")).hexdigest()


def comparar_competencias(
    linhas_antigas: list[dict], linhas_novas: list[dict]
):
    """
    Compara as competências do relatório anterior com o novo relatório.

    Retorna:
    - complementos: competências novas (não existiam antes)
    - retificacoes: competências que existiam, mas com valor diferente
    - iguais: competências idênticas
    """

    def chave_linha(l: dict):
        return (
            l.get("competencia_ano"),
            l.get("competencia_mes"),
            l.get("documento_tomador"),
        )

    mapa_antigas = {chave_linha(l): l for l in linhas_antigas}

    complementos: list[dict] = []
    retificacoes: list[dict] = []
    iguais: list[dict] = []

    for l in linhas_novas:
        chave = chave_linha(l)
        antiga = mapa_antigas.get(chave)

        if antiga is None:
            # Competência nova (complemento)
            complementos.append(l)
            continue

        antigo_valor = float(antiga.get("remuneracao") or 0)
        novo_valor = float(l.get("remuneracao") or 0)

        if antigo_valor != novo_valor:
            # Retificação de competência
            retificacoes.append(
                {
                    "competencia_ano": l.get("competencia_ano"),
                    "competencia_mes": l.get("competencia_mes"),
                    "competencia_date": l.get("competencia_date"),
                    "competencia_literal": l.get("competencia_literal"),
                    "valor_antigo": antigo_valor,
                    "valor_novo": novo_valor,
                    "valor_antigo_literal": antiga.get("remuneracao_literal"),
                    "valor_novo_literal": l.get("remuneracao_literal"),
                }
            )
        else:
            # Igual (sem alteração)
            iguais.append(l)

    return complementos, retificacoes, iguais


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

    # Busca segurado por NIT nos NITs armazenados
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
            }
        )
        .execute()
    )

    segurado_id = resp.data[0]["id"]

    # Registrar NIT
    if nit:
        supabase.table("ci_gfip_segurado_nits").insert(
            {"segurado_id": segurado_id, "nit": nit}
        ).execute()

    return segurado_id


# ============================================
# ENDPOINT PRINCIPAL — PROCESSAR CI GFIP
# ============================================

@app.post("/ci-gfip/processar")
async def processar_ci_gfip(
    arquivo: UploadFile = File(...),
    profissao: str = Form(""),
    estado: str = Form(""),
):
    if supabase is None:
        raise HTTPException(
            status_code=500, detail="Supabase não configurado na API."
        )

    conteudo = await arquivo.read()
    if not conteudo:
        raise HTTPException(status_code=400, detail="Arquivo PDF vazio.")

    # Arquivo temporário
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        tmp.write(conteudo)
        caminho_pdf = tmp.name

    # Extração do texto
    texto = ""
    with pdfplumber.open(caminho_pdf) as pdf:
        for pagina in pdf.pages:
            texto += (pagina.extract_text() or "") + "\n"

    # Detectar layout
    layout = detectar_layout_ci_gfip(texto)

    # Parse geral
    resultado = parse_ci_gfip(texto)
    if resultado.get("erro"):
        raise HTTPException(400, f"Erro no parser: {resultado['erro']}")

    cab = resultado.get("cabecalho", {}) or {}
    linhas = resultado.get("linhas", []) or []

    # Aplicar profissão / estado no cabeçalho
    cab["profissao"] = (profissao or "").strip()
    cab["estado"] = (estado or "").strip()
    resultado["cabecalho"] = cab

    # Identificar / criar segurado
    segurado_id = get_or_create_segurado(cab)
    if not segurado_id:
        raise HTTPException(
            400, "Não foi possível identificar ou criar o segurado."
        )

    # GERAR HASH DE CONTEÚDO (para detectar duplicidade real)
    hash_conteudo = gerar_hash_conteudo(cab, linhas)

    # Verificar se já existe relatório idêntico para este segurado
    r_dup = (
        supabase.table("ci_gfip_relatorios")
        .select("id")
        .eq("segurado_id", segurado_id)
        .eq("hash_conteudo", hash_conteudo)
        .execute()
    )

    if r_dup.data:
        return {
            "status": "duplicado",
            "mensagem": "Relatório idêntico já existe para este segurado. Upload ignorado.",
            "segurado_id": segurado_id,
            "hash_conteudo": hash_conteudo,
        }

    # Buscar RELATÓRIO ANTERIOR deste segurado (para comparar competências)
    rel_ant = (
        supabase.table("ci_gfip_relatorios")
        .select("id")
        .eq("segurado_id", segurado_id)
        .order("criado_em", desc=True)
        .limit(1)
        .execute()
    )

    linhas_antigas: list[dict] = []

    if rel_ant.data:
        relatorio_ant_id = rel_ant.data[0]["id"]
        resp_linhas_ant = (
            supabase.table("ci_gfip_linhas")
            .select("*")
            .eq("relatorio_id", relatorio_ant_id)
            .execute()
        )
        linhas_antigas = resp_linhas_ant.data or []

    # Se existir relatório anterior, comparamos competências
    complementos: list[dict] = []
    retificacoes: list[dict] = []
    iguais: list[dict] = []

    if linhas_antigas:
        complementos, retificacoes, iguais = comparar_competencias(
            linhas_antigas, linhas
        )

    # Salvar novo relatório CI GFIP
    hash_pdf = calcular_hash_arquivo(conteudo)

    resp_rel = (
        supabase.table("ci_gfip_relatorios")
        .insert(
            {
                "segurado_id": segurado_id,
                "tipo_relatorio": "ci_gfip",
                "modelo_relatorio": layout,
                "arquivo_storage_path": arquivo.filename,
                "hash_documento": hash_pdf,
                "hash_conteudo": hash_conteudo,
                "profissao": cab.get("profissao"),
                "estado": cab.get("estado"),
            }
        )
        .execute()
    )

    relatorio_id = resp_rel.data[0]["id"]

    # Registrar COMPLEMENTAÇÕES (se houver relatório anterior)
    for c in complementos:
        supabase.table("ci_gfip_complementacoes").insert(
            {
                "segurado_id": segurado_id,
                "relatorio_id": relatorio_id,
                "competencia_ano": c.get("competencia_ano"),
                "competencia_mes": c.get("competencia_mes"),
                "competencia_date": c.get("competencia_date"),
                "competencia_literal": c.get("competencia_literal"),
                "valor": c.get("remuneracao"),
                "valor_literal": c.get("remuneracao_literal"),
            }
        ).execute()

    # Registrar RETIFICAÇÕES
    for r in retificacoes:
        supabase.table("ci_gfip_retificacoes").insert(
            {
                "segurado_id": segurado_id,
                "relatorio_id": relatorio_id,
                "competencia_ano": r.get("competencia_ano"),
                "competencia_mes": r.get("competencia_mes"),
                "competencia_date": r.get("competencia_date"),
                "competencia_literal": r.get("competencia_literal"),
                "valor_antigo": r.get("valor_antigo"),
                "valor_novo": r.get("valor_novo"),
                "valor_antigo_literal": r.get("valor_antigo_literal"),
                "valor_novo_literal": r.get("valor_novo_literal"),
            }
        ).execute()

    # Inserir todas as linhas deste novo relatório
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
        "status": "processado",
        "mensagem": "CI GFIP processada com sucesso.",
        "layout_detectado": layout,
        "cabecalho": resultado.get("cabecalho"),
        "total_linhas": len(linhas),
        "arquivo": arquivo.filename,
        "segurado_id": segurado_id,
        "relatorio_id": relatorio_id,
        "complementos": len(complementos),
        "retificacoes": len(retificacoes),
        "iguais": len(iguais),
    }
