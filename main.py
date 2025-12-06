import re
import os
import hashlib
import tempfile
import requests

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
# HELPERS GERAIS
# ============================================

def so_numeros(valor: str | None) -> str:
    return re.sub(r"\D", "", valor or "")


def calcular_hash_arquivo(conteudo: bytes) -> str:
    return hashlib.sha256(conteudo).hexdigest()


# ============================================
# CONSULTA À BRASILAPI
# ============================================

def consultar_brasilapi_cnpj(cnpj: str) -> dict | None:
    """
    Consulta o CNPJ na BrasilAPI.
    Retorna None se a API falhar ou o CNPJ estiver inválido.
    """
    try:
        url = f"https://brasilapi.com.br/api/cnpj/v1/{cnpj}"
        resp = requests.get(url, timeout=10)

        if resp.status_code != 200:
            return None

        data = resp.json()
        return {
            "razao_social": data.get("razao_social"),
            "nome_fantasia": data.get("nome_fantasia"),
            "cnae_principal": data.get("cnae_fiscal_descricao"),
            "natureza_juridica": data.get("natureza_juridica"),
            "endereco": f"{data.get('logradouro', '')}, {data.get('numero', '')}, {data.get('bairro', '')}, {data.get('municipio', '')}-{data.get('uf', '')}, CEP {data.get('cep', '')}",
            "telefone": data.get("telefone"),
            "situacao_cadastral": data.get("situacao_cadastral"),
            "data_abertura": data.get("data_inicio_atividade"),
        }

    except:
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

    # Busca segurado pelos NITs adicionais
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
# EMPRESAS (CNPJ / RAIZ / DOMÉSTICO)
# ============================================

def classificar_documento_tomador(doc_bruto: str | None) -> dict | None:
    if not doc_bruto:
        return None

    doc = so_numeros(doc_bruto)
    if not doc:
        return None

    if len(doc) == 11:
        return {
            "tipo": "empregador_domestico",
            "cnpj_raiz": doc,
            "cnpj_completo": None,
        }

    if len(doc) == 14:
        return {
            "tipo": "cnpj_completo",
            "cnpj_raiz": doc[:8],
            "cnpj_completo": doc,
        }

    if len(doc) == 8:
        return {
            "tipo": "cnpj_raiz",
            "cnpj_raiz": doc,
            "cnpj_completo": None,
        }

    return None


def get_or_create_empresa(doc_tomador: str | None) -> str | None:
    if supabase is None:
        return None

    info = classificar_documento_tomador(doc_tomador)
    if not info:
        return None

    tipo = info["tipo"]
    raiz = info["cnpj_raiz"]
    cnpj_completo = info["cnpj_completo"]

    # Buscar empresa pela raiz
    resp = (
        supabase.table("ci_gfip_empresas")
        .select("*")
        .eq("cnpj_raiz", raiz)
        .limit(1)
        .execute()
    )
    dados = resp.data or []

    # Caso a empresa já exista
    if dados:
        empresa = dados[0]
        updates = {}

        # Se vier CNPJ completo pela primeira vez
        if tipo == "cnpj_completo":
            ordem = cnpj_completo[8:12]

            if not empresa.get("cnpj_completo"):
                updates["cnpj_completo"] = cnpj_completo

            if ordem == "0001":  # matriz
                updates["tipo_empresa"] = "matriz"
                updates["cnpj_referencia"] = cnpj_completo
            else:  # filial
                if not empresa.get("cnpj_referencia"):
                    updates["cnpj_referencia"] = cnpj_completo
                updates["tipo_empresa"] = "filial"

            # CONSULTA BRASILAPI
            enriched = consultar_brasilapi_cnpj(cnpj_completo)
            if enriched:
                for campo, valor in enriched.items():
                    if valor:
                        updates[campo] = valor

        if updates:
            updates["atualizado_em"] = "now()"
            supabase.table("ci_gfip_empresas").update(updates).eq("id", empresa["id"]).execute()

        return empresa["id"]

    # Criar nova empresa (nunca existiu)
    insert_data = {
        "cnpj_raiz": raiz,
        "cnpj_completo": cnpj_completo,
        "tipo_empresa": "nao_identificada",
        "status": "ativa",
        "cnpj_referencia": None,
    }

    if tipo == "cnpj_completo":
        ordem = cnpj_completo[8:12]
        insert_data["cnpj_referencia"] = cnpj_completo

        if ordem == "0001":
            insert_data["tipo_empresa"] = "matriz"
        else:
            insert_data["tipo_empresa"] = "filial"

        # CONSULTA BRASILAPI
        enriched = consultar_brasilapi_cnpj(cnpj_completo)
        if enriched:
            for campo, valor in enriched.items():
                insert_data[campo] = valor

    elif tipo == "cnpj_raiz":
        insert_data["tipo_empresa"] = "raiz_incompleta"
        insert_data["status"] = "provisoria"

    elif tipo == "empregador_domestico":
        insert_data["tipo_empresa"] = "empregador_domestico"
        insert_data["status"] = "ativa"

    # Inserir empresa nova
    resp_new = (
        supabase.table("ci_gfip_empresas")
        .insert(insert_data)
        .execute()
    )

    return resp_new.data[0]["id"] if resp_new.data else None


# ============================================
# VÍNCULOS + LINHAS
# ============================================

from datetime import date

def salvar_relatorio_completo(parser: dict, arquivo_nome: str, arquivo_bytes: bytes, modelo: str):

    cab = parser.get("cabecalho", {}) or {}
    linhas = parser.get("linhas", []) or []

    segurado_id = get_or_create_segurado(cab)

    # Salvar relatório
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

    linhas_insert = []
    vinculos_insert = []

    for l in linhas:
        # salvar linha
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

        # Criar vínculo empresa ↔ segurado
        empresa_id = get_or_create_empresa(l.get("documento_tomador"))

        if empresa_id and l.get("competencia_ano") and l.get("competencia_mes"):
            vinculos_insert.append(
                {
                    "empresa_id": empresa_id,
                    "segurado_id": segurado_id,
                    "relatorio_id": relatorio_id,
                    "competencia": l.get("competencia_literal"),
                    "competencia_ano": l.get("competencia_ano"),
                    "competencia_mes": l.get("competencia_mes"),
                    "categoria_codigo": l.get("categoria_codigo"),
                    "fpas": l.get("fpas"),
                    "remuneracao": l.get("remuneracao"),
                    "extemporaneo": l.get("extemporaneo"),
                }
            )

    if linhas_insert:
        supabase.table("ci_gfip_linhas").insert(linhas_insert).execute()

    if vinculos_insert:
        supabase.table("ci_gfip_empresas_vinculos").insert(vinculos_insert).execute()

    return {
        "status": "sucesso",
        "segurado_id": segurado_id,
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
        return {"status": "erro", "mensagem": "Arquivo PDF vazio."}

    # Arquivo temporário
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
        return {"status": "erro", "mensagem": f"Erro no parser: {resultado['erro']}"}

    cab = resultado.get("cabecalho", {})
    cab["profissao"] = profissao.strip()
    cab["estado"] = estado.strip()
    resultado["cabecalho"] = cab

    save = salvar_relatorio_completo(
        parser=resultado,
        arquivo_nome=arquivo.filename,
        arquivo_bytes=conteudo,
        modelo=layout,
    )

    return save
