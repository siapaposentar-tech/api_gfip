import re
from typing import Dict, Any


def detectar_layout_ci_gfip(texto: str) -> str:
    """
    Detecta automaticamente qual é o modelo de CI GFIP:

    - modelo_1 → layouts antigos (relatórios SEFIP / GFIP tradicional)
    - modelo_2 → CONSULTA VALORES CI GFIP / eSocial / INSS (layout novo misto)

    Se não conseguir identificar com segurança, retorna "desconhecido".
    """

    texto_upper = texto.upper()

    # ---- Modelo 2 (o mais comum hoje) ----
    # Palavras típicas que aparecem nos PDFs novos:
    # "CONSULTA VALORES CI GFIP", "CI GFIP/ESOCIAL/INSS", "ESOCIAL"
    if (
        "CONSULTA VALORES CI GFIP" in texto_upper
        or "CI GFIP/ESOCIAL/INSS" in texto_upper
        or "CI GFIP / ESOCIAL / INSS" in texto_upper
        or "ESOCIAL" in texto_upper
    ):
        return "modelo_2"

    # ---- Modelo 1 (SEFIP antigo) ----
    # Geralmente aparece "SEFIP" e alguma referência a "DATA DE ENVIO" ou
    # colunas mais antigas de GFIP.
    if "SEFIP" in texto_upper and "DATA DE ENVIO" in texto_upper:
        return "modelo_1"

    # Se em algum momento você identificar novas frases-chave de um layout
    # diferente, basta acrescentar aqui mais blocos if/elif.

    return "desconhecido"


def _normalizar_resultado_parser(resultado: Dict[str, Any]) -> Dict[str, Any]:
    """
    Garante que o dicionário de saída sempre tenha as mesmas chaves básicas.
    """

    if resultado is None:
        resultado = {}

    cabecalho = resultado.get("cabecalho") or {}
    linhas = resultado.get("linhas") or []

    # Se o parser específico não calculou total_linhas, calculamos aqui.
    total_linhas = resultado.get("total_linhas")
    if total_linhas is None:
        total_linhas = len(linhas)

    resultado["cabecalho"] = cabecalho
    resultado["linhas"] = linhas
    resultado["total_linhas"] = total_linhas

    return resultado


def parse_ci_gfip(texto: str) -> Dict[str, Any]:
    """
    PARSER UNIVERSAL

    - Detecta o layout automaticamente.
    - Chama o parser específico (modelo_1 ou modelo_2).
    - Se o layout vier "desconhecido", tenta os dois parsers em modo fallback.
    - Sempre devolve:
        cabecalho, linhas, total_linhas, layout_detectado
      e, em caso de erro, a chave "erro".
    """

    layout = detectar_layout_ci_gfip(texto)
    resultado: Dict[str, Any]

    try:
        # -----------------------------
        # 1) Caminho normal: layout detectado
        # -----------------------------
        if layout == "modelo_2":
            from .ci_gfip_modelo_2 import parse_ci_gfip_modelo_2

            resultado = parse_ci_gfip_modelo_2(texto)
            resultado = _normalizar_resultado_parser(resultado)
            resultado["layout_detectado"] = "modelo_2"
            return resultado

        if layout == "modelo_1":
            from .ci_gfip_modelo_1 import parse_ci_gfip_modelo_1

            resultado = parse_ci_gfip_modelo_1(texto)
            resultado = _normalizar_resultado_parser(resultado)
            resultado["layout_detectado"] = "modelo_1"
            return resultado

        # -----------------------------
        # 2) Fallback: layout "desconhecido"
        #    Tenta modelo_2, depois modelo_1.
        # -----------------------------
        from .ci_gfip_modelo_2 import parse_ci_gfip_modelo_2
        from .ci_gfip_modelo_1 import parse_ci_gfip_modelo_1

        # Tenta modelo_2 primeiro (mais comum hoje).
        try:
            tentativa_m2 = parse_ci_gfip_modelo_2(texto)
            tentativa_m2 = _normalizar_resultado_parser(tentativa_m2)

            if tentativa_m2["total_linhas"] > 0:
                tentativa_m2["layout_detectado"] = "modelo_2"
                return tentativa_m2
        except Exception:
            # Se der erro interno, simplesmente desconsideramos e seguimos para o próximo.
            tentativa_m2 = None

        # Tenta modelo_1 em seguida.
        try:
            tentativa_m1 = parse_ci_gfip_modelo_1(texto)
            tentativa_m1 = _normalizar_resultado_parser(tentativa_m1)

            if tentativa_m1["total_linhas"] > 0:
                tentativa_m1["layout_detectado"] = "modelo_1"
                return tentativa_m1
        except Exception:
            tentativa_m1 = None

        # Se chegou aqui, nenhum parser conseguiu extrair linhas.
        resultado = {
            "cabecalho": {},
            "linhas": [],
            "total_linhas": 0,
            "layout_detectado": "desconhecido",
            "erro": "layout_nao_identificado_ou_sem_linhas",
        }
        return resultado

    except Exception as e:
        # Qualquer erro não tratado cai aqui para não derrubar a API.
        resultado = {
            "cabecalho": {},
            "linhas": [],
            "total_linhas": 0,
            "layout_detectado": layout or "desconhecido",
            "erro": f"erro_parser_universal: {type(e).__name__}: {e}",
        }
        return resultado

