import re

def detectar_layout_ci_gfip(texto: str) -> str:
    """
    Detecta automaticamente qual é o modelo de CI GFIP:
    - modelo_1 (SEFIP tradicional)
    - modelo_2 (CONSULTA VALORES CI GFIP/eSocial/INSS)
    """

    # Modelo 2 (o mais comum hoje)
    if "CONSULTA VALORES CI GFIP" in texto.upper() or "ESOCIAL" in texto.upper():
        return "modelo_2"

    # Modelo 1 (SEFIP antigo)
    if "SEFIP" in texto.upper() and "DATA DE ENVIO" in texto.upper():
        return "modelo_1"

    # Caso não identifique
    return "desconhecido"


def parse_ci_gfip(texto: str) -> dict:
    """
    Parser Universal: escolhe automaticamente qual subparser usar.
    """

    layout = detectar_layout_ci_gfip(texto)

    if layout == "modelo_2":
        from .ci_gfip_modelo_2 import parse_ci_gfip_modelo_2
        return parse_ci_gfip_modelo_2(texto)

    if layout == "modelo_1":
        from .ci_gfip_modelo_1 import parse_ci_gfip_modelo_1
        return parse_ci_gfip_modelo_1(texto)

    # Se não identificou layout
    return {
        "cabecalho": {},
        "linhas": [],
        "total_linhas": 0,
        "erro": "layout_nao_identificado"
    }
