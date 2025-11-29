import re
from decimal import Decimal

def parse_modelo_1(texto: str) -> dict:
    """
    Parser para o CI GFIP Modelo 1 (SEFIP tradicional).
    Esse modelo possui informações mais espalhadas e menos estruturadas.
    """

    linhas = texto.splitlines()

    cabecalho = {
        "nit": "",
        "nome": "",
        "nome_mae": "",
        "data_nascimento": "",
        "cpf": "",
        "profissao": "",
        "estado": ""
    }

    registros = []

    # ----------------------------------------
    # CAPTURA DO CABEÇALHO
    # ----------------------------------------
    for linha in linhas:

        if "NIT" in linha.upper():
            match = re.search(r"NIT[:\s]+(\d+)", linha, re.IGNORECASE)
            if match:
                cabecalho["nit"] = match.group(1)

        if "NOME" in linha.upper() and "MAE" not in linha.upper():
            match = re.search(r"NOME[:\s]+(.+)", linha, re.IGNORECASE)
            if match:
                cabecalho["nome"] = match.group(1).strip()

        if "MAE" in linha.upper():
            match = re.search(r"MAE[:\s]+(.+)", linha, re.IGNORECASE)
            if match:
                cabecalho["nome_mae"] = match.group(1).strip()

        if "NASCTO" in linha.upper() or "NASCIMENTO" in linha.upper():
            match = re.search(r"(\d{2}/\d{2}/\d{4})", linha)
            if match:
                cabecalho["data_nascimento"] = match.group(1)

        if "CPF" in linha.upper():
            match = re.search(r"CPF[:\s]+([\d\. -]+)", linha)
            if match:
                cabecalho["cpf"] = match.group(1).strip()

    # ----------------------------------------
    # CAPTURA DAS LINHAS DE MOVIMENTO (tabela)
    # ----------------------------------------

    dentro_tabela = False

    for linha in linhas:

        if (
            "COMPET" in linha.upper()
            and "FPAS" in linha.upper()
            and "CATEG" in linha.upper()
        ):
            dentro_tabela = True
            continue

        if dentro_tabela:

            partes = re.split(r"\s{2,}", linha.strip())

            if len(partes) < 5:
                continue

            try:
                competencia = partes[0]
                fpas = partes[1]
                categoria = partes[2]
                remuneracao = partes[3] if len(partes) > 3 else "0"
                valor_retido = partes[4] if len(partes) > 4 else "0"

                remun_num = Decimal(re.sub(r"[^\d,]", "", remuneracao).replace(",", ".")) if remuneracao else Decimal("0")
                retido_num = Decimal(re.sub(r"[^\d,]", "", valor_retido).replace(",", ".")) if valor_retido else Decimal("0")

                registros.append({
                    "fonte": "GFIP",
                    "numero_documento": "",
                    "nit": cabecalho["nit"],
                    "competencia_literal": competencia,
                    "competencia_date": "",
                    "documento_tomador": "",
                    "fpas": fpas,
                    "categoria_codigo": categoria,
                    "codigo_gfip": "",
                    "data_envio_literal": "",
                    "data_envio_date": "",
                    "tipo_remuneracao": "",
                    "remuneracao_literal": remuneracao,
                    "remuneracao": float(remun_num),
                    "valor_retido_literal": valor_retido,
                    "valor_retido": float(retido_num),
                    "extemporaneo_literal": "",
                    "extemporaneo": False
                })

            except:
                continue

    return {
        "cabecalho": cabecalho,
        "linhas": registros,
        "total_linhas": len(registros)
    }
