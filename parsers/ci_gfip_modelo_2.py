import re
from decimal import Decimal

def parse_modelo_2(texto: str) -> dict:
    """
    Parser para o CI GFIP Modelo 2 (CONSULTA VALORES CI GFIP/eSocial/INSS).
    Este layout é tabelado e mais padronizado.
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

    # -------------------------
    # CAPTURA DO CABEÇALHO
    # -------------------------

    for linha in linhas:
        if "NIT:" in linha.upper():
            match = re.search(r"NIT:\s*(\d+)", linha, re.IGNORECASE)
            if match:
                cabecalho["nit"] = match.group(1)

        if "NOME:" in linha.upper():
            match = re.search(r"NOME:\s*(.+)", linha, re.IGNORECASE)
            if match:
                cabecalho["nome"] = match.group(1).strip()

        if "CPF:" in linha.upper():
            match = re.search(r"CPF:\s*([\d\. -]+)", linha, re.IGNORECASE)
            if match:
                cabecalho["cpf"] = match.group(1).strip()

    # -------------------------
    # CAPTURA DAS LINHAS DA TABELA
    # -------------------------

    tabela_encontrada = False

    for linha in linhas:
        # Identifica início da tabela
        if (
            "FONTE" in linha.upper()
            and "COMPETÊNCIA" in linha.upper()
            and "FPAS" in linha.upper()
        ):
            tabela_encontrada = True
            continue

        if tabela_encontrada:
            partes = re.split(r"\s{2,}", linha.strip())

            if len(partes) < 6:
                continue

            try:
                fonte = partes[0]
                competencia = partes[1]
                documento_tomador = partes[2]
                fpas = partes[3]
                categoria = partes[4]
                codigo_gfip = partes[5]

                data_envio = partes[6] if len(partes) > 6 else ""
                remuneracao = partes[7] if len(partes) > 7 else "0"
                valor_retido = partes[8] if len(partes) > 8 else "0"
                extemporaneo = partes[9] if len(partes) > 9 else "N"

                # Converte valores
                remuneracao_num = Decimal(re.sub(r"[^\d,]", "", remuneracao).replace(",", ".")) if remuneracao else Decimal("0")
                valor_retido_num = Decimal(re.sub(r"[^\d,]", "", valor_retido).replace(",", ".")) if valor_retido else Decimal("0")

                registros.append({
                    "fonte": fonte,
                    "numero_documento": "",
                    "nit": cabecalho["nit"],
                    "competencia_literal": competencia,
                    "competencia_date": "",
                    "documento_tomador": documento_tomador,
                    "fpas": fpas,
                    "categoria_codigo": categoria,
                    "codigo_gfip": codigo_gfip,
                    "data_envio_literal": data_envio,
                    "data_envio_date": "",
                    "tipo_remuneracao": "",
                    "remuneracao_literal": remuneracao,
                    "remuneracao": float(remuneracao_num),
                    "valor_retido_literal": valor_retido,
                    "valor_retido": float(valor_retido_num),
                    "extemporaneo_literal": extemporaneo,
                    "extemporaneo": (extemporaneo.upper().startswith("S"))
                })

            except Exception:
                continue

    return {
        "cabecalho": cabecalho,
        "linhas": registros,
        "total_linhas": len(registros)
    }
