import re
from datetime import datetime

def parse_data(data_str):
    try:
        return datetime.strptime(data_str, "%d/%m/%Y").strftime("%Y-%m-%d")
    except:
        return None

def parse_competencia(comp):
    try:
        m, a = comp.split('/')
        return f"{a}-{m}-01"
    except:
        return None

def limpar_valor(valor):
    if not valor:
        return None
    valor = valor.replace(".", "").replace(",", ".")
    try:
        return float(valor)
    except:
        return None

def parse_modelo_1(texto: str) -> dict:
    cabecalho = {
        "nit": "",
        "nome": "",
        "nome_mae": "",
        "data_nascimento": "",
        "cpf": "",
        "profissao": "",
        "estado": ""
    }

    # Captura nome
    nome_match = re.search(r"Nome:\s*(.+)", texto)
    if nome_match:
        cabecalho["nome"] = nome_match.group(1).strip().title()

    # Captura NIT
    nit_match = re.search(r"NIT[:\s]+([\d\.\-]+)", texto)
    if nit_match:
        cabecalho["nit"] = nit_match.group(1).strip()

    # Captura data de nascimento
    nasc = re.search(r"Data de Nascimento[:\s]+(\d{2}/\d{2}/\d{4})", texto)
    if nasc:
        cabecalho["data_nascimento"] = parse_data(nasc.group(1))

    # Captura nome da mãe
    mae = re.search(r"Nome da M[ãa]e[:\s]+(.+)", texto)
    if mae:
        cabecalho["nome_mae"] = mae.group(1).strip().title()

    linhas = []

    # Regex do modelo antigo (linhas verticais)
    regex_linha = re.compile(
        r"(\d{2}/\d{4})\s+([\d\.\/\-]+)?\s+(\d+)?\s+(\d{2}/\d{2}/\d{4})?\s+([\d\.]+)\s+([\d\.]+)",
        re.MULTILINE
    )

    for m in regex_linha.finditer(texto):
        competencia = m.group(1)
        tomador = m.group(2)
        categoria = m.group(3)
        data_envio = m.group(4)
        remuneracao = m.group(5)
        valor_retido = m.group(6)

        linhas.append({
            "fonte": "GFIP",
            "numero_documento": "",
            "nit": cabecalho["nit"],
            "competencia_literal": competencia,
            "competencia_date": parse_competencia(competencia),
            "documento_tomador": tomador,
            "fpas": "",
            "categoria_codigo": categoria,
            "codigo_gfip": "",
            "data_envio_literal": data_envio,
            "data_envio_date": parse_data(data_envio) if data_envio else None,
            "tipo_remuneracao": "",
            "remuneracao_literal": remuneracao,
            "remuneracao": limpar_valor(remuneracao),
            "valor_retido_literal": valor_retido,
            "valor_retido": limpar_valor(valor_retido),
            "extemporaneo_literal": "",
            "extemporaneo": False
        })

    return {
        "cabecalho": cabecalho,
        "linhas": linhas,
        "total_linhas": len(linhas)
    }
