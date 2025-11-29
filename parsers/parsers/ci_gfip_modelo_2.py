import re
from datetime import datetime

def limpar_valor(valor):
    if not valor:
        return None
    valor = valor.replace('.', '').replace(',', '.')
    try:
        return float(valor)
    except:
        return None

def parse_competencia(comp):
    try:
        m, a = comp.split('/')
        return f"{a}-{m}-01"
    except:
        return None

def parse_modelo_2(texto: str) -> dict:
    linhas = []
    cabecalho = {
        "nit": "",
        "nome": "",
        "nome_mae": "",
        "data_nascimento": "",
        "cpf": "",
        "profissao": "",
        "estado": ""
    }

    # Captura NIT
    nit_match = re.search(r"NIT:\s*([\d\.\-]+)", texto)
    if nit_match:
        cabecalho["nit"] = nit_match.group(1).strip()

    # Captura nome
    nome_match = re.search(r"Nome:\s*(.+)", texto)
    if nome_match:
        cabecalho["nome"] = nome_match.group(1).strip().title()

    # Regex para linhas do modelo 2
    padrao_linha = re.compile(
        r"(\w+)\s+(\d+)\s+([\d\.\/\-]+)\s+(\d{2}\/\d{4})\s+([\d\.\/\-]+)\s+(\d+)\s+(\d+)\s+(\d+)\s+([\d\.\/\-]+)\s+([\d\.]+)\s+([\d\.]+)\s+(Sim|Não)",
        re.MULTILINE
    )

    for m in padrao_linha.finditer(texto):
        fonte = m.group(1)
        numero_documento = m.group(2)
        nit = m.group(3)
        competencia_literal = m.group(4)
        documento_tomador = m.group(5)
        fpas = m.group(6)
        categoria_codigo = m.group(7)
        codigo_gfip = m.group(8)
        data_envio_literal = m.group(9)
        remuneracao_literal = m.group(10)
        valor_retido_literal = m.group(11)
        extemporaneo_literal = m.group(12)

        linhas.append({
            "fonte": fonte,
            "numero_documento": numero_documento,
            "nit": nit,
            "competencia_literal": competencia_literal,
            "competencia_date": parse_competencia(competencia_literal),
            "documento_tomador": documento_tomador,
            "fpas": fpas,
            "categoria_codigo": categoria_codigo,
            "codigo_gfip": codigo_gfip,
            "data_envio_literal": data_envio_literal,
            "data_envio_date": data_envio_literal,
            "tipo_remuneracao": "",
            "remuneracao_literal": remuneracao_literal,
            "remuneracao": limpar_valor(remuneracao_literal),
            "valor_retido_literal": valor_retido_literal,
            "valor_retido": limpar_valor(valor_retido_literal),
            "extemporaneo_literal": extemporaneo_literal,
            "extemporaneo": (extemporaneo_literal.lower() == "sim")
        })

    return {
        "cabecalho": cabecalho,
        "linhas": linhas,
        "total_linhas": len(linhas)
    }
