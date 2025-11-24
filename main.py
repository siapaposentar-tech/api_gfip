from fastapi import FastAPI, UploadFile, File
from fastapi.responses import JSONResponse
from pdf2image import convert_from_bytes
import pytesseract
import tempfile
import os

app = FastAPI(
    title="API de Extração CI GFIP",
    description="Extrator do Modelo 1 - Projeto EXTRATOR CNIS",
    version="1.0"
)

# -------------------------------------------------------------
#  FUNÇÃO BASE DE EXTRAÇÃO DE TEXTO
# -------------------------------------------------------------
def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """
    Extrai texto de um PDF usando OCR (pdf2image + Tesseract).
    Funciona mesmo quando o PDF não tem texto digital.
    """
    try:
        images = convert_from_bytes(pdf_bytes)
        text = ""

        for img in images:
            text += pytesseract.image_to_string(img, lang="por")

        return text

    except Exception as e:
        return f"Erro na extração OCR: {e}"


# -------------------------------------------------------------
#  ENDPOINT PRINCIPAL - MODELO 1 CI GFIP
# -------------------------------------------------------------
@app.post("/extract/ci-gfip-modelo-1")
async def extract_ci_gfip_modelo_1(file: UploadFile = File(...)):
    """
    Recebe o PDF enviado pela Edge Function ou pelo teste manual,
    extrai o texto e devolve uma estrutura pronta para ser interpretada.
    """

    try:
        pdf_bytes = await file.read()

        # 1) EXTRAI TEXTO BRUTO
        extracted_text = extract_text_from_pdf(pdf_bytes)

        # 2) AQUI ENTRARÁ O PARSER REAL DO CI GFIP
        #    Por enquanto, apenas devolvemos o texto bruto para teste.
        response_json = {
            "status": "sucesso",
            "arquivo_recebido": file.filename,
            "tamanho_bytes": len(pdf_bytes),
            "texto_extraido": extracted_text,
            "mensagem": "Parser real será adicionado quando Ronaldo validar os campos."
        }

        return JSONResponse(content=response_json)

    except Exception as e:
        return JSONResponse(
            content={"status": "erro", "detalhes": str(e)},
            status_code=500
        )


# -------------------------------------------------------------
#  ENDPOINT DE TESTE
# -------------------------------------------------------------
@app.get("/")
def root():
    return {"message": "API CI GFIP operando. Use POST /extract/ci-gfip-modelo-1"}
