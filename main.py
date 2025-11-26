from fastapi import FastAPI, UploadFile, File, HTTPException

app = FastAPI()

@app.get("/")
def root():
    return {"status": "ok", "mensagem": "API CI GFIP ativa."}

@app.post("/extract/ci-gfip-modelo-1")
async def extract_ci_gfip_modelo_1(file: UploadFile = File(...)):
    # Validar PDF
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(
            status_code=400,
            detail="O arquivo enviado precisa ser um PDF."
        )

    # Ler PDF (somente para teste — não extrai nada ainda)
    conteudo = await file.read()
    tamanho = len(conteudo)

    return {
        "status": "sucesso",
        "arquivo_recebido": file.filename,
        "tamanho_bytes": tamanho,
        "texto_extraido": "OCR desativado no Render (tesseract não permitido).",
        "mensagem": "Parser real será implementado por Ronaldo."
    }
