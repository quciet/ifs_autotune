from fastapi import FastAPI
from .settings import settings

app = FastAPI(title="BIGPOPA API")

@app.get("/health")
def health():
    return {"status": "ok"}
