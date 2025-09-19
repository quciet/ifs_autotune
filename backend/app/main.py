from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app import ifscheck

from .settings import settings
from .storage.db import init_db, record_run
from .runner import apply_inputs_stub, run_ifs_stub
from .metrics import metric_stub

app = FastAPI(title="BIGPOPA API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(ifscheck.router, prefix="/ifs")


# Ensure the database exists even when startup events are not triggered (e.g., in some
# test harnesses).
init_db()


@app.on_event("startup")
def startup():
    init_db()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/ifs/run")
def ifs_run(config: dict):
    apply_inputs_stub(config)
    output = run_ifs_stub()
    metric = metric_stub(output)
    run_id = record_run(config, output, metric, "success")
    return {"run_id": run_id, "metric": metric, "output": output}
