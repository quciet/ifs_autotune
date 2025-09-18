from fastapi import FastAPI
from .settings import settings
from .storage.db import init_db, record_run
from .runner import apply_inputs_stub, run_ifs_stub
from .metrics import metric_stub

app = FastAPI(title="BIGPOPA API")


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
