# BIGPOPA (local web app)
Local-first application for automating and optimizing IFs model runs.

## Dev (backend)
cd backend
pip install -e .
uvicorn app.main:app --reload

Open http://localhost:8000/health and expect {"status":"ok"}.

## Tests
cd backend
pytest -q

## Stubbed IFs run
POST /ifs/run with a JSON body (e.g. {"parameters":{"tfrmin":1.5}}) returns a run_id, a toy metric, and a fake output.
