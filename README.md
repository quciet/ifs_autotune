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
