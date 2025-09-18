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

## Current status

As of now, the BIGPOPA backend can:

- Serve a FastAPI app with a `/health` endpoint
- Run a stubbed IFs run via `/ifs/run`  
  - Accepts a JSON config  
  - Produces a fake output and toy metric  
  - Logs each run into a local SQLite database (`bigpopa.db`)
- Validate an IFs installation folder via `/ifs/check`  
  - Checks presence of `ifs.exe`, `IFsInit.db`, key subfolders (`net8`, `RUNFILES`, `Scenario`, `DATA`)  
  - Ensures required data files exist in `DATA`  
  - Extracts the model base year from `IFsInit.db`

This provides the skeleton plumbing: API, stubbed run logging, and IFs environment validation. Next stages will implement real IFs subprocess calls, results parsing, and optimization loop logic.
