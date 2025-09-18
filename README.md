# BIGPOPA (local web app)

Local-first application for automating and optimizing IFs model runs.

## Run the app locally

1. **Clone the repository**
   ```bash
   git clone https://github.com/<your-org>/ifs_autotune.git
   cd ifs_autotune
   ```

2. **(Optional) Set up a Python virtual environment**
   - Using the built-in `venv` module:
     ```bash
     cd backend
     python -m venv .venv
     source .venv/bin/activate  # On Windows use: .venv\\Scripts\\activate
     ```
   - When you're done, exit the virtual environment with `deactivate`.
   - Using Conda or another environment manager works as well—just create and activate your environment before installing dependencies.
   - If you prefer to work without a virtual environment, ensure the following commands are run in the Python environment where you want the dependencies installed.

3. **Start the backend API**
   - Ensure you have Python 3.11 or later installed.
   - Install dependencies and launch the FastAPI server:
     ```bash
     cd backend
     pip install -e .
     uvicorn app.main:app --reload
     ```
   - The backend exposes a health endpoint at http://localhost:8000/health which should respond with `{ "status": "ok" }`.

4. **Start the frontend**
   - Ensure you have Node.js 18+ and npm available.
   - In a new terminal window, install dependencies and run the dev server:
     ```bash
     cd frontend
     npm install
     npm run dev
     ```
   - Open http://localhost:5173 in your browser. Type an IFs folder path into the form and click **Validate** to send a request to the backend checker.

Once both servers are running, the frontend will communicate with the backend API locally.

## Tests

```bash
cd backend
pytest -q
```

## Stubbed IFs run

`POST /ifs/run` with a JSON body (e.g. `{ "parameters": { "tfrmin": 1.5 } }`) returns a `run_id`, a toy metric, and a fake output.

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
