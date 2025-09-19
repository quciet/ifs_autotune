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

3. **Install backend dependencies**
   - Ensure you have Python 3.11 or later installed.
   - Install the FastAPI app in editable mode:
     ```bash
     cd backend
     pip install -e .
     cd ..
     ```
   - The backend exposes a health endpoint at http://localhost:8000/health which should respond with `{ "status": "ok" }` once the server is running.

4. **Install frontend dependencies**
   - Ensure you have Node.js 18+ and npm available.
   - Install packages with:
     ```bash
     cd frontend
     npm install
     cd ..
     ```

5. **Launch the combined dev environment**
   - Run the helper script from the project root to start both servers with hot reload:
     ```bash
     python dev.py
     # or, if you prefer, ./dev.py
     ```
  - The backend will be available at http://localhost:8000 and the frontend at http://localhost:5173. Open the frontend in your browser. Click **Browse** to select your IFs installation folder—the selected path will be displayed above the **Validate** button. Click **Validate** to send a request to the backend checker.
  - You can now browse for an IFs folder using the folder picker. The selected path will display automatically in the text field.
  - Validation now checks `net8/ifs.exe` directly instead of separate `net8` + `ifs.exe` entries.
  - Validation results display a checklist of required files/folders with ✅ or ❌ indicators.
   - During development, the frontend at http://localhost:5173 must call backend APIs at http://localhost:8000. CORS middleware has been enabled to allow this. In production, the frontend can be built and served from the backend directly.

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
  - Checks for `IFsInit.db`, `DATA/SAMBase.db`, `DATA/DataDict.db`, `DATA/IFsHistSeries.db`, `net8/ifs.exe`, `RUNFILES/`, and `Scenario/`
  - Extracts the model base year from `IFsInit.db`

This provides the skeleton plumbing: API, stubbed run logging, and IFs environment validation. Next stages will implement real IFs subprocess calls, results parsing, and optimization loop logic.
