# BIGPOPA (Desktop App)

Local-first desktop application for automating and optimizing IFs model runs.  
Runs fully offline: React/Electron frontend + Python backend, with Electron acting as the bridge.

---

## Run the app locally (development)

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
     .venv\Scripts\activate    # On Windows
     # or: source .venv/bin/activate  # On Linux/Mac
     ```
   - Exit the environment with `deactivate` when done.
   - Using Conda or another manager also works.

3. **Install backend in editable mode**
   - Ensure you have Python 3.11 or later installed.
   - From the `backend` folder:
     ```bash
     cd backend
     pip install -e .
     cd ..
     ```

4. **Install frontend + desktop dependencies**
   - Ensure you have Node.js 18+ and npm installed.
   - Install packages for both frontend and Electron:
     ```bash
     cd frontend
     npm install
     cd ../desktop
     npm install
     cd ..
     ```

5. **Place the default input workbook (optional but recommended)**
   - Create an `input` folder inside `desktop/` if it does not already exist.
   - Copy your `StartingPointTable.xlsx` into `desktop/input/StartingPointTable.xlsx`.
   - The desktop app will automatically point the file picker at this location when it launches.

6. **Launch the desktop app (dev mode)**
   - From the `desktop/` folder:
     ```bash
     npm run electron-dev
     ```
   - This will:
     - Start the React frontend with hot reload (Vite).
     - Start Electron and open a desktop window.
   - Use the **Browse** button (native folder picker) or paste a path to select your IFs installation folder, then click **Validate**.
   - Validation results display:
     - ✅ or ❌ for required files/folders (`IFsInit.db`, `DATA/...`, `net8/ifs.exe`, `RUNFILES`, `Scenario`).
     - Extracted base year from `IFsInit.db`.

## Current status

Even in its early stage the project already wires the Electron shell, React renderer, and FastAPI backend together so the desktop app can exercise real backend logic:

- **Installation validation API.** `/ifs/check` walks an IFs install folder, confirming every required database, executable, and directory, reporting why anything is missing, and extracting the latest historical/forecast base year directly from `IFsInit.db`.
- **Workbook inspection.** The validator also opens the provided Excel workbook and checks that the `AnalFunc`, `TablFunc`, `IFsVar`, and `DataDict` sheets exist so tuning inputs are guaranteed to be usable before a run is attempted.
- **Output-path readiness checks.** Output folders are inspected for readability and writability so automated runs can safely write scenario files.
- **Automation loop scaffolding.** `/ifs/run` already captures end-to-end orchestration: it applies the submitted configuration stub, simulates an IFs run, scores the result with a placeholder metric, and records the full config/output/metric/status bundle into the local SQLite `runs` table for later review.
- **Database bootstrapping.** The backend initializes the SQLite store on startup (and when imported) so history tracking works even in test harnesses.
- **Health endpoint.** `/health` exposes a simple readiness probe that keeps the desktop shell informed that the backend is reachable.
