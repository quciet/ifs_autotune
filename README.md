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

3. **Install backend dependencies**
   ```bash
   cd backend
   pip install -r requirements.txt
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

5. **Launch the desktop app (dev mode)**
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

---

## Build the desktop app (production)

From the `desktop/` folder, run:

```bash
npm run electron-build
```

This produces an installer (`.exe`) that bundles:
- Electron runtime
- React UI (compiled build)
- Python backend scripts

Users can install and run BIGPOPA locally with no extra setup.

---

## Tests

```bash
cd backend
pytest -q
```

---

## Current status

As of now, the BIGPOPA desktop app can:

- Run as a true **desktop application** (no local server needed).
- Validate an IFs installation folder:
  - Checks for `IFsInit.db`, `DATA/SAMBase.db`, `DATA/DataDict.db`, `DATA/IFsHistSeries.db`, `net8/ifs.exe`, `RUNFILES/`, and `Scenario/`.
  - Extracts the model base year from `IFsInit.db`.
- Display validation results in the Electron window (green ✅ / red ❌).
- Provide a foundation for later stages:
  - Running IFs.exe in subprocess
  - Logging runs to SQLite
  - Optimization loop with ML
