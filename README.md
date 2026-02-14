# BIGPOPA (Desktop App)

Local-first desktop application for automating and optimizing IFs model runs.  
Runs fully offline: React/Electron frontend + Python backend, with Electron acting as the bridge.

---
## Quick Start (Windows, recommended)

### 1) Clone the repository
```bash
git clone <repo-url>
cd BIGPOPA\ifs_autotune
```

### 2) Install prerequisites
- Python **3.11 or newer**
- Node.js LTS (includes `npm`)

### 3) Launch BIGPOPA (double-click)
- In File Explorer, double-click `scripts\Run_BIGPOPA.bat`.
- Or from terminal:
```bat
scripts\Run_BIGPOPA.bat
```

The launcher is idempotent and safe to run multiple times. It will:
- verify Python 3.11+ and npm are installed,
- create `backend\.venv` only if missing,
- install/update backend, frontend, and desktop dependencies,
- launch frontend (`npm run dev`, when `frontend\package.json` exists) and desktop (`npm run start:electron`).

> `backend\.venv` is **required** for this project and is automatically created on first run.

### Optional PowerShell launcher
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\Run_BIGPOPA.ps1
```

### Troubleshooting launch failures
- **Python not found / wrong version**: install Python 3.11+ and relaunch.
- **npm not found**: install Node.js LTS, reopen terminal, and relaunch.
- **`desktop\package.json` missing**: make sure you are in the correct repo and that the `desktop` folder is present.
- **Frontend does not open**: if `frontend\package.json` is missing, backend + desktop can still launch; restore frontend files to enable UI dev server launch.
- **Backend install error** (`pip install -e backend`):
  ```bat
  rmdir /s /q backend\bigpopa_backend.egg-info
  scripts\Run_BIGPOPA.bat
  ```

## Manual Run (advanced users)

If you prefer to run each service manually, this workflow is still supported.

1. Install backend deps in the required project venv:
   ```bash
   python -m venv backend/.venv
   backend/.venv/Scripts/python -m pip install -U pip
   backend/.venv/Scripts/python -m pip install -e backend
   ```
2. Install frontend + desktop deps:
   ```bash
   cd frontend && npm install
   cd ../desktop && npm install
   ```
3. Run desktop dev mode:
   ```bash
   cd desktop
   npm run electron-dev
   ```

> Note: `backend\.venv` remains required in both quickstart and manual development paths.

---
## How it Works

BIGPOPA is a desktop tool for automating and optimizing International Futures (IFs) model runs. The application guides the user through the following workflow:

1.  **Validation:** The user selects their IFs installation folder, an output directory, and an input Excel file (`StartingPointTable.xlsx`). The app validates that all required files and folders are present and readable.
2.  **Model Setup:** The user configures the simulation end year. The backend then prepares the IFs model based on the settings defined in the `StartingPointTable.xlsx` file.
3.  **ML Optimization:** The app runs an active learning loop to find the best model parameters. It repeatedly executes the IFs model, extracts results, and compares them against historical data to calculate a fitness score.
4.  **Results:** The best model configuration is identified, and the results are displayed in the user interface. All runs and their outputs are stored in a SQLite database for review and reuse.

## Working Files

-   **`StartingPointTable.xlsx`**: This is the primary input file for configuring the model and the machine learning optimization. Users can define model parameters, coefficients, and other settings in the various sheets of this Excel workbook.
-   **`bigpopa.db`**: This is a SQLite database that is created in the user-specified output directory. It stores all model configurations, run artifacts, and final results. It also functions as a cache, allowing the optimization loop to reuse results from previously evaluated model configurations, which significantly speeds up the process.

---
