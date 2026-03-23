# BIGPOPA

BIGPOPA is a local-first desktop app for running and tuning International Futures (IFs) scenarios on Windows. It combines a React UI, an Electron desktop shell, and Python backend tools to validate an IFs installation, build a baseline model configuration, run iterative IFs experiments, compare modeled outputs to historical data, and keep every configuration and score in a local SQLite database.

All core work happens on the local machine. The main runtime record is `bigpopa.db`, and each evaluated model gets its own artifact folder under the selected output directory.

## Quick Start

### 1. Clone the repository
```bash
git clone <repo-url>
cd BIGPOPA\ifs_autotune
```

### 2. Install prerequisites
- Python 3.11 or newer
- Node.js LTS with `npm`

### 3. Start BIGPOPA

#### Option A: double-click the launcher
- In File Explorer, double-click `scripts\Run_BIGPOPA.bat`.

#### Option B: run the launcher from a terminal
```bat
scripts\Run_BIGPOPA.bat
```

The launcher is safe to run multiple times. It will:
- verify Python 3.11+ and `npm`
- create `backend\.venv` if needed
- install or refresh backend, frontend, and desktop dependencies
- start the frontend dev server
- launch the Electron desktop app

#### Optional PowerShell launcher
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\Run_BIGPOPA.ps1
```

## Manual Run

If you want to run the pieces yourself instead of using the `.bat` launcher:

```bash
python -m venv backend/.venv
backend/.venv/Scripts/python -m pip install -U pip
backend/.venv/Scripts/python -m pip install -e backend
cd frontend && npm install
cd ../desktop && npm install
cd ../desktop && npm run electron-dev
```

`backend\.venv` is the expected Python environment for the Electron app and backend scripts.

## What The App Does

BIGPOPA turns an IFs installation plus a workbook configuration into a repeatable tuning workflow:

1. Validate the IFs folder, output folder, and `StartingPointTable.xlsx`.
2. Register IFs metadata and defaults in `bigpopa.db`.
3. Build a baseline configuration from workbook-selected parameters, coefficients, and output variables.
4. Persist the chosen `ml_method` and `fit_metric` into `bigpopa.db`.
5. Train a surrogate ensemble on the baseline plus compatible historical runs.
6. Rebuild a proposal pool each ML iteration, score it, and choose the next IFs input.
7. Run IFs, extract outputs, compute fit scores, and reuse prior exact results when possible.
8. Report the best model found and keep all artifacts on disk for later review.

## How It Works

### 1. Validation
- The app checks the selected IFs folder for required files such as `IFsInit.db`, `RUNFILES/IFsHistSeries.db`, `RUNFILES/DataDict.db`, `Scenario/`, and `net8/ifs.exe`.
- It checks that the output folder exists and is writable.
- It checks that the workbook contains the required sheets: `AnalFunc`, `TablFunc`, `IFsVar`, and `DataDict`.
- It derives the base year from `IFsInit.db` when possible.

### 2. Model setup
- BIGPOPA records IFs version metadata plus parameter and coefficient defaults in `bigpopa.db`.
- It reads workbook-selected parameters, coefficients, output variables, and ML settings.
- It builds a canonical baseline configuration, computes a `dataset_id`, and hashes a deterministic `model_id`.
- It persists `ml_method` and `fit_metric` into `ifs_version`.
- It immediately runs extraction for the baseline configuration so the database starts with a scored baseline.

### 3. Refreshed ML search
- Workbook runtime controls such as `n_sample`, `n_max_iteration`, `n_convergence`, and `min_convergence_pct` are still read from `StartingPointTable.xlsx`.
- `ml_method` is replayed from `bigpopa.db`, not re-selected from the workbook after setup.
- The default ML path rebuilds a proposal pool every iteration instead of reusing one static `X_grid`.
- Each iteration retrains a surrogate ensemble on all observed compatible runs, including reused historical samples.
- The next input is chosen by ranking the refreshed proposal pool with the surrogate's mean and uncertainty.

### 4. IFs execution
- For each chosen candidate, BIGPOPA writes parameter values into `Scenario/Working.sce` and coefficient values into `RUNFILES/Working.run.db`.
- It launches `net8/ifs.exe`, watches progress, and copies the resulting `Working.run.db` and `Working.sce` into a per-model output folder.

### 5. Extraction and scoring
- BIGPOPA extracts the configured IFs output variables from the run database.
- It combines them with historical data from `RUNFILES/IFsHistSeries.db`.
- It writes per-variable and pooled fit metrics back to `bigpopa.db` and also saves fit summaries into the model folder.

## Core Files And Outputs

- `StartingPointTable.xlsx`
  Controls which parameters, coefficients, and output variables participate in tuning. The `ML` sheet still provides runtime controls such as `n_sample`, `n_max_iteration`, `n_convergence`, and `min_convergence_pct`. Optional `Step` and `LevelCount` columns define explicit dimensions in the search space.
- `<output>\bigpopa.db`
  BIGPOPA's local state store. It keeps IFs metadata, canonical model configurations, persisted run-level controls such as `ml_method` and `fit_metric`, run status, fit scores, and ML progress history.
- `<output>\<model_id>\`
  Per-model artifact folder. Typical contents include `Working.<model_id>.run.db`, `Working.<model_id>.sce`, extracted CSV files, combined comparison files, and `fit_<model_id>.csv` plus `fit_<model_id>.json`.
- ML proposal pool
  The current candidate pool is held in RAM only for the current ML iteration. It is regenerated during the run and is not written into `bigpopa.db`.
- `RUNFILES\Working.run.db`
  Temporary IFs working database used for the current run. BIGPOPA resets it from `IFsBase.run.db` after each completed run.

## Deep Dive Docs

- [Workflow And Runtime Artifacts](docs/WORKFLOW.md)
- [Workbook, Bounds, And Search Space Rules](docs/SEARCH_SPACE.md)
- [ML Process And Neural-Network Search](docs/ML_PROCESS.md)
- [Architecture Repo Map](docs/ARCHITECTURE.md)

## Troubleshooting

- Python not found or wrong version
  Install Python 3.11 or newer, reopen the terminal, and run `scripts\Run_BIGPOPA.bat` again.
- `npm` not found
  Install Node.js LTS, reopen the terminal, and rerun the launcher.
- `desktop\package.json` missing
  Make sure you are in the correct repository and the `desktop` folder exists.
- Backend editable install fails
  Delete stale `backend\*.egg-info` content and rerun the launcher.
- Frontend does not open
  Confirm that `frontend\package.json` exists and that `npm install` completed successfully.
- Validation fails even with a valid IFs folder
  Check that `backend\tools\ParquetReaderlite.exe` is present. The Electron validation path also treats that helper as required.
