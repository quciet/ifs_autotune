# Architecture Repo Map

This document is the repo-and-runtime map for BIGPOPA. It focuses on system boundaries, stage contracts, and persistent state. For user workflow and tuning semantics, see:

- [Workflow And Runtime Artifacts](WORKFLOW.md)
- [Workbook, Bounds, And Search Space Rules](SEARCH_SPACE.md)
- [ML Process And Neural-Network Search](ML_PROCESS.md)

## Overview

BIGPOPA is a local-first desktop app made of three layers:

- `frontend/`
  React renderer for validation, setup, tuning, and progress display.
- `desktop/`
  Electron main process and preload bridge. It owns the desktop window, IPC handlers, Python subprocess orchestration, and ML job state.
- `backend/`
  Python scripts that validate IFs, register metadata, build model configs, run IFs, extract outputs, score results, and orchestrate the ML loop.

The central runtime store is `<output>/bigpopa.db`. It holds IFs metadata, canonical model inputs, run status, fit metrics, and ML progress history.

## Repo Map

```text
.
|-- backend/
|   |-- validate_ifs.py
|   |-- model_setup.py
|   |-- ml_driver.py
|   |-- run_ifs.py
|   |-- extract_compare.py
|   |-- prepare_coeff_param.py
|   |-- common_sce_utils.py
|   |-- dataset_utils.py
|   |-- ml_progress.py
|   |-- log_ifs_version.py
|   `-- optimization/
|-- desktop/
|   |-- main.js
|   |-- preload.js
|   |-- input/template/
|   `-- output/
|-- frontend/
|   `-- src/
|-- docs/
|   |-- ARCHITECTURE.md
|   |-- WORKFLOW.md
|   |-- SEARCH_SPACE.md
|   `-- ML_PROCESS.md
|-- scripts/
|   `-- Run_BIGPOPA.bat
`-- README.md
```

## Main Runtime Stages

### 1. Validation

- Renderer calls the Electron validation handler.
- Electron launches `backend/validate_ifs.py`.
- Backend verifies IFs folder contents, workbook sheets, output-folder readiness, and base year.
- Electron also checks that `backend/tools/ParquetReaderlite.exe` exists and treats it as required for a valid setup.

### 2. Model setup

- Renderer calls the `model_setup` IPC handler.
- Electron launches `backend/model_setup.py`.
- Backend records IFs metadata, builds the baseline config, persists run-level values such as `ml_method` and `fit_metric`, inserts `model_input`, and triggers baseline extraction.

### 3. ML run

- Renderer calls `run-ml`.
- Electron launches `backend/ml_driver.py` and keeps ML job state in memory so the UI can reattach after renderer reloads.
- ML progress is streamed back through `ml-log`, `model-setup-progress`, and `ifs-progress`.
- `ml_driver.py` now builds a proposal generator, not just one static candidate matrix.
- The current desktop flow uses the candidate-generator path in `active_learning.py`, which rebuilds the current proposal pool each iteration.

### 4. IFs execution

- `ml_driver.py` calls `run_ifs.py` for unevaluated candidates.
- `run_ifs.py` writes `Working.sce` and `Working.run.db`, launches `net8/ifs.exe`, snapshots artifacts, resets the working DB, and triggers extraction.

### 5. Extraction and scoring

- `extract_compare.py` extracts configured outputs, combines them with history, computes fit metrics, writes fit files, and updates `model_output`.

## Key Files

### Electron side

- `desktop/main.js`
  App lifecycle, default input/output folders, file pickers, validation bridge, model setup bridge, ML process management, and progress relays.
- `desktop/preload.js`
  Safe IPC bridge exposed to the renderer.

### Frontend side

- `frontend/src/App.tsx`
  Main validation and tuning flow, including progress display and ML-history modal.
- `frontend/src/api.ts`
  Typed IPC wrapper layer for validation, setup, ML runs, and progress history.

### Backend side

- `backend/validate_ifs.py`
  Environment validation and template initialization.
- `backend/model_setup.py`
  IFs metadata registration, workbook selection handling, baseline config creation, and initial extraction trigger.
- `backend/ml_driver.py`
  Search-space construction, proposal-generator setup, caching/reuse checks, and active-learning orchestration.
- `backend/run_ifs.py`
  Per-model IFs execution wrapper and post-run artifact handling.
- `backend/extract_compare.py`
  Output extraction, history joins, fit computation, and DB updates.
- `backend/log_ifs_version.py`
  Loads IFs parameter and coefficient metadata into `bigpopa.db`.
- `backend/dataset_utils.py`
  Computes `dataset_id` and loads structurally compatible prior samples.
- `backend/ml_progress.py`
  Reads trial history for the renderer's ML progress chart.
- `backend/optimization/active_learning.py`
  Iterative active-learning loop that can consume either a static candidate grid or a candidate generator.
- `backend/optimization/ensemble_training.py`
  Surrogate-ensemble training and prediction utilities.
- `backend/optimization/surrogate_models.py`
  Bounds scaling, target transformation, and surrogate model implementations.

## Persistent State

### Runtime databases

- `<output>/bigpopa.db`
  BIGPOPA runtime database.
- `<ifs_root>/IFsInit.db`
  Validation and base-year source.
- `<ifs_root>/RUNFILES/IFsBase.run.db`
  Baseline run database used for resets.
- `<ifs_root>/RUNFILES/Working.run.db`
  Mutable working DB for the current model.
- `<ifs_root>/RUNFILES/IFsHistSeries.db`
  Historical comparison source.

### Main `bigpopa.db` tables

- `ifs_static`
  IFs static metadata layer keyed by IFs version content.
- `parameter`
  Parameter catalog with default and min/max bounds.
- `coefficient`
  Coefficient catalog with defaults and optional standard deviations.
- `ifs_version`
  Run-level IFs metadata such as base year, end year, fit metric, and ML method.
- `model_input`
  Canonical model configurations, including `input_param`, `input_coef`, `output_set`, and `dataset_id`.
- `model_output`
  Run status, fit metrics, trial tracking columns, and timestamps.

## ML Runtime Boundaries

The current ML runtime path works like this:

1. `ml_driver.py` loads the baseline configuration plus compatible historical observations.
2. It builds the search space and the proposal generator.
3. It creates the input scaler and target transformer used by the surrogate ensemble.
4. It calls `active_learning.py`.
5. `active_learning.py` retrains the surrogate ensemble every iteration on the accumulated observed samples.
6. It requests a fresh proposal pool from the generator, ranks that pool, and selects the next candidate.
7. `ml_driver.py` either reuses a cached exact score or delegates the new candidate to `run_ifs.py`.

Important runtime facts:
- only the current proposal pool lives in RAM
- the proposal pool is not stored in `bigpopa.db`
- exact result reuse happens through `model_id`
- historical warm-start reuse happens through `dataset_id`

For the detailed training and acquisition rules, see [ML Process And Neural-Network Search](ML_PROCESS.md).

## IPC Contracts

### Request/response handlers

- `validate-ifs-folder`
  Validates IFs path, output path, and workbook path.
- `model_setup`
  Starts baseline setup and returns `ifs_id` plus baseline `model_id`.
- `run-ml`
  Starts the ML driver and resolves with the final ML summary payload.
- `ml:getProgressHistory`
  Reads trial history for the current dataset cohort.
- `ml:jobStatus`
  Returns the current in-memory ML job state.
- `ml:requestStop`
  Signals a graceful stop after the current evaluation.

### Streamed renderer events

- `model-setup-progress`
  Human-readable status lines from backend stages.
- `ifs-progress`
  Year-based progress updates from IFs execution.
- `ml-log`
  ML status lines, including iteration progress and proposal-pool logging.

## CLI Contracts

- `validate_ifs.py <ifs_path> [--output-path ...] [--input-file ...]`
- `model_setup.py --ifs-root ... --input-file ... --end-year ... --output-folder ... [--base-year ...]`
- `ml_driver.py --ifs-root ... --end-year ... --output-folder ... --initial-model-id ... --bigpopa-db ...`
- `run_ifs.py --ifs-root ... --end-year ... --output-dir ... --model-id ... --ifs-id ... --base-year ...`
- `extract_compare.py --ifs-root ... --model-db ... --input-file ... --model-id ... --ifs-id ... [--bigpopa-db ...]`
- `ml_progress.py --bigpopa-db ... --model-id ...`

## Output Artifacts

Typical per-model outputs under `<output>/<model_id>/` include:

- `Working.<model_id>.run.db`
- `Working.<model_id>.sce`
- parquet payloads extracted from IFs blobs
- historical CSV exports
- combined comparison CSV files
- `fit_<model_id>.csv`
- `fit_<model_id>.json`

## Current Sharp Edges

- `ParquetReaderlite.exe` is treated as required by the Electron validation path.
- Workbook `Switch` handling is not perfectly uniform across baseline selection and grid parsing.
- `model_output` schema evolution is still managed from multiple scripts, so table changes must stay synchronized.
- `fit_pooled` is the ML objective even when the fit metric is `r2`, where the stored value is `1 - pooled_r2`.
- `run_seed` does not yet guarantee full end-to-end NN reproducibility.
