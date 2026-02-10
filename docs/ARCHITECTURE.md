# Architecture Repo Map

## Overview
- BIGPOPA is a **local-first desktop app**: React UI (`frontend`) runs inside Electron (`desktop`), which uses IPC to call Python CLIs (`backend`).
- The main user path is: **validate IFs install → model setup → run IFs repeatedly with ML active learning → extract fit metrics → persist to `bigpopa.db`**.
- Electron is the orchestration boundary: `desktop/main.js` manages windows, file pickers, validation, progress relays, and subprocess spawning.
- Python scripts are intentionally stage-oriented (`validate_ifs.py`, `model_setup.py`, `run_ifs.py`, `extract_compare.py`, `ml_driver.py`) and communicate status via line-delimited JSON.
- `bigpopa.db` is the central state store for IFs version metadata, model configs (`model_input`), and scores (`model_output`); it also enables caching/reuse in the ML loop.
- IFs execution artifacts are per-model in `output/<model_id>/` and include copied IFs run DB/SCE plus extracted CSV/parquet-derived files and fit summaries.
- `StartingPointTable.xlsx` controls parameter/coefficient/output variable selection and ML settings; template copies are bundled under `desktop/input/template`.
- Optimization logic is isolated under `backend/optimization/` and called by `ml_driver.py`.

## Repo map (deterministic tree)

```text
.
├── backend/                        # Python pipeline + IFs orchestration
│   ├── validate_ifs.py             # Validate IFs folder/input/output readiness
│   ├── model_setup.py              # Register IFs version, build initial model_input
│   ├── run_ifs.py                  # Apply config + execute ifs.exe + artifact copy
│   ├── extract_compare.py          # Extract IFs outputs, compare to history, write fit metrics
│   ├── ml_driver.py                # Active-learning loop over model configurations
│   ├── prepare_coeff_param.py      # Writes model_input values into Working.sce / Working.run.db
│   ├── log_ifs_version.py          # Populate ifs_static / parameter / coefficient / ifs_version
│   ├── dataset_utils.py            # dataset_id + compatible sample loading
│   ├── combine_var_hist.py         # Helper for merged modeled-vs-history series
│   ├── optimization/               # Surrogate + acquisition + loop internals
│   └── tools/
│       └── ParquetReaderlite.exe   # Converts extracted parquet blobs to CSV
├── desktop/                        # Electron main process + preload bridge
│   ├── main.js                     # BrowserWindow + IPC handlers + Python subprocess bridge
│   ├── preload.js                  # Safe renderer API (`window.electron`)
│   ├── input/
│   │   └── template/
│   │       ├── StartingPointTable_clean.xlsx
│   │       └── bigpopa_clean.db
│   └── output/                     # Default runtime output root (contains bigpopa.db + model folders)
├── frontend/                       # React + Vite renderer app
│   ├── src/main.tsx                # React entrypoint
│   ├── src/App.tsx                 # Validation + tuning UI flow
│   ├── src/api.ts                  # IPC-facing API layer + response normalization
│   └── src/styles.css
├── docs/
│   ├── ARCHITECTURE.md
│   └── DEVELOPMENT_PLAN.md
├── dev.py                          # Convenience script for frontend dev server
└── README.md
```

## Key modules

| Module / file | Purpose | Depends on |
|---|---|---|
| `desktop/main.js` | App lifecycle, folder/file pickers, IPC contract, subprocess management, progress forwarding. | Electron APIs, Python scripts in `backend/`, filesystem. |
| `desktop/preload.js` | Exposes constrained IPC surface to React (`invoke`, `on`, pickers). | Electron `contextBridge`/`ipcRenderer`. |
| `frontend/src/App.tsx` | UX/state machine for validation and tune workflow; listens to progress events. | `frontend/src/api.ts`, preload bridge. |
| `frontend/src/api.ts` | Typed wrappers around IPC channels and stage-response parsing. | `window.electron` channels from `desktop/main.js`. |
| `backend/validate_ifs.py` | Verifies IFs install contents + output folder writability + required Excel sheets. | IFs filesystem layout, SQLite (`IFsInit.db`), Excel zip metadata. |
| `backend/model_setup.py` | Registers IFs version metadata, selects active params/coefs/output set from Excel, inserts `model_input`, bootstraps initial extraction. | `log_ifs_version.py`, `dataset_utils.py`, `extract_compare.py`, `bigpopa.db`. |
| `backend/ml_driver.py` | Runs active-learning loop, chooses candidates, reuses cached fits, calls `run_ifs.py`, emits final best model summary. | `optimization/*`, `dataset_utils.py`, `run_ifs.py`, `bigpopa.db`. |
| `backend/run_ifs.py` | Loads chosen model config, mutates IFs working files, runs `ifs.exe`, snapshots outputs, triggers extraction. | `prepare_coeff_param.py`, IFs executable + runfiles, `extract_compare.py`. |
| `backend/extract_compare.py` | Pulls modeled series blobs, converts/joins with historical tables, computes per-variable and pooled MSE, updates `model_output`. | `ParquetReaderlite.exe`, `combine_var_hist.py`, `IFsHistSeries.db`, pandas, `bigpopa.db`. |
| `backend/optimization/active_learning.py` | Iterative surrogate-based candidate selection with early stopping. | `acquisition_functions.py`, `ensemble_training.py`, numpy. |

## Runtime flows

### Flow 1 — Validation and readiness
1. UI asks Electron to validate selected IFs folder + output folder + input workbook.
2. Electron calls `backend/validate_ifs.py` and relays JSON result.
3. Backend checks required IFs files/folders, extracts base year from `IFsInit.db`, validates workbook sheets, and ensures local template working files exist.
4. UI gates access to tuning view only if validation passes.

### Flow 2 — Model setup (seed configuration)
1. UI sends `{ifsRoot, inputFile, endYear, outputFolder, baseYear?}` to `model_setup` channel.
2. `backend/model_setup.py` logs/loads IFs static metadata into `bigpopa.db` (`ifs_static`, `parameter`, `coefficient`, `ifs_version`).
3. It reads enabled rows in `StartingPointTable.xlsx` and builds canonical `input_param`, `input_coef` (non-zero coefficient selection), and `output_set`.
4. It computes `dataset_id` + deterministic `model_id` hash, inserts into `model_input`, creates `<output>/<model_id>/`, and runs `extract_compare.py` on baseline DB context.

### Flow 3 — Single model execution
1. ML driver (or direct run path) calls `run_ifs.py` with `model_id`.
2. `run_ifs.py` fetches config from `bigpopa.db`, applies it to `Scenario/Working.sce` and `RUNFILES/Working.run.db`, launches `net8/ifs.exe`, and streams year progress.
3. On success it copies `Working.run.db` + `Working.sce` into `<output>/<model_id>/` and resets `Working.run.db` from `IFsBase.run.db`.
4. It calls `extract_compare.py` to compute `fit_var` and `fit_pooled`; DB row in `model_output` is updated to `evaluated`.

### Flow 4 — ML optimization loop
1. `ml_driver.py` loads initial model + compatible historical samples from `bigpopa.db` (same `dataset_id`/structure).
2. It builds search ranges from `parameter`/`coefficient` defaults and sampled grid candidates.
3. `optimization.active_learning_loop` proposes candidates; each candidate is canonicalized to a `model_id`.
4. If `model_output.fit_pooled` already exists for that `model_id`, cached value is reused; otherwise it runs IFs via `run_ifs.py`.
5. Driver emits final JSON containing `best_model_id`, `best_fit_pooled`, and iteration count.

## Data & state

### Core databases
- `desktop/input/template/bigpopa_clean.db`: seed schema/template DB shipped with app.
- `<output>/bigpopa.db`: runtime DB used by all backend stages.
- `<ifs_root>/IFsInit.db`: validation + base year/version source.
- `<ifs_root>/RUNFILES/IFsBase.run.db`: baseline coefficients DB copied to `Working.run.db` between runs.
- `<ifs_root>/RUNFILES/Working.run.db`: mutable run DB for currently executing configuration.
- `<ifs_root>/RUNFILES/IFsHistSeries.db`: historical tables for comparison.

### High-level table contracts (`bigpopa.db`)
- `ifs_static`: unique static IFs version/base-year layer.
- `parameter`: parameter dictionary/ranges/defaults keyed by `ifs_static_id`.
- `coefficient`: regression coefficient dictionary/defaults keyed by `ifs_static_id`.
- `ifs_version`: run-level metadata (`base_year`, `end_year`, fit metric, method) keyed by `ifs_id`.
- `model_input`: one row per deterministic `model_id` with JSON blobs:
  - `input_param` (flat param→value map)
  - `input_coef` (nested function→x→beta→value map)
  - `output_set` (variable→historical table map)
  - `dataset_id` (structure hash for compatibility grouping)
- `model_output`: evaluation state and results per `model_id`:
  - `model_status` (`completed` / `evaluated` / `error`)
  - `fit_var` (JSON variable→MSE)
  - `fit_pooled` (float pooled MSE)

### Other key artifacts
- `desktop/input/template/StartingPointTable_clean.xlsx` and runtime `StartingPointTable.xlsx` copies.
- `<output>/<model_id>/Working.<model_id>.run.db`, `Working.<model_id>.sce`.
- `<output>/<model_id>/*_<model_id>.csv` extraction files, `Combined_*` merged files.
- `<output>/<model_id>/fit_<model_id>.csv` and `fit_<model_id>.json` metrics summaries.
- `<ifs_root>/RUNFILES/progress.txt` parsed for end-year/WGDP completion checks.

## Contracts

### IPC contracts (renderer ↔ electron)
- `validate-ifs-folder(payload)`
  - Input: `{ ifsPath, outputPath?, inputFilePath? }`
  - Output: `{ valid, requirements[], base_year?, pathChecks{ifsFolder,outputFolder,inputFile} }`
- `model_setup(payload)`
  - Input: `{ validatedPath, inputFilePath, baseYear?, endYear, outputFolder? }`
  - Output stage JSON: `{ status, stage:'model_setup', data:{ ifs_id, model_id } }`
- `run-ml(payload)`
  - Input: `{ initialModelId, ifsRoot, outputFolder, endYear, baseYear?, inputFilePath? }`
  - Output: process completion `{ code }` plus streamed logs/progress on channels.

### Backend CLI contracts
- `backend/validate_ifs.py <ifs_path> [--output-path ...] [--input-file ...]`
  - Produces single JSON validation object on stdout.
- `backend/model_setup.py --ifs-root ... --input-file ... --end-year ... --output-folder ... [--base-year ...]`
  - Produces stage JSON with deterministic `model_id` + `ifs_id`.
- `backend/ml_driver.py --ifs-root ... --end-year ... --output-folder ... --initial-model-id ... --bigpopa-db ...`
  - Produces stage JSON for start/success/error; success data includes best model metrics.
- `backend/run_ifs.py --ifs-root ... --end-year ... --output-dir ... --model-id ... --ifs-id ... --base-year ...`
  - Produces run + extraction stage JSON and streams IFs text progress lines.
- `backend/extract_compare.py --ifs-root ... --model-db ... --input-file ... --model-id ... --ifs-id ... [--bigpopa-db ...]`
  - Produces fit artifacts on disk and updates `model_output.fit_var/fit_pooled`.

## Extension points / where to add features
- **New UI controls or workflow steps:** `frontend/src/App.tsx` and typed bridge in `frontend/src/api.ts`; add corresponding IPC handler in `desktop/main.js`.
- **New backend stage/CLI:** add script in `backend/`, emit stage JSON (`status/stage/message/data`) so existing renderer patterns can consume it.
- **Improve optimization strategy:** extend `backend/optimization/` and wire into `ml_driver.py` (new acquisition function, surrogate model, stopping rule).
- **Add new fit metrics:** evolve `extract_compare.py` (compute/store additional metrics), then update `model_output` usage in UI/ML ranking.
- **Schema evolution:** apply migrations/DDL in one place per script (`ensure_bigpopa_schema` patterns), and keep `model_setup.py` + `extract_compare.py` + `ml_driver.py` aligned.
- **Additional input workbook sheets:** parse in `model_setup.py` and/or `ml_driver.py` (`_load_ml_settings`) with explicit defaults.

## Known sharp edges / assumptions
- `backend/prepare_coeff_param.py` is labeled placeholder but currently performs critical writes to `Working.sce` and `Working.run.db`; behavior is simple and may not cover advanced IFs cases.
- Several schema checks are duplicated (`ensure_bigpopa_schema` in multiple scripts), so table changes must be synchronized manually.
- `run-ml` IPC currently returns only `{code}` while richer stage payload logic also exists elsewhere; UI depends mainly on streamed logs/status text.
- `extract_compare.py` requires Windows binary `backend/tools/ParquetReaderlite.exe`; missing binary degrades conversion and may leave expected CSV artifacts absent.
- Validation/bootstrap paths assume desktop-local working files under `desktop/input` and `desktop/output`; packaged/runtime paths must preserve this layout.
