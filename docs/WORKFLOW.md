# BIGPOPA Workflow And Runtime Artifacts

This document explains the operator-facing workflow in BIGPOPA and maps each step to the files and records created by the backend.

## High-Level Flow

1. Select an IFs installation folder.
2. Select an output folder.
3. Select `StartingPointTable.xlsx`.
4. Validate the environment.
5. Run model setup to register IFs metadata and seed the baseline model.
6. Start the ML tuning run.
7. Review the best model and the saved artifacts in the output folder.

## 1. Validation

The UI asks the user for:
- an IFs root folder
- an output folder
- an input workbook path

The backend validation step checks:
- required IFs files and folders:
  - `IFsInit.db`
  - `DATA/SAMBase.db`
  - `RUNFILES/DataDict.db`
  - `RUNFILES/IFsHistSeries.db`
  - `RUNFILES/`
  - `Scenario/`
  - `net8/ifs.exe`
- a writable output folder
- a readable workbook with these required sheets:
  - `AnalFunc`
  - `TablFunc`
  - `IFsVar`
  - `DataDict`

The validation response also advertises `Step` and `LevelCount` as optional workbook columns for `IFsVar`, `TablFunc`, and `AnalFunc`, but it does not require them.

Base year detection comes from `IFsInit.db`:
- BIGPOPA first looks for `LastYearHistory%`
- it also looks for `FirstYearForecast%`
- if both are present and consistent, it prefers the historical year as the base year

## 2. Model Setup

`model_setup.py` is the baseline-seeding step. It does more than just prepare a run.

It:
- logs IFs version metadata into `bigpopa.db`
- imports parameter metadata into the `parameter` table
- imports coefficient metadata into the `coefficient` table
- reads workbook selections
- reads and normalizes the workbook `ml_method`
- persists `ml_method` and `fit_metric` into `ifs_version`
- builds a deterministic baseline config
- inserts that config into `model_input`
- runs extraction immediately so the baseline gets a score

### What gets selected from the workbook

- Parameters come from enabled rows in `IFsVar`.
- Coefficients come from enabled rows in `TablFunc` and `AnalFunc`.
- Output variables come from enabled rows in `DataDict`.
- Runtime ML controls such as `n_sample`, `n_max_iteration`, `n_convergence`, and `min_convergence_pct` remain workbook-driven.

### Where the baseline values come from

The baseline is not seeded from workbook min/max values. Instead:
- parameter values come from `bigpopa.db.parameter.param_default`
- coefficient values come from `bigpopa.db.coefficient.beta_default`

If a selected parameter is missing from the `parameter` table, model setup fails. For coefficients, BIGPOPA keeps only rows it can match to IFs coefficient metadata.

### IDs created during setup

- `dataset_id`
  A structure hash based on `ifs_id` plus the selected parameter keys, coefficient keys, and output variable keys. It does not depend on the actual numeric values.
- `model_id`
  A SHA-256 hash of the canonicalized baseline configuration, including rounded numeric values.

These IDs matter later:
- `dataset_id` controls which prior runs are considered structurally compatible for ML training history
- `model_id` controls exact-result reuse for identical configurations

### Baseline extraction

After inserting the baseline config, BIGPOPA calls `extract_compare.py` immediately using `IFsBase.run.db`. That creates an initial fit record before any ML-driven IFs run starts.

## 3. ML Tuning Run

Once model setup succeeds, the UI starts `ml_driver.py`.

The ML driver:
- loads the selected baseline config from `model_input`
- loads `ml_method` and `fit_metric` from `ifs_version`
- loads compatible historical samples using `dataset_id`
- builds parameter and coefficient search bounds
- builds the refreshed proposal generator
- runs active learning against a per-iteration candidate pool

### What the observed training set looks like at the start

Before the first new IFs run, the ML process already has observations:
- the scored baseline model
- any prior compatible runs from the same `dataset_id` cohort that already have `fit_pooled`

That observed set becomes the initial training data for the surrogate ensemble.

### Current control split

Still read from `StartingPointTable.xlsx` at runtime:
- `n_sample`
- `n_max_iteration`
- `n_convergence`
- `min_convergence_pct`

Persisted in `bigpopa.db` and replayed by the ML process:
- `ifs_version.ml_method`
- `ifs_version.fit_metric`

### Runtime loop

The active-learning loop now works like this:

1. Train a surrogate ensemble on all currently observed compatible samples.
2. Build a fresh candidate pool for the current iteration.
3. Rank that pool with the acquisition function.
4. Select the best unevaluated candidate.
5. Reuse a prior score if the same `model_id` already exists.
6. Otherwise run IFs, extract outputs, compute fit scores, and append the result.
7. Repeat until the run converges, exhausts the iteration budget, or is stopped.

### Candidate-pool lifecycle

The candidate pool is no longer one persistent `X_grid` for the whole run.

Current behavior:
- a fresh proposal pool is generated each iteration
- the pool exists in RAM only for the current iteration
- it is not written to `bigpopa.db`
- BIGPOPA logs the realized pool shape and raw NumPy memory size when it is generated

### Candidate evaluation lifecycle

For each chosen candidate:
- BIGPOPA reconstructs parameter and coefficient dictionaries from the numeric vector
- it hashes the canonical configuration into a `model_id`
- if that `model_id` already has a `fit_pooled` value in `model_output`, BIGPOPA reuses the score instead of running IFs again
- otherwise it inserts the config into `model_input` and launches `run_ifs.py`

## 4. IFs Execution

`run_ifs.py` is responsible for turning one model configuration into a physical IFs run.

Before launching `ifs.exe`, it:
- reads `input_param` and `input_coef` for the selected `model_id`
- resolves `ifs_static_id` from `ifs_version`
- writes parameters into `Scenario/Working.sce`
- writes coefficients into `RUNFILES/Working.run.db`
- refreshes `RUNFILES/ifsForDyadicWork.db` from `DATA/IFsForDyadic.db`

### `Working.sce` writing policy

Parameter lines in `Working.sce` follow the IFs metadata dimension flag:
- if `param_type` parses to `1`, BIGPOPA writes `CUSTOM,<param>,World,...`
- if `param_type` parses to `0`, BIGPOPA writes `CUSTOM,<param>,...`
- otherwise the parameter is skipped

This logic is shared by `prepare_coeff_param.py` and `common_sce_utils.py`.

### After IFs finishes

BIGPOPA:
- checks `RUNFILES/progress.txt`
- verifies the final year matches the requested end year
- copies `Working.run.db` and `Working.sce` into `<output>\<model_id>\`
- resets `RUNFILES/Working.run.db` from `RUNFILES/IFsBase.run.db`
- calls `extract_compare.py`

## 5. Extraction And Fit Scoring

`extract_compare.py` computes the score for a model.

It:
- reads `output_set` from `model_input`
- extracts each requested IFs variable blob from the run DB
- writes parquet payloads into the model folder
- converts parquet files to CSV with `backend/tools/ParquetReaderlite.exe`
- extracts the matching historical table from `RUNFILES/IFsHistSeries.db`
- writes combined comparison CSV files
- computes per-variable and pooled fit metrics
- updates `model_output.fit_var` and `model_output.fit_pooled`

The fit metric is controlled by `ifs_version.fit_metric`, which is populated during model setup and replayed during the ML run.

## 6. Output Layout

The selected output folder becomes the runtime workspace for BIGPOPA.

Typical contents:
- `<output>\bigpopa.db`
- `<output>\<model_id>\Working.<model_id>.run.db`
- `<output>\<model_id>\Working.<model_id>.sce`
- `<output>\<model_id>\<name>_<model_id>.csv` extracted model or historical files
- `<output>\<model_id>\Combined_<variable>_<model_id>.csv`
- `<output>\<model_id>\fit_<model_id>.csv`
- `<output>\<model_id>\fit_<model_id>.json`

## Model Status Meanings

The backend uses several statuses in `model_output`:

- `running`
  The ML driver has selected the candidate and is currently evaluating it.
- `reused`
  BIGPOPA found a previously evaluated identical `model_id` and reused its stored `fit_pooled`.
- `completed`
  IFs execution finished and artifacts were copied, but extraction may still be running.
- `evaluated`
  Extraction and scoring finished successfully. `fit_var` and `fit_pooled` are available.
- `failed`
  IFs execution failed or a required artifact was missing after the run.
- `error`
  Extraction or comparison failed.

In practice, the final success state for a scored model is `evaluated`.

## Current Limitations

- `run_seed` makes the refreshed proposal pool reproducible, but it does not yet guarantee full neural-network reproducibility because ensemble bootstrap and PyTorch initialization are not fully tied to the same seed path.
- `direct` proposal mode exists as a planned placeholder in the codebase, but the active desktop flow uses the `refreshed` generator path.
- If a refreshed proposal pool happens to be fully covered by already cached results, the current loop can stop even if unexplored points still exist outside that sampled pool.
