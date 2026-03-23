# Workbook, Bounds, And Search Space Rules

This document captures the tuning logic that is easy to miss when only using the UI. It is based on the current implementation in `model_setup.py`, `ml_driver.py`, `dataset_utils.py`, `log_ifs_version.py`, and `extract_compare.py`.

For the training and acquisition details of the surrogate model itself, see [ML Process And Neural-Network Search](ML_PROCESS.md).

## Workbook Responsibilities

BIGPOPA reads several sheets from `StartingPointTable.xlsx`.

### Required sheets

- `IFsVar`
  Selects which IFs parameters participate in the model.
- `TablFunc`
  Selects coefficient rows for table functions.
- `AnalFunc`
  Selects coefficient rows for analytical functions.
- `DataDict`
  Selects which output variables will be compared to historical data.

### Optional sheet

- `ML`
  Configures runtime ML settings such as sample count, stopping rules, fit metric, and ML method text.

Validation requires the first four sheets. The `ML` sheet is optional. If it is missing or unreadable, BIGPOPA falls back to defaults.

## Baseline Configuration Rules

Model setup creates the baseline config before any ML candidate is generated.

### Parameter selection

Parameters are selected from `IFsVar` rows where:
- the sheet has `Switch` and `Name` columns
- `Switch == 1`

For each selected parameter name, BIGPOPA looks up:
- `parameter.param_default`

That default becomes the baseline parameter value stored in `model_input.input_param`.

### Coefficient selection

Coefficients are selected from enabled rows in `TablFunc` and `AnalFunc` where the row has:
- `Function Name`
- `XVariable`
- `YVariable`
- `Coefficient`

For each selected coefficient identity, BIGPOPA looks up:
- `coefficient.beta_default`

That default becomes the baseline coefficient value stored in `model_input.input_coef`.

### Output-variable selection

Output variables come from `DataDict` rows where:
- `Switch == 1`
- `Variable` and `Table` are both present

The result is stored in `model_input.output_set`.

### Important note on `Switch`

Current code is not perfectly symmetric:
- baseline parameter selection in `IFsVar` expects `Switch == 1`
- baseline output selection in `DataDict` expects `Switch == 1`
- search/grid parsing accepts numeric `1` or string `on`
- coefficient row collection also accepts numeric `1` or string `on`

For predictable behavior, use numeric `1` in workbook `Switch` columns.

## Where Bounds Come From

The ML driver builds the search space from:
- the baseline config in `model_input`
- IFs metadata already recorded in `bigpopa.db`
- user overrides from the workbook

The relevant metadata tables are:
- `parameter`
- `coefficient`

## Parameter Bound Precedence

For each parameter in the baseline config:

1. BIGPOPA starts from the baseline value in `input_param`.
2. If `parameter.param_default` exists, that becomes the dimension default.
3. If the workbook row provides `Minimum` or `Maximum`, those override the corresponding DB side.
4. Otherwise BIGPOPA uses:
   - `parameter.param_min`
   - `parameter.param_max`
5. If a DB bound is missing, BIGPOPA falls back to:
   - `default - abs(default)`
   - `default + abs(default)`
6. If the default is exactly zero, the fallback becomes `[-1, 1]`.
7. If the final `min > max`, BIGPOPA swaps them and logs a warning.

### Example

If a parameter has:
- default `5`
- no Excel overrides
- no DB min/max

Then BIGPOPA uses:
- minimum `0`
- maximum `10`

If the default is `0`, the fallback becomes:
- minimum `-1`
- maximum `1`

## Coefficient Bound Precedence

For each coefficient in the baseline config:

1. BIGPOPA starts from the baseline coefficient value in `input_coef`.
2. If `coefficient.beta_default` exists, that becomes the search-space center.
3. If `coefficient.beta_std` exists, the DB range becomes:
   - `center - 3 * abs(beta_std)`
   - `center + 3 * abs(beta_std)`
4. Workbook `Minimum` and `Maximum` override the corresponding sides when provided.
5. If no DB range exists, BIGPOPA falls back to:
   - `center - abs(center)`
   - `center + abs(center)`
6. If the center is exactly zero, the fallback becomes `[-1, 1]`.
7. If `beta_default` exists and Excel did not specify both bounds, BIGPOPA preserves sign:
   - positive default forces the lower bound up to at least `0`
   - negative default forces the upper bound down to at most `0`
8. If the final `min > max`, BIGPOPA swaps them and logs a warning.

### Example

If a coefficient has:
- `beta_default = 2.0`
- `beta_std = 0.3`
- no Excel overrides

Then BIGPOPA uses:
- minimum `1.1`
- maximum `2.9`

If the same coefficient has only an Excel maximum of `4.0`, BIGPOPA still preserves positivity because Excel did not fully specify both bounds.

## Search-Space Roles

Once bounds are built, each tuned dimension falls into one of two roles:

- explicit dimensions
  Dimensions with `Step` or `LevelCount`. These create discrete level sets.
- free dimensions
  Dimensions without `Step` or `LevelCount`. These remain continuous and are sampled within bounds.

This distinction now drives the refreshed proposal policy.

## Refreshed Proposal Policy

The current desktop flow uses a refreshed proposal generator, not one fixed candidate matrix for the whole run.

Current default:
- proposal mode is `refreshed`
- `candidate_refresh_interval = 1`

That means BIGPOPA rebuilds a new proposal pool every iteration.

### What `n_sample` means now

`n_sample` is still the workbook-controlled cap on the per-iteration proposal pool size.

In the current policy:
- BIGPOPA targets up to `n_sample` candidate rows per iteration
- those rows are allocated across selected discrete combinations
- the realized pool is deduplicated and then backfilled if needed

The current pool exists in RAM only for that iteration and is not persisted to `bigpopa.db`.

## Discrete Combination Selection

BIGPOPA first decides which explicit discrete combinations to consider for the current iteration.

### Small discrete spaces

If the Cartesian product of all explicit dimensions is small enough, BIGPOPA enumerates every discrete combination.

The current threshold is controlled by:
- `MAX_ENUMERATED_DISCRETE_COMBINATIONS = 4096`
- and the current `n_sample`

Enumeration happens when:
- `total_possible <= min(4096, n_sample)`

### Large discrete spaces

If the discrete product is larger than that threshold, BIGPOPA does not enumerate everything.

Instead it:
- computes a discrete-combination budget from `n_sample`
- uses a balanced subset sampler
- tries to spread representation across the level values of each explicit dimension

Current behavior uses:
- at least about `4` total candidates per selected discrete combination
- deduplication of repeated sampled combinations

This makes the discrete side of the search broader than greedy random picking without requiring a full Cartesian explosion.

## Candidate Allocation Within Each Discrete Combination

After BIGPOPA chooses the discrete combinations for the current iteration, it divides the total `n_sample` budget across them:

- `base_count, remainder = divmod(n_sample, combo_count)`
- each combination gets `base_count`
- the first `remainder` combinations get one extra row

So every chosen discrete combination gets representation in the current proposal pool.

## Global And Local Continuous Proposals

For each selected discrete combination, BIGPOPA fills the free dimensions with a mix of global and local proposals.

### Global proposals

A fraction of each combination's rows is sampled uniformly across the full allowed bounds of the free dimensions.

Current default:
- `DEFAULT_GLOBAL_PROPOSAL_FRACTION = 0.25`

In plain language, about 25% of each combination's candidates are broad exploration points.

### Local proposals

The remaining rows are sampled around the current best observed regions.

Current behavior:
- seed rows come from the top `k=5` best observed samples by `fit_pooled`
- if possible, BIGPOPA prefers top observed rows matching the same discrete combination
- if no matching top rows exist, it falls back to the global top rows

These local rows are sampled with a Gaussian draw around the seed point and clipped back into the configured bounds.

## Adaptive Local Radius

Local proposals narrow over time.

Current defaults:
- initial local radius fraction: `0.15`
- decay factor per iteration: `0.85`
- minimum local radius fraction: `0.05`

That means:
- early iterations explore broader neighborhoods around good points
- later iterations tighten around the best observed regions
- the local neighborhood never shrinks below 5% of the dimension span

## Deduplication And Backfill

The refreshed pool is deduplicated before it is used.

Current behavior:
- each row is rounded for deduplication
- if duplicates reduce the pool below the requested size, BIGPOPA keeps sampling additional rows
- those backfill rows respect the same bounds and explicit/free dimension structure

This is why the realized pool can differ from a naive “Cartesian product plus samples” picture.

## Active In-Code Defaults

Current search-policy defaults in code are:

- proposal mode: `refreshed`
- candidate refresh interval: `1`
- distance penalty: on
- distance penalty strength: `0.15`
- candidate memory budget default: `512 MB`
- ensemble size: `8`
- acquisition: `LCB`
- `kappa` anneals from `1.6` to `0.8`
- bootstrap: on in the active-learning loop used by `ml_driver.py`

### Distance penalty

The default distance penalty is applied in scaled input space.

Current effect:
- candidates farther from the baseline default configuration get a higher penalty
- with LCB, that penalty is added to the acquisition score
- this gently biases the search back toward default-space proximity unless the surrogate predicts a strong enough benefit elsewhere

## ML Settings From The `ML` Sheet

BIGPOPA reads `ML` rows where:
- `Method` is `general`

Still read from the workbook at runtime:
- `n_sample`
- `n_max_iteration`
- `n_convergence`
- `min_convergence_pct`

Persisted during setup and replayed from `bigpopa.db`:
- `fit_metric`
- `ml_method`

Defaults when the `ML` sheet is missing or incomplete:
- `n_sample = 200`
- `n_max_iteration = 30`
- `n_convergence = 10`
- `min_convergence_pct = 0.01 / 100 = 0.0001`
- `fit_metric = mse`
- `ml_method = neural network`

### `min_convergence_pct` units

The workbook entry is interpreted as a percent, not a fraction.

Example:
- workbook value `0.01`
- meaning `0.01%`
- internal threshold `0.0001`

## Reuse, Caching, And Cohorting

BIGPOPA has two reuse mechanisms.

### 1. Exact configuration reuse via `model_id`

`model_id` is a SHA-256 hash of the canonical configuration:
- `ifs_id`
- `input_param`
- `input_coef`
- `output_set`

Numbers are rounded before hashing, so equivalent configs map to the same ID. If `model_output.fit_pooled` already exists for that `model_id`, BIGPOPA reuses the stored score and skips the IFs run.

### 2. Historical sample reuse via `dataset_id`

`dataset_id` groups runs with the same structural shape:
- same `ifs_id`
- same parameter keys
- same coefficient keys
- same output variable keys

It does not include numeric values.

The ML driver loads compatible prior samples from the same `dataset_id` cohort and uses them as observed training points before proposing new candidates.

## Fit Metrics

The scoring stage supports:
- `mse`
- `r2`

If the stored fit metric is missing or unknown, BIGPOPA falls back to `mse`.

### MSE behavior

- BIGPOPA computes squared error from overlapping modeled and historical values
- `fit_var` stores per-variable MSE
- `fit_pooled` stores pooled MSE across all valid points

### R2 behavior

- BIGPOPA computes country-level aggregates when enough data is present
- a country needs at least `3` overlapping points
- countries with zero historical variance are skipped in pooled R2
- the final stored `fit_pooled` is `1 - pooled_r2`

That last step is important: even when using `r2`, BIGPOPA stores a loss-like value so lower remains better during optimization.

## Current Limitations

- The refreshed pool is deterministic from `run_seed`, but the full neural-network search is not yet fully reproducible end to end.
- `direct` proposal mode is a placeholder for a future implementation and is not the active default path.
- If a refreshed pool is fully covered by cached observations, the current loop can stop even if unexplored points remain outside that sampled pool.
