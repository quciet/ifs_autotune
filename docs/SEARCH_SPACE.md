# Workbook, Bounds, And Search Space Rules

This document captures the tuning logic that is easy to miss when only using the UI. It is based on the current implementation in `model_setup.py`, `ml_driver.py`, `dataset_utils.py`, `log_ifs_version.py`, and `extract_compare.py`.

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
  Configures ML run settings such as sample count, stopping rules, fit metric, and ML method text.

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

## Grid And Random Modes

BIGPOPA has two sampling families:
- legacy random sampling
- grid-aware sampling

The switch is simple:
- if no enabled dimension has `Step` or `LevelCount`, BIGPOPA uses random sampling
- if any enabled dimension has `Step` or `LevelCount`, BIGPOPA switches into grid-aware mode

## Random Mode

In legacy random mode:
- candidate values are drawn independently and uniformly inside each dimension range
- a fixed RNG seed of `0` is used
- identical inputs produce the same candidate pool

If a dimension has identical low and high bounds, that dimension stays constant in every sample.

## Explicit Grid Rules

`Step` and `LevelCount` are read from:
- `IFsVar` for parameters
- `TablFunc` and `AnalFunc` for coefficients

### Parsing rules

- `Step` must be numeric, finite, and greater than `0`
- `LevelCount` must be numeric, finite, an integer, and at least `1`
- if both are supplied, `Step` wins

### `Step` semantics

Stepped values:
- start at `Minimum`
- repeatedly add `Step`
- stop before the next value would exceed `Maximum`

BIGPOPA does not force the maximum endpoint to appear unless the step lands on it.

### `LevelCount` semantics

- `LevelCount > 1` uses `linspace(minimum, maximum, level_count)`
- `LevelCount = 1` uses the dimension default, clipped into `[minimum, maximum]`

### Example

If:
- minimum `0`
- maximum `1`
- step `0.4`

Then the explicit levels are:
- `0.0`
- `0.4`
- `0.8`

The value `1.0` is not added because the next stepped point would overshoot.

## Full Cartesian Grid

If all tuned dimensions are explicit, BIGPOPA builds the Cartesian product of their level values.

If some dimensions are explicit and others are not, BIGPOPA can still create a grid-like pool by inferring counts for the unspecified dimensions.

### Role of `n_sample`

In grid-aware mode, `n_sample` is not always the exact number of final combinations from the start. Instead it is the cap or target used to decide how dense the candidate pool is allowed to become.

For unspecified dimensions:
- BIGPOPA starts each one at a level count of `1`
- it repeatedly increments the currently smallest count
- ties are broken deterministically by dimension key
- it stops when another increment would push the total Cartesian product above `n_sample`

If the explicit grid alone already exceeds `n_sample`, BIGPOPA raises an error.

### Example

If:
- one explicit dimension has `20` levels
- one explicit dimension has `5` levels
- `n_sample = 99`

Then BIGPOPA fails because the explicit grid already requires `100` combinations.

## Hybrid Grid

Hybrid mode happens when:
- at least one dimension is explicit
- at least one dimension is still free

In this case BIGPOPA:
1. builds the explicit Cartesian grid
2. divides `n_sample` across those explicit combinations
3. randomly samples the free dimensions inside each explicit combination

The allocation rule is:
- `base_count, remainder = divmod(n_sample, explicit_count)`
- each explicit combination gets `base_count`
- the first `remainder` combinations get one extra sample

This means:
- every explicit combination is represented
- extra budget is assigned deterministically to the earliest combinations

### Example

If:
- explicit combinations = `6`
- `n_sample = 20`

Then the allocation becomes:
- first `2` combinations get `4` samples each
- remaining `4` combinations get `3` samples each

## ML Settings From The `ML` Sheet

BIGPOPA reads `ML` rows where:
- `Method` is `general`

Supported parameters:
- `n_sample`
- `n_max_iteration`
- `n_convergence`
- `min_convergence_pct`
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
