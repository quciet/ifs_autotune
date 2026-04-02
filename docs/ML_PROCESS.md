# ML Process And Neural-Network Search

This document explains the current machine-learning process used by BIGPOPA. It is implementation-faithful to the current code path, not a future design document.

The current desktop flow uses the persisted `ml_method` stored in `bigpopa.db`. This document focuses on the active neural-network path and the shared acquisition logic around it.

## What The ML Process Starts With

When `ml_driver.py` starts, it does not begin from an empty training set.

The initial observed data includes:
- the scored baseline model created during model setup
- any prior runs from the exact same `dataset_id` cohort that already have `fit_pooled`

Those observations become:
- `X_obs`
  Flattened input vectors containing all tuned parameters and coefficients
- `Y_obs`
  The corresponding pooled fit scores

So the first ML iteration already has real scored samples to learn from.

## What Is Configured From The Workbook Versus The Database

Still read from `StartingPointTable.xlsx` at runtime:
- `n_sample`
- `n_max_iteration`
- `n_convergence`
- `min_convergence_pct`

Persisted during setup and replayed from `bigpopa.db`:
- `ifs_version.ml_method`
- `ifs_version.fit_metric`

That means the runtime surrogate family is locked in from setup, while the current iteration-count and sample-budget controls still come from the workbook.

## Input Preprocessing

Before training the surrogate ensemble, BIGPOPA applies two important transforms.

### BoundsScaler

`BoundsScaler` maps every tuned dimension from its raw bounded range into `[-1, 1]`.

Current behavior:
- the lower bound maps to `-1`
- the upper bound maps to `1`
- values are clipped into that range
- dimensions with zero span stay constant

Why this matters:
- parameters and coefficients can live on very different numeric scales
- scaling makes NN training more stable
- the default distance penalty is also computed in this scaled space

### LogClippedTargetTransform

`LogClippedTargetTransform` compresses large positive loss values before training.

Current defaults:
- upper quantile: `95.0`
- absolute cap: `FAIL_Y`

Current behavior:
- BIGPOPA fits an upper clip threshold from the observed `Y_obs`
- it clips large losses to that threshold
- it applies `log1p(...)` before training
- predictions are mapped back to the original loss scale for acquisition and reporting

Why this matters:
- very bad runs or failed runs should not dominate the surrogate fit
- the NN sees a less extreme target distribution than the raw loss values

## Neural-Network Ensemble

The surrogate used for `ml_method = neural network` is a feed-forward PyTorch network.

Current defaults:
- hidden layers: `[32, 32]`
- activation: `relu`
- dropout: `0.0`
- epochs: `200`
- learning rate: `1e-3`
- optimizer: `Adam`
- loss: mean squared error

Each ensemble member predicts one scalar output:
- the pooled fit loss for that candidate input

## How Ensemble Training Works

The active-learning loop retrains the surrogate ensemble every iteration on the full accumulated observed dataset.

Current defaults:
- ensemble size `M = 8`
- bootstrap enabled in the desktop ML path

Current per-iteration training process:

1. Take the current observed samples `X_obs` and `Y_obs`.
2. Fit the target transform on the current `Y_obs`.
3. For each of the 8 ensemble members:
   - draw a bootstrap resample of the observed rows
   - scale inputs with `BoundsScaler`
   - transform targets with `LogClippedTargetTransform`
   - train one NN model
4. Keep all ensemble members for prediction.

Why bootstrap matters:
- each model sees a slightly different resampled dataset
- the ensemble therefore produces a spread of predictions
- that spread is used as an uncertainty signal during acquisition

## What Gets Recomputed Each Iteration

Recomputed each iteration:
- the full surrogate ensemble
- the current refreshed proposal pool
- the ensemble predictions for that pool
- the acquisition ranking

Reused across iterations:
- all previously observed samples
- all cached exact scores for already evaluated points
- the search-space bounds and scaling definition

Not persisted:
- the current proposal pool
- intermediate surrogate model weights

Persisted:
- evaluated model configurations
- their fit scores
- run status and artifacts

## Refreshed Proposal Pool

The current desktop flow uses a candidate generator, not one static `X_grid` for the entire run.

Current default:
- proposal mode `refreshed`
- `candidate_refresh_interval = 1`

That means a fresh proposal pool is generated every iteration.

The per-iteration pool:
- is capped by workbook `n_sample`
- is held in RAM only for that iteration
- is not written into `bigpopa.db`
- is logged with its realized shape and raw NumPy memory size

## How The Next Input Is Proposed

BIGPOPA does not directly optimize the acquisition function in continuous space today. Instead it:

1. Builds a refreshed proposal pool for the current iteration.
2. Predicts ensemble mean and uncertainty for that pool.
3. Applies the acquisition function.
4. Picks the best unevaluated candidate from the pool.

### Discrete versus continuous dimensions

The proposal generator separates:
- explicit dimensions
  Dimensions with `Step` or `LevelCount`
- free dimensions
  Dimensions without explicit levels

It then:
- enumerates or samples discrete combinations
- fills continuous coordinates with a mix of global and local proposals

### Global proposals

Global proposals are uniform random draws across the full bounded range of the free dimensions.

Current default:
- 25% of each combination's proposal rows

### Local proposals

Local proposals are drawn around good observed points.

Current defaults:
- use the top `k = 5` best observed samples by `fit_pooled`
- prefer seed rows matching the same discrete combination
- otherwise fall back to the global top rows
- sample with a Gaussian radius based on the current iteration

Current radius schedule:
- starts at `15%` of the dimension span
- multiplies by `0.85` each iteration
- bottoms out at `5%`

In plain language:
- early iterations look broadly around good areas
- later iterations tighten around the most promising region

## Acquisition Function

The active default acquisition is LCB, lower confidence bound.

Current defaults:
- acquisition: `LCB`
- `kappa_start = 1.6`
- `kappa_end = 0.8`

Each iteration:
- the ensemble predicts mean `mu`
- the ensemble predicts uncertainty `sigma`
- BIGPOPA computes `LCB = mu - kappa * sigma`
- lower scores are preferred

What this means:
- low predicted loss is good
- high uncertainty can also make a candidate attractive
- the search balances exploitation and exploration

As the run progresses, `kappa` decreases from `1.6` to `0.8`, so the search gradually becomes a bit less uncertainty-seeking.

## Default Distance Penalty

The current desktop path enables a default distance penalty.

Current defaults:
- penalty enabled
- strength `0.15`

Current behavior:
- BIGPOPA scales candidate inputs into `[-1, 1]`
- it computes squared distance from the baseline default configuration
- it multiplies that by the configured strength
- for LCB, it adds that penalty to the acquisition score

Practical effect:
- two candidates with similar surrogate value will favor the one closer to the default configuration
- the penalty is a bias, not a hard constraint

## Exact Reuse And Historical Reuse

BIGPOPA reuses information in two different ways.

### Exact reuse

If the selected candidate hashes to a `model_id` that already has a stored `fit_pooled`, BIGPOPA reuses that score instead of rerunning IFs.

### Historical reuse

Before the first new iteration, BIGPOPA also loads prior runs from the exact same `dataset_id`.

So the surrogate learns from:
- the baseline
- any earlier compatible scored runs
- every new or reused score encountered during the current run

## Stopping Behavior

The ML loop can stop because:
- it reached `n_max_iteration`
- the graceful stop signal was requested
- improvement stayed below `min_convergence_pct` for `n_convergence` consecutive iterations
- the current proposal pool contains no unevaluated candidate

The last case is important because the current loop judges exhaustion against the current refreshed pool, not the entire theoretical search space.

## Current Limitations

- `run_seed` makes the proposal-pool sampling reproducible, but the full neural-network run is not yet guaranteed to be exactly reproducible end to end.
- The active search path is proposal-pool ranking, not direct acquisition optimization.
- `direct` proposal mode is a placeholder for a future implementation and is not the active default.
