# BIGPOPA Development Plan

## Deferred ML Ideas

- Cross-dataset subset-transfer reuse is intentionally deferred.
  The active ML path now trains only on prior runs from the exact same `dataset_id` cohort.
  The earlier subset/imputation concept is preserved as a future design option, but it is not active because cross-cohort `fit_pooled` comparability needs a clearer policy before it should influence the surrogate.
