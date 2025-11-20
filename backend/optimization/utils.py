"""Utility helpers for reproducibility."""

import random

import numpy as np

def set_global_seed(seed: int = 0) -> None:
    """Set NumPy and Python random seeds for reproducible simulations."""

    np.random.seed(seed)
    random.seed(seed)
    print(f"Global seed set to: {seed}")
