import math

import numpy as np

_erf = np.vectorize(math.erf)


def lcb(mu, sigma, kappa=1.6):
    return mu - kappa * sigma


def _norm_pdf(x: np.ndarray) -> np.ndarray:
    return (1.0 / math.sqrt(2 * math.pi)) * np.exp(-0.5 * x**2)


def _norm_cdf(x: np.ndarray) -> np.ndarray:
    return 0.5 * (1 + _erf(x / math.sqrt(2)))


def expected_improvement(mu, sigma, y_best, xi=0.01):
    sigma = np.maximum(sigma, 1e-8)
    imp = y_best - mu - xi
    Z = imp / sigma
    ei = imp * _norm_cdf(Z) + sigma * _norm_pdf(Z)
    return ei
