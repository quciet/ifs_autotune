"""Lightweight surrogate model abstractions used by the ensemble."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import torch
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from sklearn.tree import DecisionTreeRegressor
from torch import nn, optim


@dataclass
class PolynomialSurrogate:
    """Multivariate polynomial surrogate using sklearn's PolynomialFeatures."""

    model: LinearRegression
    poly: PolynomialFeatures

    def predict(self, X: np.ndarray) -> np.ndarray:
        X = np.atleast_2d(np.asarray(X, dtype=float))
        Xp = self.poly.transform(X)
        return self.model.predict(Xp)

    @classmethod
    def fit(cls, X: np.ndarray, Y: np.ndarray, degree: int = 3) -> "PolynomialSurrogate":
        X = np.atleast_2d(np.asarray(X, dtype=float))
        Y = np.asarray(Y, dtype=float).flatten()
        poly = PolynomialFeatures(degree=degree, include_bias=True)
        Xp = poly.fit_transform(X)
        reg = LinearRegression().fit(Xp, Y)
        return cls(model=reg, poly=poly)


class TreeSurrogate:
    """Decision-tree surrogate that supports multi-dimensional X."""

    def __init__(self, model: DecisionTreeRegressor):
        self.model = model

    @classmethod
    def fit(
        cls,
        X: np.ndarray,
        Y: np.ndarray,
        max_depth: int = 5,
        random_state: int | None = None,
    ) -> "TreeSurrogate":
        X = np.atleast_2d(np.asarray(X, dtype=float))
        Y = np.asarray(Y, dtype=float).flatten()
        reg = DecisionTreeRegressor(max_depth=max_depth, random_state=random_state)
        reg.fit(X, Y)
        return cls(reg)

    def predict(self, X: np.ndarray) -> np.ndarray:
        X = np.atleast_2d(np.asarray(X, dtype=float))
        return self.model.predict(X)


class NNSurrogate:
    """Configurable PyTorch neural-network surrogate supporting multi-dimensional X."""

    def __init__(self, model: nn.Module):
        self.model = model

    @classmethod
    def fit(
        cls,
        X: np.ndarray,
        Y: np.ndarray,
        hidden_layers: list[int] = [32, 32],
        activation: str = "relu",
        dropout: float = 0.0,
        epochs: int = 200,
        lr: float = 1e-3,
    ) -> "NNSurrogate":
        X = np.atleast_2d(np.asarray(X, dtype=float))
        if X.ndim == 1:
            X = X.reshape(-1, 1)
        Y = np.asarray(Y, dtype=float).reshape(-1, 1)

        X_t = torch.tensor(X, dtype=torch.float32)
        Y_t = torch.tensor(Y, dtype=torch.float32)

        act_map = {
            "relu": nn.ReLU,
            "tanh": nn.Tanh,
            "sigmoid": nn.Sigmoid,
            "leakyrelu": nn.LeakyReLU,
        }
        act_cls = act_map.get(activation.lower(), nn.ReLU)

        layers: list[nn.Module] = []
        input_dim = X_t.shape[1]
        for h in hidden_layers:
            layers += [nn.Linear(input_dim, h), act_cls()]
            if dropout > 0:
                layers.append(nn.Dropout(dropout))
            input_dim = h
        layers.append(nn.Linear(input_dim, 1))
        model = nn.Sequential(*layers)

        optimizer = optim.Adam(model.parameters(), lr=lr)
        criterion = nn.MSELoss()

        model.train()
        for _ in range(epochs):
            optimizer.zero_grad()
            preds = model(X_t)
            loss = criterion(preds, Y_t)
            loss.backward()
            optimizer.step()

        model.eval()
        return cls(model)

    def predict(self, X: np.ndarray) -> np.ndarray:
        X = np.atleast_2d(np.asarray(X, dtype=float))
        if X.ndim == 1:
            X = X.reshape(-1, 1)
        X_t = torch.tensor(X, dtype=torch.float32)
        with torch.no_grad():
            preds = self.model(X_t)
        return preds.cpu().numpy().flatten()
