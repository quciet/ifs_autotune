"""Lightweight surrogate model abstractions used by the ensemble."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import torch
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from sklearn.tree import DecisionTreeRegressor
from torch import nn, optim


def _ensure_2d_inputs(X: np.ndarray) -> np.ndarray:
    array = np.atleast_2d(np.asarray(X, dtype=float))
    if array.ndim == 1:
        return array.reshape(-1, 1)
    return array


@dataclass(frozen=True)
class BoundsScaler:
    """Scale raw inputs into a fixed numeric range using configured bounds."""

    lower: np.ndarray
    upper: np.ndarray
    clip: bool = True

    def transform(self, X: np.ndarray) -> np.ndarray:
        values = _ensure_2d_inputs(X)
        lower = np.asarray(self.lower, dtype=float)
        upper = np.asarray(self.upper, dtype=float)
        span = upper - lower
        scaled = np.zeros_like(values, dtype=float)
        non_constant = ~np.isclose(span, 0.0)
        if np.any(non_constant):
            scaled[:, non_constant] = (
                2.0 * (values[:, non_constant] - lower[non_constant]) / span[non_constant]
            ) - 1.0
        if self.clip:
            np.clip(scaled, -1.0, 1.0, out=scaled)
        return scaled


@dataclass
class LogClippedTargetTransform:
    """Compress large positive losses so failed runs do not dominate training."""

    upper_quantile: float = 95.0
    absolute_cap: float | None = None
    lower_bound: float = 0.0
    upper_clip_: float | None = None

    def fit(self, y: np.ndarray) -> "LogClippedTargetTransform":
        values = np.asarray(y, dtype=float).reshape(-1)
        finite = values[np.isfinite(values)]
        if finite.size == 0:
            self.upper_clip_ = self.absolute_cap if self.absolute_cap is not None else 1.0
            return self

        positive = finite[finite >= self.lower_bound]
        reference = positive if positive.size else finite
        upper = float(np.percentile(reference, self.upper_quantile))
        if self.absolute_cap is not None:
            upper = min(upper, float(self.absolute_cap))
        self.upper_clip_ = max(float(self.lower_bound), upper)
        return self

    def transform(self, y: np.ndarray) -> np.ndarray:
        values = np.asarray(y, dtype=float).reshape(-1)
        upper = self.upper_clip_
        if upper is None:
            upper = float(self.absolute_cap) if self.absolute_cap is not None else float(np.max(values))
        clipped = np.clip(values, self.lower_bound, upper)
        return np.log1p(clipped)

    def inverse(self, y_transformed: np.ndarray) -> np.ndarray:
        values = np.asarray(y_transformed, dtype=float).reshape(-1)
        restored = np.expm1(values)
        if self.upper_clip_ is not None:
            restored = np.clip(restored, self.lower_bound, self.upper_clip_)
        return restored


def _transform_inputs(X: np.ndarray, x_scaler: BoundsScaler | None) -> np.ndarray:
    inputs = _ensure_2d_inputs(X)
    if x_scaler is None:
        return inputs
    return x_scaler.transform(inputs)


def _transform_target(y: np.ndarray, y_transformer: LogClippedTargetTransform | None) -> np.ndarray:
    values = np.asarray(y, dtype=float).reshape(-1)
    if y_transformer is None:
        return values
    return y_transformer.transform(values)


def _inverse_target(y: np.ndarray, y_transformer: LogClippedTargetTransform | None) -> np.ndarray:
    values = np.asarray(y, dtype=float).reshape(-1)
    if y_transformer is None:
        return values
    return y_transformer.inverse(values)


@dataclass
class PolynomialSurrogate:
    """Multivariate polynomial surrogate using sklearn's PolynomialFeatures."""

    model: LinearRegression
    poly: PolynomialFeatures
    x_scaler: BoundsScaler | None = None
    y_transformer: LogClippedTargetTransform | None = None

    def predict(self, X: np.ndarray) -> np.ndarray:
        X_scaled = _transform_inputs(X, self.x_scaler)
        Xp = self.poly.transform(X_scaled)
        predictions = self.model.predict(Xp)
        return _inverse_target(predictions, self.y_transformer)

    @classmethod
    def fit(
        cls,
        X: np.ndarray,
        Y: np.ndarray,
        degree: int = 3,
        x_scaler: BoundsScaler | None = None,
        y_transformer: LogClippedTargetTransform | None = None,
    ) -> "PolynomialSurrogate":
        X_scaled = _transform_inputs(X, x_scaler)
        Y_scaled = _transform_target(Y, y_transformer)
        poly = PolynomialFeatures(degree=degree, include_bias=True)
        Xp = poly.fit_transform(X_scaled)
        reg = LinearRegression().fit(Xp, Y_scaled)
        return cls(model=reg, poly=poly, x_scaler=x_scaler, y_transformer=y_transformer)


class TreeSurrogate:
    """Decision-tree surrogate that supports multi-dimensional X."""

    def __init__(
        self,
        model: DecisionTreeRegressor,
        *,
        x_scaler: BoundsScaler | None = None,
        y_transformer: LogClippedTargetTransform | None = None,
    ):
        self.model = model
        self.x_scaler = x_scaler
        self.y_transformer = y_transformer

    @classmethod
    def fit(
        cls,
        X: np.ndarray,
        Y: np.ndarray,
        max_depth: int = 5,
        random_state: int | None = None,
        x_scaler: BoundsScaler | None = None,
        y_transformer: LogClippedTargetTransform | None = None,
    ) -> "TreeSurrogate":
        X_scaled = _transform_inputs(X, x_scaler)
        Y_scaled = _transform_target(Y, y_transformer)
        reg = DecisionTreeRegressor(max_depth=max_depth, random_state=random_state)
        reg.fit(X_scaled, Y_scaled)
        return cls(reg, x_scaler=x_scaler, y_transformer=y_transformer)

    def predict(self, X: np.ndarray) -> np.ndarray:
        X_scaled = _transform_inputs(X, self.x_scaler)
        predictions = self.model.predict(X_scaled)
        return _inverse_target(predictions, self.y_transformer)


class NNSurrogate:
    """Configurable PyTorch neural-network surrogate supporting multi-dimensional X."""

    def __init__(
        self,
        model: nn.Module,
        *,
        x_scaler: BoundsScaler | None = None,
        y_transformer: LogClippedTargetTransform | None = None,
    ):
        self.model = model
        self.x_scaler = x_scaler
        self.y_transformer = y_transformer

    @classmethod
    def fit(
        cls,
        X: np.ndarray,
        Y: np.ndarray,
        hidden_layers: list[int] | None = None,
        activation: str = "relu",
        dropout: float = 0.0,
        epochs: int = 200,
        lr: float = 1e-3,
        x_scaler: BoundsScaler | None = None,
        y_transformer: LogClippedTargetTransform | None = None,
    ) -> "NNSurrogate":
        if hidden_layers is None:
            hidden_layers = [32, 32]

        X_scaled = _transform_inputs(X, x_scaler)
        Y_scaled = _transform_target(Y, y_transformer).reshape(-1, 1)

        X_t = torch.tensor(X_scaled, dtype=torch.float32)
        Y_t = torch.tensor(Y_scaled, dtype=torch.float32)

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
        return cls(model, x_scaler=x_scaler, y_transformer=y_transformer)

    def predict(self, X: np.ndarray) -> np.ndarray:
        X_scaled = _transform_inputs(X, self.x_scaler)
        X_t = torch.tensor(X_scaled, dtype=torch.float32)
        with torch.no_grad():
            preds = self.model(X_t)
        return _inverse_target(preds.cpu().numpy().flatten(), self.y_transformer)
