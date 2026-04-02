import {
  useEffect,
  useId,
  useMemo,
  useRef,
  useState,
  type ChangeEvent as ReactChangeEvent,
  type FocusEvent as ReactFocusEvent,
  type PointerEvent as ReactPointerEvent,
  type WheelEvent as ReactWheelEvent,
} from "react";
import type { MLProgressTrial } from "./api";

export type ChartPoint = {
  sequenceIndex: number;
  derivedRoundIndex: number | null;
  trialIndex: number;
  batchIndex: number | null;
  fitPooled: number | null;
  fitMissing: boolean;
  bestSoFar: number | null;
  startedAtUtc: string | null;
  completedAtUtc: string | null;
  modelId: string | null;
  modelStatus: string | null;
  progressRowId: number | null;
};

type RollingMetricPoint = ChartPoint & {
  rollingMean: number | null;
  rollingMedian: number | null;
  rollingQ1: number | null;
  rollingQ3: number | null;
  plottedFit: number | null;
  isOutlier: boolean;
};

type Viewport = {
  xMin: number;
  xMax: number;
  yMin: number;
  yMax: number;
};

type RangeInputs = {
  xMin: string;
  xMax: string;
  yMin: string;
  yMax: string;
};

type PlotBounds = {
  left: number;
  top: number;
  width: number;
  height: number;
};

type DerivedMetrics = {
  points: RollingMetricPoint[];
  defaultViewport: Viewport;
  clippedUpper: number;
  clippedMarkerY: number;
  outlierCount: number;
};

type DerivedMetricsCache = {
  derivedMetrics: DerivedMetrics;
  rollingWindow: number;
  sourceLength: number;
  lastProgressRowId: number | null;
  validFitValues: number[];
  sortedFitValues: number[];
};

type DragState = {
  pointerId: number;
  startSvgX: number;
  startSvgY: number;
  startViewport: Viewport;
};

type SelectedPointMeta = {
  id: string;
  point: RollingMetricPoint;
  x: number;
  y: number;
  alignLeft: boolean;
  alignAbove: boolean;
};

type CanvasPointEntry = {
  id: string;
  point: RollingMetricPoint;
  x: number;
  y: number;
  isOutlier: boolean;
  isBestPoint: boolean;
  isLatestPoint: boolean;
};

const CHART_WIDTH = 860;
const CHART_HEIGHT = 360;
const PADDING = { top: 28, right: 24, bottom: 48, left: 64 };
const ZOOM_IN_FACTOR = 0.85;
const ZOOM_OUT_FACTOR = 1 / ZOOM_IN_FACTOR;
const MIN_POSITIVE_SPAN = 1e-6;
const MANUAL_MIN_X_SPAN = 1;
const MANUAL_MIN_Y_SPAN = 1e-4;

function formatViewportInput(value: number, digits: number): string {
  if (!Number.isFinite(value)) {
    return "";
  }

  const rounded = Math.round(value);
  if (Math.abs(value - rounded) < 1e-6) {
    return rounded.toString();
  }

  return value.toFixed(digits).replace(/\.?0+$/, "");
}

function buildRangeInputs(viewport: Viewport): RangeInputs {
  return {
    xMin: formatViewportInput(Math.round(viewport.xMin), 0),
    xMax: formatViewportInput(Math.round(viewport.xMax), 0),
    yMin: formatViewportInput(viewport.yMin, 4),
    yMax: formatViewportInput(viewport.yMax, 4),
  };
}

function parseRangeInput(value: string): number | null {
  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }

  const parsed = Number(trimmed);
  return Number.isFinite(parsed) ? parsed : null;
}

function roundToDecimals(value: number, digits: number): number {
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

function normalizeRangeNumber(field: keyof RangeInputs, value: number): number {
  if (field === "xMin" || field === "xMax") {
    return Math.round(value);
  }

  return roundToDecimals(value, 4);
}

function normalizeRangeDisplay(field: keyof RangeInputs, value: number): string {
  if (field === "xMin" || field === "xMax") {
    return formatViewportInput(Math.round(value), 0);
  }

  return formatViewportInput(roundToDecimals(value, 4), 4);
}

function areRangeInputsEqual(left: RangeInputs, right: RangeInputs): boolean {
  return (
    left.xMin === right.xMin &&
    left.xMax === right.xMax &&
    left.yMin === right.yMin &&
    left.yMax === right.yMax
  );
}

function parseProgressTimestamp(value: string | null | undefined): number | null {
  if (!value) {
    return null;
  }

  const parsed = new Date(value);
  const timestamp = parsed.getTime();
  return Number.isNaN(timestamp) ? null : timestamp;
}

export function normalizeProgressTrials(trials: MLProgressTrial[]): ChartPoint[] {
  const sorted = [...trials]
    .filter(
      (trial): trial is MLProgressTrial & { trial_index: number } =>
        typeof trial.trial_index === "number" &&
        Number.isFinite(trial.trial_index) &&
        trial.trial_index >= 0,
    )
    .sort((left, right) => {
      const leftSequence =
        typeof left.sequence_index === "number" &&
        Number.isFinite(left.sequence_index) &&
        left.sequence_index > 0
          ? left.sequence_index
          : null;
      const rightSequence =
        typeof right.sequence_index === "number" &&
        Number.isFinite(right.sequence_index) &&
        right.sequence_index > 0
          ? right.sequence_index
          : null;

      if (leftSequence != null && rightSequence != null && leftSequence !== rightSequence) {
        return leftSequence - rightSequence;
      }

      const leftTimestamp =
        parseProgressTimestamp(left.started_at_utc) ??
        parseProgressTimestamp(left.completed_at_utc);
      const rightTimestamp =
        parseProgressTimestamp(right.started_at_utc) ??
        parseProgressTimestamp(right.completed_at_utc);

      if (leftTimestamp != null && rightTimestamp != null && leftTimestamp !== rightTimestamp) {
        return leftTimestamp - rightTimestamp;
      }

      if (leftTimestamp == null && rightTimestamp != null) {
        return 1;
      }

      if (leftTimestamp != null && rightTimestamp == null) {
        return -1;
      }

      if (left.trial_index !== right.trial_index) {
        return left.trial_index - right.trial_index;
      }

      return (left.model_id ?? "").localeCompare(right.model_id ?? "");
    });

  let bestSoFar: number | null = null;

  return sorted.map((trial, index) => {
    const fitPooled =
      typeof trial.fit_pooled === "number" && Number.isFinite(trial.fit_pooled)
        ? trial.fit_pooled
        : null;
    const fitMissing = Boolean(trial.fit_missing) || fitPooled == null;

    if (fitPooled != null) {
      bestSoFar = bestSoFar == null ? fitPooled : Math.min(bestSoFar, fitPooled);
    }

    return {
      sequenceIndex:
        typeof trial.sequence_index === "number" &&
        Number.isFinite(trial.sequence_index) &&
        trial.sequence_index > 0
          ? trial.sequence_index
          : index + 1,
      derivedRoundIndex:
        typeof trial.derived_round_index === "number" &&
        Number.isFinite(trial.derived_round_index) &&
        trial.derived_round_index > 0
          ? trial.derived_round_index
          : null,
      trialIndex: trial.trial_index,
      batchIndex:
        typeof trial.batch_index === "number" && Number.isFinite(trial.batch_index)
          ? trial.batch_index
          : null,
      fitPooled,
      fitMissing,
      bestSoFar,
      startedAtUtc: trial.started_at_utc ?? null,
      completedAtUtc: trial.completed_at_utc ?? null,
      modelId: trial.model_id ?? null,
      modelStatus: trial.model_status ?? null,
      progressRowId:
        typeof trial.progress_rowid === "number" &&
        Number.isFinite(trial.progress_rowid) &&
        trial.progress_rowid > 0
          ? trial.progress_rowid
          : null,
    };
  });
}

export function appendNormalizedProgressTrials(
  existing: ChartPoint[],
  trials: MLProgressTrial[],
): ChartPoint[] {
  if (trials.length === 0) {
    return existing;
  }

  let bestSoFar = existing.length > 0 ? existing[existing.length - 1].bestSoFar : null;
  const appended = trials.map((trial, index) => {
    const fitPooled =
      typeof trial.fit_pooled === "number" && Number.isFinite(trial.fit_pooled)
        ? trial.fit_pooled
        : null;
    const fitMissing = Boolean(trial.fit_missing) || fitPooled == null;

    if (fitPooled != null) {
      bestSoFar = bestSoFar == null ? fitPooled : Math.min(bestSoFar, fitPooled);
    }

    return {
      sequenceIndex:
        typeof trial.sequence_index === "number" &&
        Number.isFinite(trial.sequence_index) &&
        trial.sequence_index > 0
          ? trial.sequence_index
          : existing.length + index + 1,
      derivedRoundIndex:
        typeof trial.derived_round_index === "number" &&
        Number.isFinite(trial.derived_round_index) &&
        trial.derived_round_index > 0
          ? trial.derived_round_index
          : null,
      trialIndex:
        typeof trial.trial_index === "number" && Number.isFinite(trial.trial_index)
          ? trial.trial_index
          : existing.length + index,
      batchIndex:
        typeof trial.batch_index === "number" && Number.isFinite(trial.batch_index)
          ? trial.batch_index
          : null,
      fitPooled,
      fitMissing,
      bestSoFar,
      startedAtUtc: trial.started_at_utc ?? null,
      completedAtUtc: trial.completed_at_utc ?? null,
      modelId: trial.model_id ?? null,
      modelStatus: trial.model_status ?? null,
      progressRowId:
        typeof trial.progress_rowid === "number" &&
        Number.isFinite(trial.progress_rowid) &&
        trial.progress_rowid > 0
          ? trial.progress_rowid
          : null,
    };
  });

  return [...existing, ...appended];
}

function percentile(values: number[], quantile: number): number {
  if (values.length === 0) {
    return Number.NaN;
  }

  const sorted = [...values].sort((left, right) => left - right);
  return percentileSorted(sorted, quantile);
}

function percentileSorted(sorted: number[], quantile: number): number {
  if (sorted.length === 0) {
    return Number.NaN;
  }

  const clampedQuantile = Math.min(1, Math.max(0, quantile));
  const index = (sorted.length - 1) * clampedQuantile;
  const lowerIndex = Math.floor(index);
  const upperIndex = Math.ceil(index);

  if (lowerIndex === upperIndex) {
    return sorted[lowerIndex];
  }

  const weight = index - lowerIndex;
  return sorted[lowerIndex] * (1 - weight) + sorted[upperIndex] * weight;
}

function insertSortedValue(values: number[], value: number): void {
  let low = 0;
  let high = values.length;

  while (low < high) {
    const mid = Math.floor((low + high) / 2);
    if (values[mid] <= value) {
      low = mid + 1;
    } else {
      high = mid;
    }
  }

  values.splice(low, 0, value);
}

function rollingWindowValues(
  values: number[],
  window: number,
  calculate: (windowValues: number[]) => number,
): Array<number | null> {
  const result = Array<number | null>(values.length).fill(null);
  for (let index = window - 1; index < values.length; index += 1) {
    result[index] = calculate(values.slice(index - window + 1, index + 1));
  }
  return result;
}

function computeDisplayStats(
  points: ChartPoint[],
  sortedFitValues: number[],
): {
  clippedUpper: number;
  clippedMarkerY: number;
  defaultViewport: Viewport;
  outlierCount: number;
} {
  if (sortedFitValues.length === 0) {
    throw new Error("No successful completed trials to plot yet.");
  }

  const lowerBound = sortedFitValues[0];
  const actualMax = sortedFitValues[sortedFitValues.length - 1];
  const q1 = percentileSorted(sortedFitValues, 0.25);
  const q3 = percentileSorted(sortedFitValues, 0.75);
  const iqr =
    Number.isFinite(q1) && Number.isFinite(q3) ? Math.max(0, q3 - q1) : 0;
  let robustUpper =
    Number.isFinite(q3) && Number.isFinite(iqr) ? q3 + 1.5 * iqr : actualMax;

  if (iqr <= 0 || !Number.isFinite(robustUpper)) {
    robustUpper = actualMax;
  }

  let clippedUpper = Math.min(actualMax, robustUpper);
  if (clippedUpper <= lowerBound) {
    clippedUpper = actualMax;
  }

  const outlierCount = sortedFitValues.filter((value) => value > clippedUpper).length;
  const displaySpan = Math.max(clippedUpper - lowerBound, 1e-6);
  const bottomPadding = Math.max(displaySpan * 0.2, Math.abs(clippedUpper) * 0.04, 0.02);
  const topPadding = Math.max(displaySpan * 0.06, clippedUpper * 0.02, 0.002);
  const clippedMarkerY = clippedUpper + topPadding * 0.45;
  const xMin = Math.min(...points.map((point) => point.sequenceIndex));
  const xMax = Math.max(...points.map((point) => point.sequenceIndex));
  const xRangeActual = Math.max(1, xMax - xMin);
  const xRightPadding = Math.max(8, xRangeActual * 0.03);
  const zeroMargin = Math.max(bottomPadding * 0.6, displaySpan * 0.08, 0.01);
  const yMinCandidate = lowerBound - bottomPadding;
  const yMaxDisplay = clippedUpper + topPadding;
  const yMinDisplay =
    lowerBound >= 0 && yMinCandidate > -zeroMargin ? -zeroMargin : yMinCandidate;

  return {
    clippedUpper,
    clippedMarkerY,
    outlierCount,
    defaultViewport: {
      xMin,
      xMax: xMax + xRightPadding,
      yMin: yMinDisplay,
      yMax: yMaxDisplay,
    },
  };
}

function finalizeMetricPoints(
  metricPoints: RollingMetricPoint[],
  clippedUpper: number,
): RollingMetricPoint[] {
  return metricPoints.map((point) => ({
    ...point,
    plottedFit:
      point.fitPooled == null ? null : Math.min(point.fitPooled, clippedUpper),
    isOutlier: point.fitPooled != null && point.fitPooled > clippedUpper,
  }));
}

function buildDerivedMetricsFromScratch(
  points: ChartPoint[],
  rollingWindow: number,
): DerivedMetricsCache {
  const validEntries = points.flatMap((point, index) =>
    typeof point.fitPooled === "number" && Number.isFinite(point.fitPooled)
      ? [{ index, value: point.fitPooled }]
      : [],
  );
  const fitValues = validEntries.map((entry) => entry.value);

  if (fitValues.length === 0) {
    throw new Error("No successful completed trials to plot yet.");
  }

  const displayStats = computeDisplayStats(points, [...fitValues].sort((left, right) => left - right));
  const rollingMean = Array<number | null>(points.length).fill(null);
  const rollingMedian = Array<number | null>(points.length).fill(null);
  const rollingQ1 = Array<number | null>(points.length).fill(null);
  const rollingQ3 = Array<number | null>(points.length).fill(null);

  if (fitValues.length >= rollingWindow) {
    const meanValues = rollingWindowValues(
      fitValues,
      rollingWindow,
      (windowValues) =>
        windowValues.reduce((sum, value) => sum + value, 0) / windowValues.length,
    );
    const medianValues = rollingWindowValues(fitValues, rollingWindow, (windowValues) =>
      percentile(windowValues, 0.5),
    );
    const q1Values = rollingWindowValues(fitValues, rollingWindow, (windowValues) =>
      percentile(windowValues, 0.25),
    );
    const q3Values = rollingWindowValues(fitValues, rollingWindow, (windowValues) =>
      percentile(windowValues, 0.75),
    );

    validEntries.forEach((entry, validIndex) => {
      rollingMean[entry.index] = meanValues[validIndex];
      rollingMedian[entry.index] = medianValues[validIndex];
      rollingQ1[entry.index] = q1Values[validIndex];
      rollingQ3[entry.index] = q3Values[validIndex];
    });
  }

  const metricPoints = points.map((point, index) => ({
    ...point,
    rollingMean: rollingMean[index],
    rollingMedian: rollingMedian[index],
    rollingQ1: rollingQ1[index],
    rollingQ3: rollingQ3[index],
    plottedFit: null,
    isOutlier: false,
  }));

  const finalizedPoints = finalizeMetricPoints(metricPoints, displayStats.clippedUpper);

  return {
    derivedMetrics: {
      points: finalizedPoints,
      defaultViewport: displayStats.defaultViewport,
      clippedUpper: displayStats.clippedUpper,
      clippedMarkerY: displayStats.clippedMarkerY,
      outlierCount: displayStats.outlierCount,
    },
    rollingWindow,
    sourceLength: points.length,
    lastProgressRowId: points[points.length - 1]?.progressRowId ?? null,
    validFitValues: fitValues,
    sortedFitValues: [...fitValues].sort((left, right) => left - right),
  };
}

function buildDerivedMetrics(
  points: ChartPoint[],
  rollingWindow: number,
  previousCache: DerivedMetricsCache | null,
): DerivedMetricsCache {
  if (
    previousCache == null ||
    previousCache.rollingWindow !== rollingWindow ||
    points.length < previousCache.sourceLength
  ) {
    return buildDerivedMetricsFromScratch(points, rollingWindow);
  }

  if (points.length === previousCache.sourceLength) {
    return buildDerivedMetricsFromScratch(points, rollingWindow);
  }

  if (
    previousCache.sourceLength > 0 &&
    points[previousCache.sourceLength - 1]?.progressRowId !== previousCache.lastProgressRowId
  ) {
    return buildDerivedMetricsFromScratch(points, rollingWindow);
  }

  const metricPoints = previousCache.derivedMetrics.points.map((point) => ({ ...point }));
  const validFitValues = previousCache.validFitValues.slice();
  const sortedFitValues = previousCache.sortedFitValues.slice();

  for (let index = previousCache.sourceLength; index < points.length; index += 1) {
    const point = points[index];
    let rollingMean: number | null = null;
    let rollingMedian: number | null = null;
    let rollingQ1: number | null = null;
    let rollingQ3: number | null = null;

    if (typeof point.fitPooled === "number" && Number.isFinite(point.fitPooled)) {
      validFitValues.push(point.fitPooled);
      insertSortedValue(sortedFitValues, point.fitPooled);

      if (validFitValues.length >= rollingWindow) {
        const windowValues = validFitValues.slice(validFitValues.length - rollingWindow);
        rollingMean =
          windowValues.reduce((sum, value) => sum + value, 0) / windowValues.length;
        rollingMedian = percentile(windowValues, 0.5);
        rollingQ1 = percentile(windowValues, 0.25);
        rollingQ3 = percentile(windowValues, 0.75);
      }
    }

    metricPoints.push({
      ...point,
      rollingMean,
      rollingMedian,
      rollingQ1,
      rollingQ3,
      plottedFit: null,
      isOutlier: false,
    });
  }

  const displayStats = computeDisplayStats(points, sortedFitValues);
  const finalizedPoints = finalizeMetricPoints(metricPoints, displayStats.clippedUpper);

  return {
    derivedMetrics: {
      points: finalizedPoints,
      defaultViewport: displayStats.defaultViewport,
      clippedUpper: displayStats.clippedUpper,
      clippedMarkerY: displayStats.clippedMarkerY,
      outlierCount: displayStats.outlierCount,
    },
    rollingWindow,
    sourceLength: points.length,
    lastProgressRowId: points[points.length - 1]?.progressRowId ?? null,
    validFitValues,
    sortedFitValues,
  };
}

function formatUtcTimestamp(value: string | null): string {
  if (!value) {
    return "In progress";
  }

  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? value : parsed.toLocaleString();
}

function formatFitValue(value: number | null, fitMissing = false): string {
  if (fitMissing || value == null) {
    return "Missing";
  }

  return value.toFixed(4);
}

function viewportEquals(left: Viewport | null, right: Viewport | null, epsilon = 1e-6): boolean {
  if (left === right) {
    return true;
  }

  if (left == null || right == null) {
    return false;
  }

  return (
    Math.abs(left.xMin - right.xMin) < epsilon &&
    Math.abs(left.xMax - right.xMax) < epsilon &&
    Math.abs(left.yMin - right.yMin) < epsilon &&
    Math.abs(left.yMax - right.yMax) < epsilon
  );
}

function clampViewport(
  viewport: Viewport,
  bounds: Viewport,
  minXSpan: number,
  minYSpan: number,
): Viewport {
  const actualBoundXSpan = Math.max(bounds.xMax - bounds.xMin, MIN_POSITIVE_SPAN);
  const actualBoundYSpan = Math.max(bounds.yMax - bounds.yMin, MIN_POSITIVE_SPAN);
  const effectiveMinXSpan = Math.min(Math.max(minXSpan, MIN_POSITIVE_SPAN), actualBoundXSpan);
  const effectiveMinYSpan = Math.min(Math.max(minYSpan, MIN_POSITIVE_SPAN), actualBoundYSpan);

  let xSpan = Math.max(
    effectiveMinXSpan,
    Math.min(actualBoundXSpan, viewport.xMax - viewport.xMin),
  );
  let ySpan = Math.max(
    effectiveMinYSpan,
    Math.min(actualBoundYSpan, viewport.yMax - viewport.yMin),
  );

  let xMin = viewport.xMin;
  let yMin = viewport.yMin;

  if (xMin < bounds.xMin) {
    xMin = bounds.xMin;
  }
  if (xMin + xSpan > bounds.xMax) {
    xMin = bounds.xMax - xSpan;
  }

  if (yMin < bounds.yMin) {
    yMin = bounds.yMin;
  }
  if (yMin + ySpan > bounds.yMax) {
    yMin = bounds.yMax - ySpan;
  }

  if (xSpan >= actualBoundXSpan) {
    xMin = bounds.xMin;
    xSpan = actualBoundXSpan;
  }

  if (ySpan >= actualBoundYSpan) {
    yMin = bounds.yMin;
    ySpan = actualBoundYSpan;
  }

  return {
    xMin,
    xMax: xMin + xSpan,
    yMin,
    yMax: yMin + ySpan,
  };
}

function zoomViewport(
  viewport: Viewport,
  bounds: Viewport,
  scaleFactor: number,
  anchorRatioX: number,
  anchorRatioY: number,
  minXSpan: number,
  minYSpan: number,
): Viewport {
  const currentXSpan = viewport.xMax - viewport.xMin;
  const currentYSpan = viewport.yMax - viewport.yMin;
  const nextXSpan = currentXSpan * scaleFactor;
  const nextYSpan = currentYSpan * scaleFactor;

  const anchorX = viewport.xMin + currentXSpan * anchorRatioX;
  const anchorY = viewport.yMin + currentYSpan * (1 - anchorRatioY);

  const nextViewport = {
    xMin: anchorX - nextXSpan * anchorRatioX,
    xMax: anchorX + nextXSpan * (1 - anchorRatioX),
    yMin: anchorY - nextYSpan * (1 - anchorRatioY),
    yMax: anchorY + nextYSpan * anchorRatioY,
  };

  return clampViewport(nextViewport, bounds, minXSpan, minYSpan);
}

function buildLinePath(
  points: RollingMetricPoint[],
  xFor: (value: number) => number,
  yFor: (value: number) => number,
  valueAccessor: (point: RollingMetricPoint) => number | null,
): string {
  let path = "";

  points.forEach((point) => {
    const value = valueAccessor(point);
    if (value == null || !Number.isFinite(value)) {
      return;
    }

    const command = path.length === 0 ? "M" : "L";
    path += `${command}${xFor(point.sequenceIndex).toFixed(2)},${yFor(value).toFixed(2)} `;
  });

  return path.trim();
}

function buildBandPath(
  points: RollingMetricPoint[],
  xFor: (value: number) => number,
  yFor: (value: number) => number,
): string {
  const topPoints = points.flatMap((point) =>
    point.rollingQ3 != null
      ? [[xFor(point.sequenceIndex), yFor(point.rollingQ3)] as const]
      : [],
  );
  const bottomPoints = points
    .flatMap((point) =>
      point.rollingQ1 != null
        ? [[xFor(point.sequenceIndex), yFor(point.rollingQ1)] as const]
        : [],
    )
    .reverse();

  if (topPoints.length < 2 || bottomPoints.length < 2) {
    return "";
  }

  const topPath = topPoints
    .map(([x, y], index) => `${index === 0 ? "M" : "L"}${x.toFixed(2)},${y.toFixed(2)}`)
    .join(" ");
  const bottomPath = bottomPoints
    .map(([x, y]) => `L${x.toFixed(2)},${y.toFixed(2)}`)
    .join(" ");

  return `${topPath} ${bottomPath} Z`;
}

function formatTick(value: number, isIntegerPreferred: boolean): string {
  if (isIntegerPreferred) {
    return Math.round(value).toString();
  }

  if (Math.abs(value) >= 100) {
    return value.toFixed(1);
  }

  if (Math.abs(value) >= 10) {
    return value.toFixed(2);
  }

  return value.toFixed(3);
}

function buildTicks(min: number, max: number, count: number, integerPreferred = false): number[] {
  if (count <= 1 || max <= min) {
    return [min];
  }

  const ticks = Array.from({ length: count }, (_, index) => min + ((max - min) * index) / (count - 1));
  if (!integerPreferred) {
    return ticks;
  }

  const rounded = Array.from(new Set(ticks.map((tick) => Math.round(tick))));
  return rounded.length >= 2 ? rounded : [Math.round(min), Math.round(max)];
}

function isBestSoFarPoint(point: RollingMetricPoint, previousPoint: RollingMetricPoint | null): boolean {
  if (point.fitPooled == null || point.bestSoFar == null) {
    return false;
  }

  const epsilon = 1e-9;
  if (Math.abs(point.fitPooled - point.bestSoFar) > epsilon) {
    return false;
  }

  const previousBest = previousPoint?.bestSoFar ?? null;
  return previousBest == null || point.bestSoFar < previousBest - epsilon;
}

function getSvgCoordinates(
  svg: SVGSVGElement,
  clientX: number,
  clientY: number,
): { x: number; y: number } {
  const rect = svg.getBoundingClientRect();
  const scaleX = rect.width > 0 ? CHART_WIDTH / rect.width : 1;
  const scaleY = rect.height > 0 ? CHART_HEIGHT / rect.height : 1;

  return {
    x: (clientX - rect.left) * scaleX,
    y: (clientY - rect.top) * scaleY,
  };
}

function isInsidePlotArea(position: { x: number; y: number }, plotBounds: PlotBounds): boolean {
  return (
    position.x >= plotBounds.left &&
    position.x <= plotBounds.left + plotBounds.width &&
    position.y >= plotBounds.top &&
    position.y <= plotBounds.top + plotBounds.height
  );
}

export function MLProgressChart({
  points,
  referenceFitPooled,
  referenceModelId,
  rollingWindow,
  rollingWindowInput,
  onRollingWindowInputChange,
}: {
  points: ChartPoint[];
  referenceFitPooled: number | null;
  referenceModelId: string | null;
  rollingWindow: number;
  rollingWindowInput: string;
  onRollingWindowInputChange: (value: string) => void;
}) {
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const svgRef = useRef<SVGSVGElement | null>(null);
  const dragRef = useRef<DragState | null>(null);
  const dragMovedRef = useRef(false);
  const derivedMetricsCacheRef = useRef<DerivedMetricsCache | null>(null);
  const [viewport, setViewport] = useState<Viewport | null>(null);
  const [manualViewport, setManualViewport] = useState<Viewport | null>(null);
  const [rangeInputs, setRangeInputs] = useState<RangeInputs>({
    xMin: "",
    xMax: "",
    yMin: "",
    yMax: "",
  });
  const [isDragging, setIsDragging] = useState(false);
  const [selectedPointId, setSelectedPointId] = useState<string | null>(null);
  const clipPathId = useId();
  const referenceFitValue =
    typeof referenceFitPooled === "number" && Number.isFinite(referenceFitPooled)
      ? referenceFitPooled
      : null;

  const derivedMetrics = useMemo(() => {
    if (points.length === 0) {
      derivedMetricsCacheRef.current = null;
      return null;
    }

    try {
      const nextCache = buildDerivedMetrics(
        points,
        rollingWindow,
        derivedMetricsCacheRef.current,
      );
      derivedMetricsCacheRef.current = nextCache;
      return nextCache.derivedMetrics;
    } catch {
      derivedMetricsCacheRef.current = null;
      return null;
    }
  }, [points, rollingWindow]);

  const plotBounds = useMemo<PlotBounds>(
    () => ({
      left: PADDING.left,
      top: PADDING.top,
      width: CHART_WIDTH - PADDING.left - PADDING.right,
      height: CHART_HEIGHT - PADDING.top - PADDING.bottom,
    }),
    [],
  );

  const minXSpan = useMemo(() => {
    if (!derivedMetrics) {
      return 1;
    }

    const defaultSpan = derivedMetrics.defaultViewport.xMax - derivedMetrics.defaultViewport.xMin;
    return Math.max(1, Math.min(8, defaultSpan * 0.02));
  }, [derivedMetrics]);

  const minYSpan = useMemo(() => {
    if (!derivedMetrics) {
      return 1e-4;
    }

    const defaultSpan = derivedMetrics.defaultViewport.yMax - derivedMetrics.defaultViewport.yMin;
    return Math.max(1e-4, defaultSpan * 0.05);
  }, [derivedMetrics]);

  const manualMinXSpan = useMemo(
    () =>
      derivedMetrics == null
        ? MANUAL_MIN_X_SPAN
        : Math.min(
            MANUAL_MIN_X_SPAN,
            Math.max(derivedMetrics.defaultViewport.xMax - derivedMetrics.defaultViewport.xMin, MIN_POSITIVE_SPAN),
          ),
    [derivedMetrics],
  );

  const manualMinYSpan = useMemo(
    () =>
      derivedMetrics == null
        ? MANUAL_MIN_Y_SPAN
        : Math.min(
            MANUAL_MIN_Y_SPAN,
            Math.max(derivedMetrics.defaultViewport.yMax - derivedMetrics.defaultViewport.yMin, MIN_POSITIVE_SPAN),
          ),
    [derivedMetrics],
  );

  const effectiveManualViewport = useMemo(() => {
    if (!derivedMetrics || manualViewport == null) {
      return null;
    }

    const clamped = clampViewport(
      manualViewport,
      derivedMetrics.defaultViewport,
      manualMinXSpan,
      manualMinYSpan,
    );

    return viewportEquals(clamped, derivedMetrics.defaultViewport) ? null : clamped;
  }, [derivedMetrics, manualMinXSpan, manualMinYSpan, manualViewport]);

  const activeBounds = derivedMetrics?.defaultViewport
    ? effectiveManualViewport ?? derivedMetrics.defaultViewport
    : null;

  const activeViewport =
    activeBounds == null
      ? null
      : viewport == null
        ? activeBounds
        : clampViewport(viewport, activeBounds, minXSpan, minYSpan);

  useEffect(() => {
    if (
      (manualViewport == null && effectiveManualViewport == null) ||
      viewportEquals(manualViewport, effectiveManualViewport)
    ) {
      return;
    }

    setManualViewport(effectiveManualViewport);
  }, [effectiveManualViewport, manualViewport]);

  useEffect(() => {
    if (activeBounds == null || viewport == null) {
      return;
    }

    const clamped = clampViewport(viewport, activeBounds, minXSpan, minYSpan);
    const normalized = viewportEquals(clamped, activeBounds) ? null : clamped;

    if (!viewportEquals(normalized, viewport)) {
      setViewport(normalized);
    }
  }, [activeBounds, minXSpan, minYSpan, viewport]);

  useEffect(() => {
    if (activeViewport == null) {
      return;
    }

    const nextInputs = buildRangeInputs(activeViewport);
    setRangeInputs((current) => (areRangeInputsEqual(current, nextInputs) ? current : nextInputs));
  }, [activeViewport?.xMin, activeViewport?.xMax, activeViewport?.yMin, activeViewport?.yMax]);

  if (!derivedMetrics || derivedMetrics.points.length === 0) {
    return <div className="progress-text">No successful completed trials to plot yet.</div>;
  }

  const visibleViewport = activeViewport ?? activeBounds ?? derivedMetrics.defaultViewport;
  const isZoomed = !viewportEquals(visibleViewport, derivedMetrics.defaultViewport);
  const canPan = activeBounds != null && !viewportEquals(visibleViewport, activeBounds);
  const xSpan = Math.max(visibleViewport.xMax - visibleViewport.xMin, 1e-6);
  const ySpan = Math.max(visibleViewport.yMax - visibleViewport.yMin, 1e-6);
  const xFor = (value: number) =>
    plotBounds.left + ((value - visibleViewport.xMin) / xSpan) * plotBounds.width;
  const yFor = (value: number) =>
    plotBounds.top + plotBounds.height - ((value - visibleViewport.yMin) / ySpan) * plotBounds.height;
  const yTicks = buildTicks(visibleViewport.yMin, visibleViewport.yMax, 5);
  const xTicks = buildTicks(visibleViewport.xMin, visibleViewport.xMax, 6, true);

  const meanPath = buildLinePath(derivedMetrics.points, xFor, yFor, (point) => point.rollingMean);
  const medianPath = buildLinePath(derivedMetrics.points, xFor, yFor, (point) => point.rollingMedian);
  const bandPath = buildBandPath(derivedMetrics.points, xFor, yFor);
  const latestPlottedPoint = [...derivedMetrics.points]
    .reverse()
    .find((point) => point.plottedFit != null) ?? null;
  const latestPointId =
    latestPlottedPoint == null
      ? null
      : `${latestPlottedPoint.sequenceIndex}-${latestPlottedPoint.modelId ?? "unknown"}`;
  const referenceFitVisible =
    referenceFitValue != null &&
    referenceFitValue >= visibleViewport.yMin &&
    referenceFitValue <= visibleViewport.yMax;
  const canvasPointEntries = useMemo<CanvasPointEntry[]>(() => {
    return derivedMetrics.points.flatMap((point, pointIndex) => {
      if (point.plottedFit == null) {
        return [];
      }

      const pointId = `${point.sequenceIndex}-${point.modelId ?? "unknown"}`;
      const isBestPoint = isBestSoFarPoint(
        point,
        pointIndex > 0 ? derivedMetrics.points[pointIndex - 1] : null,
      );
      const isLatestPoint = pointId === latestPointId;
      const x = xFor(point.sequenceIndex);
      const y = yFor(point.isOutlier ? derivedMetrics.clippedMarkerY : point.plottedFit);

      return [
        {
          id: pointId,
          point,
          x,
          y,
          isOutlier: point.isOutlier,
          isBestPoint,
          isLatestPoint,
        },
      ];
    });
  }, [derivedMetrics.points, derivedMetrics.clippedMarkerY, latestPointId, xFor, yFor]);

  const selectedPoint = useMemo<SelectedPointMeta | null>(() => {
    if (!selectedPointId) {
      return null;
    }

    const match = canvasPointEntries.find(
      (entry) => entry.id === selectedPointId && !entry.isOutlier,
    );

    if (!match) {
      return null;
    }

    return {
      id: match.id,
      point: match.point,
      x: match.x,
      y: match.y,
      alignLeft: match.x > CHART_WIDTH * 0.68,
      alignAbove: match.y > CHART_HEIGHT * 0.42,
    };
  }, [canvasPointEntries, selectedPointId]);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) {
      return;
    }

    const context = canvas.getContext("2d");
    if (!context) {
      return;
    }

    const dpr = window.devicePixelRatio || 1;
    canvas.width = Math.round(CHART_WIDTH * dpr);
    canvas.height = Math.round(CHART_HEIGHT * dpr);
    context.setTransform(dpr, 0, 0, dpr, 0, 0);
    context.clearRect(0, 0, CHART_WIDTH, CHART_HEIGHT);

    context.save();
    context.beginPath();
    context.rect(plotBounds.left, plotBounds.top, plotBounds.width, plotBounds.height);
    context.clip();

    const drawCircle = (
      x: number,
      y: number,
      radius: number,
      fill: string,
      stroke: string,
      lineWidth: number,
    ) => {
      context.beginPath();
      context.arc(x, y, radius, 0, Math.PI * 2);
      context.fillStyle = fill;
      context.fill();
      if (lineWidth > 0) {
        context.lineWidth = lineWidth;
        context.strokeStyle = stroke;
        context.stroke();
      }
    };

    const drawTriangle = (
      x: number,
      y: number,
      fill: string,
      stroke: string,
      lineWidth: number,
    ) => {
      context.beginPath();
      context.moveTo(x, y - 4);
      context.lineTo(x - 3.6, y + 2);
      context.lineTo(x + 3.6, y + 2);
      context.closePath();
      context.fillStyle = fill;
      context.fill();
      if (lineWidth > 0) {
        context.lineWidth = lineWidth;
        context.strokeStyle = stroke;
        context.stroke();
      }
    };

    canvasPointEntries.forEach((entry) => {
      if (entry.isOutlier || entry.isBestPoint || entry.isLatestPoint) {
        return;
      }

      drawCircle(entry.x, entry.y, 2, "#205493", "rgba(255,255,255,0.85)", 0.5);
    });

    canvasPointEntries.forEach((entry) => {
      if (entry.isOutlier || !entry.isBestPoint || entry.isLatestPoint) {
        return;
      }

      drawCircle(entry.x, entry.y, 4, "#c62828", "#ffffff", 1.15);
    });

    canvasPointEntries.forEach((entry) => {
      if (!entry.isLatestPoint || entry.isOutlier) {
        return;
      }

      drawCircle(entry.x, entry.y, 5, "#7c3aed", "#ffffff", 1.35);
    });

    canvasPointEntries.forEach((entry) => {
      if (!entry.isOutlier) {
        return;
      }

      if (entry.isLatestPoint) {
        drawCircle(entry.x, entry.y, 5, "#7c3aed", "#ffffff", 1.35);
      }

      drawTriangle(entry.x, entry.y, "#8c2d04", "#ffffff", 0.75);
    });

    context.restore();
  }, [canvasPointEntries, plotBounds]);

  useEffect(() => {
    if (!selectedPointId) {
      return;
    }

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setSelectedPointId(null);
      }
    };

    window.addEventListener("keydown", handleKeyDown);
    return () => {
      window.removeEventListener("keydown", handleKeyDown);
    };
  }, [selectedPointId]);

  const updateViewport = (
    producer: (current: Viewport, bounds: Viewport) => Viewport,
  ) => {
    if (activeBounds == null) {
      return;
    }

    setViewport((current) => {
      const base = current == null ? activeBounds : current;
      const next = producer(base, activeBounds);
      return viewportEquals(next, activeBounds) ? null : next;
    });
  };

  const parsedXMin = parseRangeInput(rangeInputs.xMin);
  const parsedXMax = parseRangeInput(rangeInputs.xMax);
  const parsedYMin = parseRangeInput(rangeInputs.yMin);
  const parsedYMax = parseRangeInput(rangeInputs.yMax);
  const trimmedRollingWindowInput = rollingWindowInput.trim();
  const parsedRollingWindow =
    /^\d+$/.test(trimmedRollingWindowInput) && trimmedRollingWindowInput.length > 0
      ? Number(trimmedRollingWindowInput)
      : null;
  const hasInvalidRollingWindow =
    trimmedRollingWindowInput.length > 0 &&
    (parsedRollingWindow == null || !Number.isFinite(parsedRollingWindow) || parsedRollingWindow <= 0);
  const hasInvalidXRange =
    (rangeInputs.xMin.trim() !== "" && parsedXMin == null) ||
    (rangeInputs.xMax.trim() !== "" && parsedXMax == null) ||
    (parsedXMin != null && parsedXMax != null && parsedXMax - parsedXMin < manualMinXSpan);
  const hasInvalidYRange =
    (rangeInputs.yMin.trim() !== "" && parsedYMin == null) ||
    (rangeInputs.yMax.trim() !== "" && parsedYMax == null) ||
    (parsedYMin != null && parsedYMax != null && parsedYMax - parsedYMin < manualMinYSpan);

  const applyManualRange = (nextInputs: RangeInputs) => {
    const nextXMinRaw = parseRangeInput(nextInputs.xMin);
    const nextXMaxRaw = parseRangeInput(nextInputs.xMax);
    const nextYMinRaw = parseRangeInput(nextInputs.yMin);
    const nextYMaxRaw = parseRangeInput(nextInputs.yMax);

    const nextXMin =
      nextXMinRaw == null ? null : normalizeRangeNumber("xMin", nextXMinRaw);
    const nextXMax =
      nextXMaxRaw == null ? null : normalizeRangeNumber("xMax", nextXMaxRaw);
    const nextYMin =
      nextYMinRaw == null ? null : normalizeRangeNumber("yMin", nextYMinRaw);
    const nextYMax =
      nextYMaxRaw == null ? null : normalizeRangeNumber("yMax", nextYMaxRaw);

    if (
      nextXMin == null ||
      nextXMax == null ||
      nextYMin == null ||
      nextYMax == null ||
      nextXMax - nextXMin < manualMinXSpan ||
      nextYMax - nextYMin < manualMinYSpan
    ) {
      return;
    }

    const clampedViewport = clampViewport(
      {
        xMin: nextXMin,
        xMax: nextXMax,
        yMin: nextYMin,
        yMax: nextYMax,
      },
      derivedMetrics.defaultViewport,
      manualMinXSpan,
      manualMinYSpan,
    );

    dragRef.current = null;
    setIsDragging(false);
    setSelectedPointId(null);
    setViewport(null);
    setManualViewport(
      viewportEquals(clampedViewport, derivedMetrics.defaultViewport) ? null : clampedViewport,
    );
  };

  const handleRangeInputChange =
    (field: keyof RangeInputs) => (event: ReactChangeEvent<HTMLInputElement>) => {
      const nextInputs = { ...rangeInputs, [field]: event.target.value };
      setRangeInputs(nextInputs);
      applyManualRange(nextInputs);
    };

  const handleRangeInputBlur =
    (field: keyof RangeInputs) => (event: ReactFocusEvent<HTMLInputElement>) => {
      const parsed = parseRangeInput(event.target.value);
      if (parsed == null) {
        return;
      }

      const normalizedValue = normalizeRangeDisplay(field, parsed);
      const nextInputs = { ...rangeInputs, [field]: normalizedValue };
      setRangeInputs(nextInputs);
      applyManualRange(nextInputs);
    };

  const handleZoomButton = (scaleFactor: number) => {
    updateViewport((current, bounds) =>
      zoomViewport(current, bounds, scaleFactor, 0.5, 0.5, minXSpan, minYSpan),
    );
  };

  const handleReset = () => {
    dragRef.current = null;
    dragMovedRef.current = false;
    setIsDragging(false);
    setSelectedPointId(null);
    setViewport(null);
    setManualViewport(null);
  };

  const handleWheel = (event: ReactWheelEvent<SVGSVGElement>) => {
    if (!svgRef.current) {
      return;
    }

    if (!event.ctrlKey) {
      return;
    }

    const position = getSvgCoordinates(svgRef.current, event.clientX, event.clientY);
    if (!isInsidePlotArea(position, plotBounds)) {
      return;
    }

    event.preventDefault();

    const anchorRatioX = (position.x - plotBounds.left) / plotBounds.width;
    const anchorRatioY = (position.y - plotBounds.top) / plotBounds.height;
    const scaleFactor = event.deltaY < 0 ? ZOOM_IN_FACTOR : ZOOM_OUT_FACTOR;

    updateViewport((current, bounds) =>
      zoomViewport(
        current,
        bounds,
        scaleFactor,
        anchorRatioX,
        anchorRatioY,
        minXSpan,
        minYSpan,
      ),
    );
  };

  const handlePointerDown = (event: ReactPointerEvent<SVGSVGElement>) => {
    if (!svgRef.current) {
      return;
    }

    const position = getSvgCoordinates(svgRef.current, event.clientX, event.clientY);
    if (!isInsidePlotArea(position, plotBounds)) {
      return;
    }

    if (selectedPointId != null) {
      setSelectedPointId(null);
    }

    if (!canPan) {
      return;
    }

    dragRef.current = {
      pointerId: event.pointerId,
      startSvgX: position.x,
      startSvgY: position.y,
      startViewport: visibleViewport,
    };
    dragMovedRef.current = false;
    setIsDragging(true);
    event.currentTarget.setPointerCapture(event.pointerId);
    event.preventDefault();
  };

  const handlePointerMove = (event: ReactPointerEvent<SVGSVGElement>) => {
    if (!dragRef.current || dragRef.current.pointerId !== event.pointerId || !canPan) {
      return;
    }

    if (!svgRef.current) {
      return;
    }

    const position = getSvgCoordinates(svgRef.current, event.clientX, event.clientY);
    const deltaSvgX = position.x - dragRef.current.startSvgX;
    const deltaSvgY = position.y - dragRef.current.startSvgY;
    if (Math.abs(deltaSvgX) > 3 || Math.abs(deltaSvgY) > 3) {
      dragMovedRef.current = true;
    }
    const startViewport = dragRef.current.startViewport;
    const startXSpan = startViewport.xMax - startViewport.xMin;
    const startYSpan = startViewport.yMax - startViewport.yMin;

    const nextViewport = clampViewport(
      {
        xMin: startViewport.xMin - (deltaSvgX / plotBounds.width) * startXSpan,
        xMax: startViewport.xMax - (deltaSvgX / plotBounds.width) * startXSpan,
        yMin: startViewport.yMin + (deltaSvgY / plotBounds.height) * startYSpan,
        yMax: startViewport.yMax + (deltaSvgY / plotBounds.height) * startYSpan,
      },
      activeBounds,
      minXSpan,
      minYSpan,
    );

    setViewport(viewportEquals(nextViewport, activeBounds) ? null : nextViewport);
    event.preventDefault();
  };

  const endDrag = (event: ReactPointerEvent<SVGSVGElement>) => {
    if (dragRef.current && dragRef.current.pointerId === event.pointerId) {
      dragRef.current = null;
      setIsDragging(false);
      if (event.currentTarget.hasPointerCapture(event.pointerId)) {
        event.currentTarget.releasePointerCapture(event.pointerId);
      }
    }
  };

  const handleDoubleClick = (event: ReactPointerEvent<SVGSVGElement>) => {
    if (!svgRef.current || dragMovedRef.current) {
      dragMovedRef.current = false;
      return;
    }

    const position = getSvgCoordinates(svgRef.current, event.clientX, event.clientY);
    if (!isInsidePlotArea(position, plotBounds)) {
      return;
    }

    const inspectRadius = 12;
    let nearestPoint: CanvasPointEntry | null = null;
    let nearestDistance = Number.POSITIVE_INFINITY;

    for (const entry of canvasPointEntries) {
      if (entry.isOutlier) {
        continue;
      }

      const dx = entry.x - position.x;
      const dy = entry.y - position.y;
      const distance = Math.sqrt(dx * dx + dy * dy);
      if (distance <= inspectRadius && distance < nearestDistance) {
        nearestPoint = entry;
        nearestDistance = distance;
      }
    }

    if (nearestPoint) {
      setSelectedPointId(nearestPoint.id);
    } else {
      setSelectedPointId(null);
    }

    dragMovedRef.current = false;
  };

  return (
    <div className="ml-progress-chart-shell">
      <div className="chart-toolbar">
        <div className="chart-notes">
          <div className="chart-note">{`Live trend view with ${rollingWindow} runs as rolling window`}</div>
          {derivedMetrics.outlierCount > 0 ? (
            <div className="chart-note chart-note-secondary">
              {`Outliers above ${derivedMetrics.clippedUpper.toFixed(4)} are clipped for readability.`}
            </div>
          ) : null}
          <div className="chart-note chart-note-secondary">
            Pinch or hold Ctrl while scrolling to zoom the chart.
          </div>
          {referenceFitValue != null && !referenceFitVisible ? (
            <div className="chart-note chart-note-secondary">
              IFs default baseline: {referenceFitValue.toFixed(4)} (outside the current view)
            </div>
          ) : null}
        </div>
        <div className="chart-toolbar-actions">
          <label className="chart-rolling-control">
            <span className="chart-rolling-label">Rolling window</span>
            <input
              type="text"
              inputMode="numeric"
              className={`chart-rolling-input ${hasInvalidRollingWindow ? "is-invalid" : ""}`}
              value={rollingWindowInput}
              onChange={(event) => onRollingWindowInputChange(event.target.value)}
              aria-label="Rolling window size"
              aria-invalid={hasInvalidRollingWindow}
            />
          </label>
        </div>
      </div>

      <div className="chart-canvas-wrapper">
        <canvas
          ref={canvasRef}
          className="chart-points-canvas"
          width={CHART_WIDTH}
          height={CHART_HEIGHT}
          aria-hidden="true"
        />
        <svg
          ref={svgRef}
          className={`ml-progress-chart ${isZoomed ? "is-zoomable" : ""} ${isDragging ? "is-dragging" : ""}`}
          viewBox={`0 0 ${CHART_WIDTH} ${CHART_HEIGHT}`}
          role="img"
          aria-label="Live ML trend chart"
          onWheel={handleWheel}
          onPointerDown={handlePointerDown}
          onPointerMove={handlePointerMove}
          onPointerUp={endDrag}
          onPointerCancel={endDrag}
          onDoubleClick={handleDoubleClick}
        >
          <defs>
            <clipPath id={clipPathId}>
              <rect
                x={plotBounds.left}
                y={plotBounds.top}
                width={plotBounds.width}
                height={plotBounds.height}
              />
            </clipPath>
          </defs>

          <rect
            x={plotBounds.left}
            y={plotBounds.top}
            width={plotBounds.width}
            height={plotBounds.height}
            className="chart-plot-bg"
          />
          <rect
            x={plotBounds.left}
            y={plotBounds.top}
            width={plotBounds.width}
            height={plotBounds.height}
            className="chart-plot-border"
          />

          {yTicks.map((tick) => {
            const y = yFor(tick);
            return (
              <g key={`y-${tick}`}>
                <line
                  x1={plotBounds.left}
                  x2={plotBounds.left + plotBounds.width}
                  y1={y}
                  y2={y}
                  className="chart-grid-line"
                />
                <text
                  x={plotBounds.left - 10}
                  y={y + 4}
                  textAnchor="end"
                  className="chart-axis-label"
                >
                  {formatTick(tick, false)}
                </text>
              </g>
            );
          })}

          {xTicks.map((tick) => {
            const x = xFor(tick);
            return (
              <g key={`x-${tick}`}>
                <line
                  x1={x}
                  x2={x}
                  y1={plotBounds.top}
                  y2={plotBounds.top + plotBounds.height}
                  className="chart-grid-line chart-grid-line-vertical"
                />
                <text
                  x={x}
                  y={plotBounds.top + plotBounds.height + 18}
                  textAnchor="middle"
                  className="chart-axis-label"
                >
                  {formatTick(tick, true)}
                </text>
              </g>
            );
          })}

          <g clipPath={`url(#${clipPathId})`}>
            {bandPath ? <path d={bandPath} className="chart-iqr-band" /> : null}

            {referenceFitVisible && referenceFitValue != null ? (
              <line
                x1={plotBounds.left}
                x2={plotBounds.left + plotBounds.width}
                y1={yFor(referenceFitValue)}
                y2={yFor(referenceFitValue)}
                className="chart-reference-line"
                vectorEffect="non-scaling-stroke"
              />
            ) : null}

            {meanPath ? (
              <path
                d={meanPath}
                className="chart-mean-line"
                vectorEffect="non-scaling-stroke"
              />
            ) : null}
            {medianPath ? (
              <path
                d={medianPath}
                className="chart-median-line"
                vectorEffect="non-scaling-stroke"
              />
            ) : null}
          </g>
        </svg>

        {selectedPoint ? (
          <div
            className={`chart-tooltip ${selectedPoint.alignLeft ? "align-left" : "align-right"} ${selectedPoint.alignAbove ? "align-above" : "align-below"}`}
            style={{
              left: `${(selectedPoint.x / CHART_WIDTH) * 100}%`,
              top: `${(selectedPoint.y / CHART_HEIGHT) * 100}%`,
            }}
          >
            <button
              type="button"
              className="chart-tooltip-close"
              onClick={() => setSelectedPointId(null)}
              aria-label="Close point details"
            >
              ×
            </button>
            <div className="chart-tooltip-title">
              {`Sequence ${selectedPoint.point.sequenceIndex}`}
            </div>
            <div>{`Round: ${selectedPoint.point.derivedRoundIndex ?? "N/A"}`}</div>
            <div>{`Trial: ${selectedPoint.point.trialIndex}`}</div>
            <div>{`Fit: ${formatFitValue(selectedPoint.point.fitPooled, selectedPoint.point.fitMissing)}`}</div>
            <div>{`Best: ${formatFitValue(selectedPoint.point.bestSoFar)}`}</div>
            <div>{`Status: ${selectedPoint.point.modelStatus ?? "unknown"}`}</div>
            <div>{`Batch: ${selectedPoint.point.batchIndex ?? "N/A"}`}</div>
            <div>{formatUtcTimestamp(selectedPoint.point.completedAtUtc ?? selectedPoint.point.startedAtUtc)}</div>
          </div>
        ) : null}
      </div>

      <div className="chart-controls chart-controls-bottom" aria-label="Chart range and zoom controls">
        <div className="chart-range-controls">
          <label className="chart-range-field">
            <span className="chart-range-label">X min</span>
            <input
              type="text"
              inputMode="numeric"
              className={`chart-range-input ${hasInvalidXRange ? "is-invalid" : ""}`}
              value={rangeInputs.xMin}
              onChange={handleRangeInputChange("xMin")}
              onBlur={handleRangeInputBlur("xMin")}
              aria-label="Minimum X value"
              aria-invalid={hasInvalidXRange}
            />
          </label>
          <label className="chart-range-field">
            <span className="chart-range-label">X max</span>
            <input
              type="text"
              inputMode="numeric"
              className={`chart-range-input ${hasInvalidXRange ? "is-invalid" : ""}`}
              value={rangeInputs.xMax}
              onChange={handleRangeInputChange("xMax")}
              onBlur={handleRangeInputBlur("xMax")}
              aria-label="Maximum X value"
              aria-invalid={hasInvalidXRange}
            />
          </label>
          <label className="chart-range-field">
            <span className="chart-range-label">Y min</span>
            <input
              type="text"
              inputMode="decimal"
              className={`chart-range-input ${hasInvalidYRange ? "is-invalid" : ""}`}
              value={rangeInputs.yMin}
              onChange={handleRangeInputChange("yMin")}
              onBlur={handleRangeInputBlur("yMin")}
              aria-label="Minimum Y value"
              aria-invalid={hasInvalidYRange}
            />
          </label>
          <label className="chart-range-field">
            <span className="chart-range-label">Y max</span>
            <input
              type="text"
              inputMode="decimal"
              className={`chart-range-input ${hasInvalidYRange ? "is-invalid" : ""}`}
              value={rangeInputs.yMax}
              onChange={handleRangeInputChange("yMax")}
              onBlur={handleRangeInputBlur("yMax")}
              aria-label="Maximum Y value"
              aria-invalid={hasInvalidYRange}
            />
          </label>
        </div>
        <button type="button" className="chart-control-button" onClick={() => handleZoomButton(ZOOM_IN_FACTOR)}>
          Zoom in
        </button>
        <button type="button" className="chart-control-button" onClick={() => handleZoomButton(ZOOM_OUT_FACTOR)}>
          Zoom out
        </button>
        <button
          type="button"
          className="chart-control-button secondary"
          onClick={handleReset}
          disabled={!isZoomed}
        >
          Reset
        </button>
      </div>

      <div className="chart-legend">
        <span className="legend-item">
          <span className="legend-swatch fit" /> Raw fit
        </span>
        <span className="legend-item">
          <span className="legend-swatch best-point" /> Best so far
        </span>
        <span className="legend-item">
          <span className="legend-swatch latest" /> Latest result
        </span>
        <span className="legend-item">
          <span className="legend-swatch median" /> Median
        </span>
        <span className="legend-item">
          <span className="legend-swatch mean" /> Mean
        </span>
        <span className="legend-item">
          <span className="legend-swatch band" /> IQR
        </span>
        {referenceFitValue != null ? (
          <span className="legend-item">
            <span className="legend-swatch reference" /> IFs default
          </span>
        ) : null}
        {derivedMetrics.outlierCount > 0 ? (
          <span className="legend-item">
            <span className="legend-swatch outlier" /> Clipped outliers
          </span>
        ) : null}
      </div>
    </div>
  );
}
