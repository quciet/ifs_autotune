import {
  ChangeEvent,
  FormEvent,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import {
  getMLProgressHistory,
  modelSetup,
  subscribeToIFsProgress,
  validateIFsFolder,
  StageError,
  type ApiStage,
  type ApiStatus,
  type CheckResponse,
  type IFsProgressEvent,
  type ModelSetupData,
  type MLDriverData,
  type MLProgressTrial,
} from "./api";

const REQUIRED_INPUT_SHEETS = ["AnalFunc", "TablFunc", "IFsVar", "DataDict"];

type View = "validate" | "tune";

type StatusLevel = "info" | "success" | "error";

type LowerPanelView = "log" | "progress";

type MLTerminationReason = "completed" | "stopped_gracefully";

type MLFinalResult = {
  best_model_id?: string | null;
  best_fit_pooled?: number | null;
  iterations?: number | null;
};

type MLJobStatus = {
  running: boolean;
  startedAt: number | null;
  pid: number | null;
  progress: { done?: number; total?: number; text?: string } | null;
  lastUpdateAt: number | null;
  exitCode: number | null;
  error: string | null;
  ifsPath: string | null;
  ifsValidated: boolean;
  inputExcelPath: string | null;
  outputDir: string | null;
  stopRequested: boolean;
  stopAcknowledged: boolean;
  finalResult: MLFinalResult | null;
  terminationReason: MLTerminationReason | null;
  runConfig: {
    endYear?: number | string | null;
    baseYear?: number | null;
    initialModelId?: string | number | null;
    datasetId?: string | null;
  } | null;
};

type LogStatus = ApiStatus | "info";

type LogEntry = {
  id: number;
  stage: ApiStage;
  message: string;
  status: LogStatus;
};

type TuneIFsPageProps = {
  onBack: () => void;
  onMLJobStatusRefresh?: () => Promise<void>;
  validatedPath: string;
  validatedInputPath: string;
  baseYear?: number | null;
  outputDirectory: string | null;
  initialMLJobRunning?: boolean;
  initialMLJobProgress?: string | null;
  initialRunConfig?: MLJobStatus["runConfig"];
  initialStopRequested?: boolean;
  initialStopAcknowledged?: boolean;
  initialFinalResult?: MLFinalResult | null;
  initialTerminationReason?: MLTerminationReason | null;
};

type ChartPoint = {
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
};

function parseProgressTimestamp(value: string | null | undefined): number | null {
  if (!value) {
    return null;
  }

  const parsed = new Date(value);
  const timestamp = parsed.getTime();
  return Number.isNaN(timestamp) ? null : timestamp;
}

function normalizeProgressTrials(trials: MLProgressTrial[]): ChartPoint[] {
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
    };
  });
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

function percentile(values: number[], quantile: number): number {
  if (values.length === 0) {
    return Number.NaN;
  }

  const sorted = [...values].sort((left, right) => left - right);
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

function MLProgressChart({
  points,
  referenceFitPooled,
  referenceModelId,
}: {
  points: ChartPoint[];
  referenceFitPooled: number | null;
  referenceModelId: string | null;
}) {
  const fitValues = points.flatMap((point) =>
    typeof point.fitPooled === "number" && Number.isFinite(point.fitPooled)
      ? [point.fitPooled]
      : [],
  );

  if (points.length === 0 || fitValues.length === 0) {
    return <div className="progress-text">No successful completed trials to plot yet.</div>;
  }

  const width = 760;
  const height = 280;
  const padding = { top: 24, right: 20, bottom: 40, left: 56 };
  const plotWidth = width - padding.left - padding.right;
  const plotHeight = height - padding.top - padding.bottom;
  const xMin = Math.min(...points.map((point) => point.sequenceIndex));
  const xMax = Math.max(...points.map((point) => point.sequenceIndex));
  const xRangeActual = Math.max(1, xMax - xMin);
  const xRightPadding = Math.max(8, xRangeActual * 0.03);
  const xDisplayMax = xMax + xRightPadding;
  const yMin = Math.min(...fitValues);
  const q1 = percentile(fitValues, 0.25);
  const q3 = percentile(fitValues, 0.75);
  const iqr =
    Number.isFinite(q1) && Number.isFinite(q3) ? Math.max(0, q3 - q1) : 0;
  const yCapBaseCandidate =
    Number.isFinite(q3) && Number.isFinite(iqr)
      ? q3 + 1.5 * iqr
      : Number.NaN;
  const yCapBase =
    Number.isFinite(yCapBaseCandidate) && yCapBaseCandidate >= yMin
      ? yCapBaseCandidate
      : Math.max(...fitValues);
  const displaySpanBase = Math.max(yCapBase - yMin, Math.abs(yCapBase), 1e-9);
  const yBottomPadding = displaySpanBase * 0.1;
  const yHeadroom = displaySpanBase * 0.05;
  const yMinDisplay = yMin - yBottomPadding;
  const yMaxDisplay = yCapBase + yHeadroom;
  const hasOutliers = fitValues.some((value) => value > yCapBase);
  const referenceFitValue =
    typeof referenceFitPooled === "number" && Number.isFinite(referenceFitPooled)
      ? referenceFitPooled
      : null;
  const bestPoint = points.reduce<ChartPoint | null>((best, point) => {
    if (point.fitPooled == null) {
      return best;
    }
    if (best == null || best.fitPooled == null) {
      return point;
    }
    if (point.fitPooled < best.fitPooled) {
      return point;
    }
    if (point.fitPooled === best.fitPooled && point.sequenceIndex > best.sequenceIndex) {
      return point;
    }
    return best;
  }, null);
  const xRange = Math.max(1, xDisplayMax - xMin);
  const yRange = Math.max(1e-9, yMaxDisplay - yMinDisplay);
  const referenceFitInRange =
    referenceFitValue != null &&
    referenceFitValue >= yMinDisplay &&
    referenceFitValue <= yMaxDisplay;

  const xFor = (sequenceIndex: number) =>
    padding.left + ((sequenceIndex - xMin) / xRange) * plotWidth;
  const yFor = (value: number) =>
    padding.top + plotHeight - ((value - yMinDisplay) / yRange) * plotHeight;

  const yTicks = Array.from(
    { length: 5 },
    (_, index) => yMinDisplay + ((yMaxDisplay - yMinDisplay) * index) / 4,
  );
  const xTicks = points.length <= 6
    ? points.map((point) => point.sequenceIndex)
    : Array.from({ length: 6 }, (_, index) => Math.round(xMin + (xRangeActual * index) / 5));

  return (
    <div className="ml-progress-chart-shell">
      <div className="chart-note">
        Showing an adaptive IQR-capped y-axis for readability
        {hasOutliers ? ` at ${yCapBase.toFixed(4)}` : ""}.
      </div>
      {referenceFitValue != null && !referenceFitInRange ? (
        <div className="chart-note">
          IFs default baseline: {referenceFitValue.toFixed(4)} (outside displayed range)
        </div>
      ) : null}
      <svg
        className="ml-progress-chart"
        viewBox={`0 0 ${width} ${height}`}
        role="img"
        aria-label="ML convergence chart"
      >
        <rect x={padding.left} y={padding.top} width={plotWidth} height={plotHeight} className="chart-plot-bg" />
        {yTicks.map((tick) => {
          const y = yFor(tick);
          return (
            <g key={`y-${tick}`}>
              <line x1={padding.left} x2={width - padding.right} y1={y} y2={y} className="chart-grid-line" />
              <text x={padding.left - 8} y={y + 4} textAnchor="end" className="chart-axis-label">
                {tick.toFixed(3)}
              </text>
            </g>
          );
        })}
        {xTicks.map((tick) => {
          const x = xFor(tick);
          return (
            <g key={`x-${tick}`}>
              <line x1={x} x2={x} y1={padding.top} y2={height - padding.bottom} className="chart-grid-line chart-grid-line-vertical" />
              <text x={x} y={height - padding.bottom + 18} textAnchor="middle" className="chart-axis-label">
                {tick}
              </text>
            </g>
          );
        })}
        {referenceFitInRange ? (
          <g>
            <line
              x1={padding.left}
              x2={width - padding.right}
              y1={yFor(referenceFitValue!)}
              y2={yFor(referenceFitValue!)}
              className="chart-reference-line"
            />
            <title>
              {`IFs default baseline\nModel ${referenceModelId ?? "unknown"}\nFit ${referenceFitValue!.toFixed(4)}`}
            </title>
          </g>
        ) : null}
        {points.map((point) => {
          if (point.fitPooled == null) {
            return null;
          }

          const x = xFor(point.sequenceIndex);
          const isOutlier = point.fitPooled > yCapBase;
          const y = yFor(isOutlier ? yCapBase : point.fitPooled);
          const eventTimestamp = point.completedAtUtc ?? point.startedAtUtc;
          return (
            <g key={`point-${point.sequenceIndex}-${point.modelId ?? "unknown"}`}>
              {isOutlier ? (
                <polygon
                  points={`${x},${y - 3.5} ${x - 2.75},${y + 1.5} ${x + 2.75},${y + 1.5}`}
                  className="chart-outlier"
                />
              ) : (
                <circle cx={x} cy={y} r={1.8} className="chart-point" />
              )}
              <title>
                {`Sequence ${point.sequenceIndex}\nRound ${point.derivedRoundIndex ?? "N/A"}\nTrial ${point.trialIndex}\nFit ${formatFitValue(point.fitPooled, point.fitMissing)}${isOutlier ? " (above displayed range)" : ""}\nBest ${formatFitValue(point.bestSoFar)}\nStatus ${point.modelStatus ?? "unknown"}\nBatch ${point.batchIndex ?? "N/A"}\n${formatUtcTimestamp(eventTimestamp)}`}
              </title>
            </g>
          );
        })}
        {bestPoint && bestPoint.fitPooled != null ? (
          <g key={`best-point-${bestPoint.sequenceIndex}`}>
            <circle
              cx={xFor(bestPoint.sequenceIndex)}
              cy={yFor(Math.min(bestPoint.fitPooled, yCapBase))}
              r={2.2}
              className="chart-point chart-point-best"
            />
            <title>
              {`Best model\nSequence ${bestPoint.sequenceIndex}\nRound ${bestPoint.derivedRoundIndex ?? "N/A"}\nTrial ${bestPoint.trialIndex}\nFit ${formatFitValue(bestPoint.fitPooled, bestPoint.fitMissing)}\nStatus ${bestPoint.modelStatus ?? "unknown"}\nBatch ${bestPoint.batchIndex ?? "N/A"}\n${formatUtcTimestamp(bestPoint.completedAtUtc ?? bestPoint.startedAtUtc)}`}
            </title>
          </g>
        ) : null}
        <text x={width / 2} y={height - 6} textAnchor="middle" className="chart-title">
          Dataset Run Sequence
        </text>
        <text
          x={18}
          y={height / 2}
          textAnchor="middle"
          className="chart-title"
          transform={`rotate(-90 18 ${height / 2})`}
        >
          Final Fit Metric (Adaptive Capped View)
        </text>
      </svg>
      <div className="chart-legend">
        <span className="legend-item"><span className="legend-swatch fit" /> Trial fit</span>
        <span className="legend-item"><span className="legend-swatch best" /> Best model</span>
        {referenceFitInRange ? (
          <span className="legend-item"><span className="legend-swatch reference" /> IFs default</span>
        ) : null}
        {hasOutliers ? (
          <span className="legend-item"><span className="legend-swatch outlier" /> Above displayed range</span>
        ) : null}
      </div>
    </div>
  );
}

function calculateProgressPercentage(
  currentYear: number,
  baseYear: number | null | undefined,
  endYear: number | null | undefined,
): number | null {
  if (!Number.isFinite(currentYear)) {
    return null;
  }

  if (typeof baseYear !== "number" || !Number.isFinite(baseYear)) {
    return null;
  }

  if (typeof endYear !== "number" || !Number.isFinite(endYear)) {
    return null;
  }

  if (endYear === baseYear) {
    return currentYear >= endYear ? 100 : 0;
  }

  const percentage = ((currentYear - baseYear) / (endYear - baseYear)) * 100;
  const clamped = Math.min(100, Math.max(0, percentage));
  return clamped;
}

function TuneIFsPage({
  onBack,
  onMLJobStatusRefresh,
  validatedPath,
  validatedInputPath,
  baseYear,
  outputDirectory,
  initialMLJobRunning,
  initialMLJobProgress,
  initialRunConfig,
  initialStopRequested,
  initialStopAcknowledged,
  initialFinalResult,
  initialTerminationReason,
}: TuneIFsPageProps) {
  const DEFAULT_END_YEAR = 2050;
  const MAX_END_YEAR = 2150;
  const FALLBACK_MIN_END_YEAR = 1900;

  const initialEndYear = Number(initialRunConfig?.endYear);
  const normalizedInitialEndYear =
    Number.isFinite(initialEndYear) && initialEndYear > 0
      ? initialEndYear
      : DEFAULT_END_YEAR;
  const initialRunResult = useMemo<MLDriverData | null>(() => {
    if (!initialFinalResult) {
      return null;
    }

    return {
      ...initialFinalResult,
      terminationReason: initialTerminationReason ?? null,
      base_year:
        typeof baseYear === "number" && Number.isFinite(baseYear) ? baseYear : null,
      end_year: normalizedInitialEndYear,
    };
  }, [
    baseYear,
    initialFinalResult,
    initialTerminationReason,
    normalizedInitialEndYear,
  ]);
  const [endYearInput, setEndYearInput] = useState(String(normalizedInitialEndYear));
  const [endYear, setEndYear] = useState<number>(normalizedInitialEndYear);
  const [running, setRunning] = useState(Boolean(initialMLJobRunning));
  const [stopRequested, setStopRequested] = useState(Boolean(initialStopRequested));
  const [stopAcknowledged, setStopAcknowledged] = useState(
    Boolean(initialStopAcknowledged),
  );
  const [modelSetupRunning, setModelSetupRunning] = useState(false);
  const [modelSetupResult, setModelSetupResult] =
    useState<ModelSetupData | null>(null);
  const [progressDatasetId, setProgressDatasetId] = useState<string | null>(
    typeof initialRunConfig?.datasetId === "string" ? initialRunConfig.datasetId : null,
  );
  const [progressReferenceModelId, setProgressReferenceModelId] = useState<string | null>(
    typeof initialRunConfig?.initialModelId === "string"
      ? initialRunConfig.initialModelId
      : null,
  );
  const [progressReferenceFitPooled, setProgressReferenceFitPooled] =
    useState<number | null>(null);
  const [progressYear, setProgressYear] = useState<number | null>(null);
  const [progressPercent, setProgressPercent] = useState(0);
  const [runResult, setRunResult] = useState<MLDriverData | null>(initialRunResult);
  const [error, setError] = useState<string | null>(null);
  const [statusMessage, setStatusMessage] = useState("Waiting to start.");
  const [statusLevel, setStatusLevel] = useState<StatusLevel>("info");
  const [logEntries, setLogEntries] = useState<LogEntry[]>([]);
  const [mlLogEntries, setMLLogEntries] = useState<string[]>([]);
  const ML_LOG_MAX_LINES = 300;
  const AUTO_SCROLL_BOTTOM_EPS_PX = 24;
  const [currentModelProgress, setCurrentModelProgress] = useState<string | null>(null);
  const [lowerPanelView, setLowerPanelView] = useState<LowerPanelView>("log");
  const [progressTrials, setProgressTrials] = useState<ChartPoint[]>([]);
  const [progressHistoryLoading, setProgressHistoryLoading] = useState(false);
  const [progressHistoryError, setProgressHistoryError] = useState<string | null>(null);
  const [effectiveBaseYear, setEffectiveBaseYear] = useState<number | null>(
    typeof baseYear === "number" && Number.isFinite(baseYear) ? baseYear : null,
  );
  const baseYearRef = useRef<number | null>(baseYear ?? null);
  const targetEndYearRef = useRef<number | null>(normalizedInitialEndYear);
  const parameterRef = useRef<Record<string, unknown>>({});
  const coefficientRef = useRef<Record<string, unknown>>({});
  const paramDimensionRef = useRef<Record<string, unknown>>({});
  const logIdRef = useRef(0);
  const mlConsoleBodyRef = useRef<HTMLDivElement | null>(null);
  const mlAutoScrollRef = useRef(true);

  const refreshMLJobStatus = async () => {
    if (!onMLJobStatusRefresh) {
      return;
    }

    try {
      await onMLJobStatusRefresh();
    } catch (error) {
      console.warn("Unable to refresh ML job status:", error);
    }
  };

  const appendLog = (stage: ApiStage, status: LogStatus, message: string) => {
    const normalized = typeof message === "string" ? message.trim() : "";
    const content = normalized.length > 0 ? normalized : message;
    setLogEntries((entries) => {
      const nextId = logIdRef.current + 1;
      logIdRef.current = nextId;
      return [...entries, { id: nextId, stage, status, message: content }];
    });
  };

  const resolveSuccessMessage = (
    stage: ApiStage,
    message: unknown,
  ): string => {
    const trimmed = typeof message === "string" ? message.trim() : "";
    if (trimmed.length > 0) {
      return trimmed;
    }

    switch (stage) {
      case "model_setup":
        return "Model setup completed successfully.";
      case "run_ifs":
        return "ML Optimization run completed successfully.";
      case "ml_driver":
        return "ML optimization completed successfully.";
      default:
        return "Operation completed successfully.";
    }
  };

  const updateStageStatus = (
    stage: ApiStage,
    level: StatusLevel,
    message: string,
    shouldLog = true,
  ) => {
    const normalized = typeof message === "string" ? message.trim() : "";
    const content = normalized.length > 0 ? normalized : message;
    setStatusMessage(`[${stage}] ${content}`);
    setStatusLevel(level);
    if (shouldLog) {
      const logStatus: LogStatus =
        level === "info" ? "info" : (level as LogStatus);
      appendLog(stage, logStatus, content);
    }
  };

  const resolveStageError = (
    err: unknown,
    fallbackStage: ApiStage,
  ): { stage: ApiStage; message: string } => {
    if (err instanceof StageError) {
      return { stage: err.stage, message: err.message };
    }

    return {
      stage: fallbackStage,
      message:
        err instanceof Error ? err.message : "An unexpected error occurred.",
    };
  };

  const minEndYear =
    typeof effectiveBaseYear === "number" && Number.isFinite(effectiveBaseYear)
      ? Math.min(effectiveBaseYear, MAX_END_YEAR)
      : FALLBACK_MIN_END_YEAR;

  const clampEndYear = (value: number) =>
    Math.min(MAX_END_YEAR, Math.max(minEndYear, value));

  useEffect(() => {
    const normalized =
      typeof baseYear === "number" && Number.isFinite(baseYear) ? baseYear : null;
    baseYearRef.current = normalized;
    setEffectiveBaseYear(normalized);
  }, [baseYear]);

  useEffect(() => {
    if (!initialMLJobRunning) {
      return;
    }

    setRunning(true);
    setStatusMessage("Re-attached to running ML Optimization job.");
    setStatusLevel("info");
    if (initialMLJobProgress) {
      setCurrentModelProgress(initialMLJobProgress);
    }
  }, [initialMLJobRunning, initialMLJobProgress]);

  useEffect(() => {
    setStopRequested(Boolean(initialStopRequested));
    setStopAcknowledged(Boolean(initialStopAcknowledged));
  }, [initialStopAcknowledged, initialStopRequested]);

  useEffect(() => {
    if (!running && initialRunResult) {
      const message =
        initialTerminationReason === "stopped_gracefully"
          ? "ML optimization stopped after the current run."
          : "ML optimization completed successfully.";
      setRunResult(initialRunResult);
      setStatusMessage(`[ml_driver] ${message}`);
      setStatusLevel("success");
    }
  }, [initialRunResult, initialTerminationReason, running]);

  useEffect(() => {
    if (!initialMLJobRunning && !initialRunResult) {
      setStatusMessage("Waiting to start.");
      setStatusLevel("info");
    }
    setModelSetupResult(null);
    setProgressDatasetId(
      typeof initialRunConfig?.datasetId === "string"
        ? initialRunConfig.datasetId
        : null,
    );
    setProgressReferenceModelId(
      typeof initialRunConfig?.initialModelId === "string"
        ? initialRunConfig.initialModelId
        : null,
    );
    setProgressReferenceFitPooled(null);
    setRunResult(initialRunResult);
    setLogEntries([]);
    logIdRef.current = 0;
    setProgressYear(null);
    setProgressPercent(0);
    setError(null);
    setLowerPanelView("log");
  }, [
    validatedPath,
    validatedInputPath,
    outputDirectory,
    initialRunConfig?.datasetId,
    initialRunConfig?.initialModelId,
  ]);

  useEffect(() => {
    setEndYear((current) => {
      const fallback = Number.isFinite(current) ? (current as number) : DEFAULT_END_YEAR;
      const next = clampEndYear(fallback);
      if (next !== current) {
        setEndYearInput(String(next));
      }
      targetEndYearRef.current = next;
      return next;
    });
  }, [minEndYear]);

  useEffect(() => {
    const unsubscribe = subscribeToIFsProgress((event: IFsProgressEvent) => {
      setProgressYear(event.year);

      setProgressPercent((previous) => {
        const directPercent =
          typeof event.percent === "number" && Number.isFinite(event.percent)
            ? event.percent
            : null;

        const computedPercent =
          directPercent ??
          calculateProgressPercentage(
            event.year,
            baseYearRef.current,
            targetEndYearRef.current,
          );

        if (computedPercent == null) {
          return previous;
        }

        const clamped = Math.min(100, Math.max(0, computedPercent));
        return Math.max(previous, clamped);
      });
    });

    return () => {
      unsubscribe();
    };
  }, []);

  useEffect(() => {
    if (runResult && typeof runResult.base_year === "number") {
      baseYearRef.current = runResult.base_year;
      setEffectiveBaseYear(runResult.base_year);
    }
  }, [runResult]);

  useEffect(() => {
    const subscribe =
      window.electron?.onMLLog ?? window.electron?.onMLProgress ?? null;

    if (!subscribe) {
      return;
    }

    const unsubscribe = subscribe((line: string) => {
      setMLLogEntries((prev) => {
        const next = [...prev, line];
        return next.length > ML_LOG_MAX_LINES
          ? next.slice(-ML_LOG_MAX_LINES)
          : next;
      });

      const match = line.match(/\[(\d+)\/(\d+)\]/);
      if (match) {
        setCurrentModelProgress(`${match[1]}/${match[2]}`);
      }
    });

    return () => unsubscribe?.();
  }, []);

  useEffect(() => {
    const el = mlConsoleBodyRef.current;
    if (!el) return;

    if (mlAutoScrollRef.current) {
      el.scrollTop = el.scrollHeight;
    }
  }, [mlLogEntries]);

  useEffect(() => {
    if (!window.electron?.on) {
      return;
    }

    const unsubscribe = window.electron.on(
      "model-setup-progress",
      (...args: unknown[]) => {
        const [first, second] = args;
        const resolvedMessage =
          typeof second === "string"
            ? second
            : typeof first === "string"
            ? first
            : null;

        if (resolvedMessage) {
          setStatusMessage(`[model_setup] ${resolvedMessage}`);
          setStatusLevel("info");
        }
      },
    );

    return () => {
      if (typeof unsubscribe === "function") {
        unsubscribe();
      }
    };
  }, []);

  useEffect(() => {
    if (lowerPanelView !== "progress") {
      return;
    }

    if (!outputDirectory) {
      setProgressTrials([]);
      setProgressReferenceFitPooled(null);
      setProgressHistoryLoading(false);
      setProgressHistoryError("Choose an output folder to view ML progress.");
      return;
    }

    if (!progressDatasetId) {
      setProgressTrials([]);
      setProgressReferenceFitPooled(null);
      setProgressHistoryLoading(false);
      setProgressHistoryError(
        "Run model setup first so progress can be scoped to a dataset.",
      );
      return;
    }

    let cancelled = false;
    let intervalId: number | null = null;

    const loadProgressHistory = async (showLoader: boolean) => {
      if (showLoader) {
        setProgressHistoryLoading(true);
      }

      try {
        const history = await getMLProgressHistory(
          outputDirectory,
          progressDatasetId,
          progressReferenceModelId,
        );
        if (cancelled) {
          return;
        }
        if (
          typeof history.dataset_id === "string" &&
          history.dataset_id.trim().length > 0 &&
          history.dataset_id !== progressDatasetId
        ) {
          setProgressDatasetId(history.dataset_id);
        }
        if (
          typeof history.reference_model_id === "string" &&
          history.reference_model_id.trim().length > 0 &&
          history.reference_model_id !== progressReferenceModelId
        ) {
          setProgressReferenceModelId(history.reference_model_id);
        }
        setProgressReferenceFitPooled(
          typeof history.reference_fit_pooled === "number" &&
            Number.isFinite(history.reference_fit_pooled)
            ? history.reference_fit_pooled
            : null,
        );
        setProgressTrials(normalizeProgressTrials(history.trials));
        setProgressHistoryError(null);
      } catch (error) {
        if (cancelled) {
          return;
        }
        setProgressReferenceFitPooled(null);
        setProgressHistoryError("Unable to load ML progress history.");
      } finally {
        if (!cancelled) {
          setProgressHistoryLoading(false);
        }
      }
    };

    loadProgressHistory(true);

    intervalId = window.setInterval(() => {
      void loadProgressHistory(false);
    }, running ? 3000 : 8000);

    return () => {
      cancelled = true;
      if (intervalId != null) {
        window.clearInterval(intervalId);
      }
    };
  }, [lowerPanelView, outputDirectory, progressDatasetId, progressReferenceModelId, running]);

  const resetModelSetupState = () => {
    setModelSetupResult(null);
    setProgressDatasetId(null);
    setProgressReferenceModelId(null);
    setProgressReferenceFitPooled(null);
    setRunResult(null);
  };

  const handleMLScroll = () => {
    const el = mlConsoleBodyRef.current;
    if (!el) return;

    const distanceFromBottom =
      el.scrollHeight - el.scrollTop - el.clientHeight;

    mlAutoScrollRef.current =
      distanceFromBottom <= AUTO_SCROLL_BOTTOM_EPS_PX;
  };

  const handleEndYearInputChange = (event: ChangeEvent<HTMLInputElement>) => {
    const value = event.target.value;
    setEndYearInput(value);
    resetModelSetupState();

    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      const clamped = clampEndYear(parsed);
      setEndYear(clamped);
      targetEndYearRef.current = clamped;
    }
  };

  const handleEndYearBlur = () => {
    const parsed = Number(endYearInput);
    if (Number.isFinite(parsed)) {
      const clamped = clampEndYear(parsed);
      setEndYear(clamped);
      setEndYearInput(String(clamped));
      targetEndYearRef.current = clamped;
    } else {
      const fallback = clampEndYear(DEFAULT_END_YEAR);
      setEndYear(fallback);
      setEndYearInput(String(fallback));
      targetEndYearRef.current = fallback;
    }

    resetModelSetupState();
  };

  const handleSliderChange = (event: ChangeEvent<HTMLInputElement>) => {
    const parsed = Number(event.target.value);
    const clamped = clampEndYear(parsed);
    setEndYear(clamped);
    setEndYearInput(String(clamped));
    targetEndYearRef.current = clamped;
    resetModelSetupState();
  };

  const handleModelSetup = async () => {
    if (running || modelSetupRunning) {
      return;
    }

    setError(null);
    setRunResult(null);
    updateStageStatus("model_setup", "info", "Starting model setup...");
    setModelSetupResult(null);
    setProgressYear(null);
    setProgressPercent(0);

    if (!validatedPath || !validatedPath.trim()) {
      const message =
        "Validated IFs folder path is missing. Please re-run validation.";
      updateStageStatus("model_setup", "error", message);
      setError(message);
      return;
    }

    if (!validatedInputPath || !validatedInputPath.trim()) {
      const message =
        "Validated input file path is missing. Please re-run validation to continue.";
      updateStageStatus("model_setup", "error", message);
      setError(message);
      return;
    }

    const parsedEndYear = Number(endYearInput);
    if (!Number.isFinite(parsedEndYear) || parsedEndYear <= 0) {
      const message = "Please enter a valid end year.";
      updateStageStatus("model_setup", "error", message);
      setError(message);
      return;
    }

    const clampedEndYear = clampEndYear(parsedEndYear);
    setEndYear(clampedEndYear);
    setEndYearInput(String(clampedEndYear));
    targetEndYearRef.current = clampedEndYear;

    setModelSetupRunning(true);

    try {
      appendLog("model_setup", "info", "Submitting model setup request.");

      const response = await modelSetup({
        endYear: clampedEndYear,
        baseYear: baseYearRef.current,
        parameters: parameterRef.current,
        coefficients: coefficientRef.current,
        paramDim: paramDimensionRef.current,
        validatedPath,
        inputFilePath: validatedInputPath,
        outputFolder: outputDirectory ?? null,
      });

      setModelSetupResult(response.data);
      setProgressDatasetId(response.data.dataset_id);
      setProgressReferenceModelId(response.data.model_id);
      setProgressReferenceFitPooled(null);
      const successMessage = resolveSuccessMessage(
        response.stage,
        response.message,
      );
      const datasetWarning =
        typeof response.data.dataset_warning === "string" &&
        response.data.dataset_warning.trim().length > 0
          ? response.data.dataset_warning.trim()
          : null;
      updateStageStatus(
        response.stage,
        "success",
        datasetWarning ? `${successMessage} ${datasetWarning}` : successMessage,
      );
      setError(null);
    } catch (err) {
      const { stage, message } = resolveStageError(err, "model_setup");
      setModelSetupResult(null);
      updateStageStatus(stage, "error", message);
      setError(message);
    } finally {
      setModelSetupRunning(false);
    }
  };

  const handleRunClick = async () => {
    if (running) {
      if (stopRequested) {
        return;
      }

      if (!window.electron?.requestMLStop) {
        const message = "Electron bridge is unavailable for graceful stop requests.";
        updateStageStatus("ml_driver", "error", message);
        setError(message);
        return;
      }

      try {
        appendLog("ml_driver", "info", "Requesting graceful stop after the current run.");
        const stopResponse = await window.electron.requestMLStop();

        if (!stopResponse.accepted && !stopResponse.alreadyRequested) {
          throw new StageError(
            "ml_driver",
            "Unable to request graceful stop for the current ML Optimization run.",
          );
        }

        setStopRequested(true);
        setStopAcknowledged(Boolean(stopResponse.stopAcknowledged));
        setStatusMessage(
          "[ml_driver] Stop requested. The current run will finish before ML Optimization stops.",
        );
        setStatusLevel("info");
        setError(null);
        await refreshMLJobStatus();
      } catch (err) {
        const { stage, message } = resolveStageError(err, "ml_driver");
        setError(message);
        updateStageStatus(stage, "error", message);
      }

      return;
    }

    setError(null);
    setRunResult(null);
    setProgressYear(null);
    setProgressPercent(0);
    setMLLogEntries([]);
    mlAutoScrollRef.current = true;
    setCurrentModelProgress(null);
    setStopRequested(false);
    setStopAcknowledged(false);

    const parsedEndYear = Number(endYearInput);
    if (!Number.isFinite(parsedEndYear) || parsedEndYear <= 0) {
      const message = "Please enter a valid end year.";
      updateStageStatus("ml_driver", "error", message);
      setError(message);
      return;
    }

    if (!outputDirectory) {
      const message = "Please choose an output folder before running ML Optimization.";
      updateStageStatus("ml_driver", "error", message);
      setError(message);
      return;
    }

    if (!modelSetupResult || modelSetupRunning) {
      const message = "Please run model setup before starting ML Optimization.";
      updateStageStatus("ml_driver", "error", message);
      setError(message);
      return;
    }

    const clampedEndYear = clampEndYear(parsedEndYear);
    targetEndYearRef.current = clampedEndYear;
    setEndYear(clampedEndYear);
    setEndYearInput(String(clampedEndYear));
    setModelSetupResult(null);
    updateStageStatus("ml_driver", "info", "Starting ML Optimization run...");
    setRunning(true);

    let shouldKeepRunning = false;

    try {
      appendLog("ml_driver", "info", "Submitting ML Optimization run request.");

      if (!window.electron?.invoke) {
        throw new StageError("ml_driver", "Electron bridge is unavailable.");
      }

      const response = await window.electron.invoke("run-ml", {
        initialModelId: modelSetupResult.model_id,
        datasetId: modelSetupResult.dataset_id,
        ifsRoot: validatedPath,
        outputFolder: outputDirectory,
        baseYear: baseYearRef.current,
        endYear: clampedEndYear,
        inputFilePath: validatedInputPath,
      });

      if (
        response &&
        typeof response === "object" &&
        "alreadyRunning" in response &&
        (response as { alreadyRunning?: unknown }).alreadyRunning === true
      ) {
        const runningResponse = response as {
          alreadyRunning: true;
          job?: MLJobStatus;
        };
        shouldKeepRunning = true;
        setError(null);
        setRunResult(null);
        setStopRequested(Boolean(runningResponse.job?.stopRequested));
        setStopAcknowledged(Boolean(runningResponse.job?.stopAcknowledged));
        setStatusMessage(
          runningResponse.job?.stopRequested
            ? "[ml_driver] Re-attached to running ML Optimization job. Stop has already been requested."
            : "[ml_driver] Re-attached to running ML Optimization job.",
        );
        setStatusLevel("info");
        await refreshMLJobStatus();
        return;
      }

      if (!response || typeof response !== "object") {
        throw new StageError(
          "ml_driver",
          "Received an unexpected response from ML Optimization.",
        );
      }

      const typedResponse = response as {
        status?: unknown;
        message?: unknown;
        data?: MLDriverData;
      };

      if (typedResponse.status !== "success" || !typedResponse.data) {
        throw new StageError(
          "ml_driver",
          typeof typedResponse.message === "string"
            ? typedResponse.message
            : "ML Optimization did not return a success payload.",
        );
      }

      const nextRunResult: MLDriverData = {
        ...typedResponse.data,
        base_year: typedResponse.data.base_year ?? baseYearRef.current ?? null,
        end_year: typedResponse.data.end_year ?? clampedEndYear,
      };
      const exitCode = nextRunResult.code ?? null;

      if (typeof exitCode !== "number") {
        throw new StageError(
          "ml_driver",
          "Received an unexpected response from ML Optimization.",
        );
      }

      if (exitCode !== 0) {
        throw new StageError(
          "ml_driver",
          `ML Optimization exited with code ${exitCode}.`,
        );
      }

      setError(null);
      setRunResult(nextRunResult);
      if (
        typeof nextRunResult.dataset_id === "string" &&
        nextRunResult.dataset_id.trim().length > 0
      ) {
        setProgressDatasetId(nextRunResult.dataset_id);
      }
      setStopRequested(false);
      setStopAcknowledged(false);

      const successMessage = resolveSuccessMessage(
        "ml_driver",
        typedResponse.message,
      );
      updateStageStatus("ml_driver", "success", successMessage);

      setProgressYear(clampedEndYear);
      setProgressPercent(100);
      targetEndYearRef.current = clampedEndYear;
    } catch (err) {
      const { stage, message } = resolveStageError(err, "ml_driver");
      setError(message);
      setRunResult(null);
      setStopRequested(false);
      setStopAcknowledged(false);
      updateStageStatus(stage, "error", message);
    } finally {
      if (!shouldKeepRunning) {
        setRunning(false);
      }
      await refreshMLJobStatus();
    }
  };

  const handleLowerPanelViewChange = (nextView: LowerPanelView) => {
    setLowerPanelView(nextView);
    void window.electron?.invoke?.("ml:lowerPanelViewChanged", {
      view: nextView,
      datasetId: progressDatasetId,
    });
    if (nextView === "progress") {
      setProgressHistoryError(
        progressDatasetId
          ? null
          : "Run model setup first so progress can be scoped to a dataset.",
      );
    }
  };

  const displayPercent = Math.min(100, Math.max(0, progressPercent));
  const showProgressBar = false;
  const runProgressLabel = running
    ? progressYear != null
      ? currentModelProgress
        ? `Running ML Optimization... Model ${currentModelProgress}, current year of run: ${progressYear}`
        : `Running ML Optimization... current year of run: ${progressYear}`
      : currentModelProgress
      ? `Running ML Optimization... Model ${currentModelProgress}`
      : stopRequested
      ? stopAcknowledged
        ? "Stopping after current run..."
        : "Stop requested. Finishing current run..."
      : "Running ML Optimization..."
    : runResult
    ? null
    : progressYear != null
    ? `Last reported year: ${progressYear}`
    : null;

  const wgdDisplay =
    runResult && typeof runResult.w_gdp === "number"
      ? runResult.w_gdp.toLocaleString(undefined, { maximumFractionDigits: 2 })
      : null;
  const runButtonDisabled =
    modelSetupRunning ||
    (running ? stopRequested : !outputDirectory || !modelSetupResult);
  const runButtonLabel = running
    ? stopRequested
      ? "Stopping after current run..."
      : "Running… Click to stop after current run"
    : "Run ML Optimization";
  const latestBestFit =
    [...progressTrials]
      .reverse()
      .find((point) => typeof point.bestSoFar === "number")?.bestSoFar ?? null;
  const progressEmptyMessage = progressDatasetId
    ? `No runs found for dataset ${progressDatasetId} yet. Previous runs will appear here when they share this exact dataset.`
    : "No runs found for the current dataset yet.";

  return (
    <section className="tune-container">
      <div className="tune-header">
        <h2>Tune IFs</h2>
        <p className="tune-description">
          Launch an IFs simulation and monitor its progress in real time.
        </p>
        <p className="tune-path">
          <span className="label">Validated folder:</span> {validatedPath || "Unknown"}
        </p>
        {effectiveBaseYear != null && (
          <p className="tune-base">Base year detected: {effectiveBaseYear}</p>
        )}
        <p className="tune-output">
          <span className="label">Output folder:</span>{" "}
          {outputDirectory ?? "No folder selected"}
        </p>
      </div>

      <div className="tune-controls">
        <div className="end-year-control">
          <label className="label" htmlFor="end-year-input">
            End Year
          </label>
          <div className="end-year-inputs">
            <input
              id="end-year-input"
              type="number"
              className="path-input end-year-number"
              value={endYearInput}
              onChange={handleEndYearInputChange}
              onBlur={handleEndYearBlur}
              min={minEndYear}
              max={MAX_END_YEAR}
              disabled={running}
            />
            <input
              type="range"
              className="end-year-slider"
              value={endYear}
              onChange={handleSliderChange}
              min={minEndYear}
              max={MAX_END_YEAR}
              disabled={running}
            />
          </div>
        </div>
      </div>

      <div className="tune-actions">
        <button
          type="button"
          className="button"
          onClick={handleModelSetup}
          disabled={running || modelSetupRunning}
        >
          {modelSetupRunning ? "Setting up..." : "Model Setup"}
        </button>
        <button
          type="button"
          className="button"
          onClick={handleRunClick}
          disabled={runButtonDisabled}
        >
          {runButtonLabel}
        </button>
        <button
          type="button"
          className="button secondary"
          onClick={onBack}
          disabled={running}
        >
          Back to Validation
        </button>
      </div>

      <div className="progress-wrapper">
        {/* Show only one progress message depending on state */}
        {running ? (
          runProgressLabel && (
            <div className="progress-text">{runProgressLabel}</div>
          )
        ) : (
          statusMessage && (
            <div className={`progress-text ${statusLevel}`}>
              {statusMessage}
            </div>
          )
        )}

        {error && <div className="progress-text error">{error}</div>}

        {showProgressBar && (
          <progress
            className="progress-indicator"
            max={100}
            value={displayPercent}
          />
        )}

        {runResult ? (
          <div className="metadata-inline">
            <ul>
              {runResult.model_id && (
                <li>
                  <strong>Model ID:</strong> {runResult.model_id}
                </li>
              )}
              {typeof runResult.ifs_id === "number" && (
                <li>
                  <strong>IFs ID:</strong> {runResult.ifs_id}
                </li>
              )}
              {runResult.best_model_id && (
                <li>
                  <strong>Best model ID:</strong> {runResult.best_model_id}
                </li>
              )}
              {typeof runResult.best_fit_pooled === "number" && (
                <li>
                  <strong>Best pooled fit:</strong>{" "}
                  {runResult.best_fit_pooled.toFixed(4)}
                </li>
              )}
              {typeof runResult.iterations === "number" && (
                <li>
                  <strong>Iterations:</strong> {runResult.iterations}
                </li>
              )}
              {wgdDisplay && (
                <li>
                  <strong>World GDP (WGDP):</strong> {wgdDisplay}
                </li>
              )}
              {runResult.run_folder && (
                <li>
                  <strong>Run folder:</strong> {runResult.run_folder}
                </li>
              )}
              {runResult.output_file && (
                <li>
                  <strong>Run database:</strong> {runResult.output_file}
                </li>
              )}
            </ul>
          </div>
        ) : null}

      </div>

      <div className="ml-lower-panel">
        <div className="ml-lower-panel-header">
          <div className="ml-panel-tabs" role="tablist" aria-label="ML lower panel views">
            <button
              type="button"
              role="tab"
              className={`ml-panel-tab ${lowerPanelView === "log" ? "active" : ""}`}
              aria-selected={lowerPanelView === "log"}
              onClick={() => handleLowerPanelViewChange("log")}
            >
              ML Optimization Log
            </button>
            <button
              type="button"
              role="tab"
              className={`ml-panel-tab ${lowerPanelView === "progress" ? "active" : ""}`}
              aria-selected={lowerPanelView === "progress"}
              onClick={() => handleLowerPanelViewChange("progress")}
              disabled={!outputDirectory}
              title={!outputDirectory ? "Choose an output folder to view ML progress." : undefined}
            >
              ML Progress
            </button>
          </div>
        </div>

        {lowerPanelView === "log" ? (
          <div className="ml-console">
            <div
              className="ml-console-body"
              ref={mlConsoleBodyRef}
              onScroll={handleMLScroll}
            >
              {mlLogEntries.length === 0 ? (
                <div className="progress-text">Waiting for ML output...</div>
              ) : (
                mlLogEntries.map((entry, index) => (
                  <div key={`${index}-${entry.slice(0, 12)}`}>{entry}</div>
                ))
              )}
            </div>
          </div>
        ) : (
          <div className="ml-progress-panel">
            <p className="modal-subtitle">
              This is read-only and refreshes live while ML Optimization keeps running.
            </p>
            <div className="ml-progress-summary">
              <span>
                <strong>Points:</strong> {progressTrials.length}
              </span>
              <span>
                <strong>Latest best:</strong>{" "}
                {latestBestFit != null ? latestBestFit.toFixed(4) : "N/A"}
              </span>
            </div>
            {progressHistoryLoading ? (
              <div className="progress-text">Loading ML progress history...</div>
            ) : progressHistoryError ? (
              <div className="progress-text error">{progressHistoryError}</div>
            ) : progressTrials.length === 0 ? (
              <div className="progress-text">
                {progressDatasetId ? (
                  <>
                    <strong>Dataset ID:</strong> {progressDatasetId}
                    <br />
                  </>
                ) : null}
                {progressEmptyMessage}
              </div>
            ) : (
              <MLProgressChart
                points={progressTrials}
                referenceFitPooled={progressReferenceFitPooled}
                referenceModelId={progressReferenceModelId}
              />
            )}
          </div>
        )}
      </div>

    </section>
  );
}

function App() {
  const [ifsFolderPath, setIfsFolderPath] = useState<string | null>(null);
  const [lastValidatedIfsFolder, setLastValidatedIfsFolder] =
    useState<string | null>(null);
  const [outputDirectory, setOutputDirectory] = useState<string | null>(null);
  const [lastValidatedOutputDirectory, setLastValidatedOutputDirectory] =
    useState<string | null>(null);
  const [inputFilePath, setInputFilePath] = useState<string>("");
  const [lastValidatedInputFile, setLastValidatedInputFile] =
    useState<string | null>(null);
  const [result, setResult] = useState<CheckResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [view, setView] = useState<View>("validate");
  const [mlJobStatus, setMLJobStatus] = useState<MLJobStatus | null>(null);
  const [info, setInfo] = useState<string | null>(null);
  const [nativeFolderPickerAvailable, setNativeFolderPickerAvailable] =
    useState<boolean>(() =>
      typeof window !== "undefined" && Boolean(window.electron?.selectFolder),
    );
  const [nativeFilePickerAvailable, setNativeFilePickerAvailable] =
    useState<boolean>(() =>
      typeof window !== "undefined" && Boolean(window.electron?.selectFile),
    );
  const defaultInputLoadedRef = useRef(false);
  const mlJobStatusPollIntervalMs = 3000;

  const refreshMLJobStatus = async () => {
    if (typeof window === "undefined") {
      return;
    }

    const electron = window.electron;
    if (!electron?.getMLJobStatus) {
      return;
    }

    try {
      const status = await electron.getMLJobStatus();

      setMLJobStatus(status);
      if (status?.ifsPath) {
        setIfsFolderPath(status.ifsPath);
      }
      if (status?.outputDir) {
        setOutputDirectory(status.outputDir);
      }
      if (status?.inputExcelPath) {
        setInputFilePath(status.inputExcelPath);
      }
      if (status?.ifsValidated) {
        setLastValidatedIfsFolder(status.ifsPath ?? null);
        setLastValidatedOutputDirectory(status.outputDir ?? null);
        setLastValidatedInputFile(status.inputExcelPath ?? null);
      }
      if (status?.ifsValidated && status?.running) {
        setView("tune");
      }
    } catch (err) {
      console.warn("Unable to load ML job status:", err);
    }
  };

  useEffect(() => {
    if (typeof window !== "undefined") {
      setNativeFolderPickerAvailable(Boolean(window.electron?.selectFolder));
      setNativeFilePickerAvailable(Boolean(window.electron?.selectFile));
    }
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    const electron = window.electron;
    if (!electron?.getMLJobStatus) {
      return;
    }
    void refreshMLJobStatus();
  }, []);

  useEffect(() => {
    if (typeof window === "undefined" || !window.electron?.getMLJobStatus) {
      return;
    }

    if (!mlJobStatus?.running && !mlJobStatus?.stopRequested) {
      return;
    }

    const intervalId = window.setInterval(() => {
      void refreshMLJobStatus();
    }, mlJobStatusPollIntervalMs);

    return () => {
      window.clearInterval(intervalId);
    };
  }, [mlJobStatus?.running, mlJobStatus?.stopRequested]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    let isMounted = true;

    const applyFallback = () => {
      if (!isMounted) {
        return;
      }
      setOutputDirectory((current) => current ?? "C:/IFs_Output");
    };

    const loadDefaultOutputDir = async () => {
      if (!window.electron?.getDefaultOutputDir) {
        applyFallback();
        return;
      }

      try {
        const defaultDir = await window.electron.getDefaultOutputDir();
        if (!isMounted) {
          return;
        }

        if (typeof defaultDir === "string" && defaultDir.trim().length > 0) {
          setOutputDirectory(defaultDir);
        } else {
          applyFallback();
        }
      } catch {
        applyFallback();
      }
    };

    loadDefaultOutputDir();

    return () => {
      isMounted = false;
    };
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") return;

    let isMounted = true;

    const updateIfUninitialized = (nextValue: string) => {
      if (!isMounted) return;

      setInputFilePath((current) => {
        if (defaultInputLoadedRef.current && current?.trim().length > 0) {
          return current;
        }

        defaultInputLoadedRef.current = true;
        return nextValue;
      });
    };

    const loadDefaultInputFile = async () => {
      try {
        if (!window.electron?.getDefaultInputFile) {
          return;
        }

        const defaultFile = await window.electron.getDefaultInputFile();
        if (!isMounted) {
          return;
        }

        if (typeof defaultFile === "string" && defaultFile.trim().length > 0) {
          updateIfUninitialized(defaultFile.trim());
        } else {
          console.warn("Default input path was empty - check Electron handler.");
        }
      } catch (err) {
        console.error("Failed to load default input file:", err);
      }
    };

    loadDefaultInputFile();

    return () => {
      isMounted = false;
    };
  }, []);

  const handleChangeIFsFolder = async () => {
    setError(null);

    if (!nativeFolderPickerAvailable || !window.electron?.selectFolder) {
      setInfo("Native folder browsing is only available in the desktop app.");
      return;
    }

    try {
      setInfo(null);
      const selectedPath = await window.electron.selectFolder(
        "ifs",
        ifsFolderPath ?? undefined,
      );
      if (selectedPath) {
        setIfsFolderPath(selectedPath);
        if (selectedPath !== lastValidatedIfsFolder) {
          setResult(null);
          setLastValidatedIfsFolder(null);
        }
        if (!mlJobStatus?.running) {
          setView("validate");
        }
      }
    } catch (err) {
      setError("Unable to open the folder picker. Please try again.");
    }
  };

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const trimmedIfsPath = ifsFolderPath?.trim() ?? "";
    const trimmedOutputPath = outputDirectory?.trim() ?? "";
    const trimmedInputPath = inputFilePath?.trim() ?? "";

    if (!trimmedIfsPath) {
      setError("Please select an IFs folder before validating.");
      setResult(null);
      return;
    }

    setLoading(true);
    setError(null);
    setInfo(null);
    setResult(null);

    try {
      const res = await validateIFsFolder({
        ifsPath: trimmedIfsPath,
        outputPath: trimmedOutputPath || null,
        inputFilePath: trimmedInputPath || null,
      });
      setIfsFolderPath(trimmedIfsPath);
      setOutputDirectory(
        trimmedOutputPath.length > 0 ? trimmedOutputPath : null,
      );
      setInputFilePath(trimmedInputPath || "");
      setResult(res);

      if (res.valid) {
        setLastValidatedIfsFolder(trimmedIfsPath);
        setLastValidatedOutputDirectory(
          trimmedOutputPath.length > 0 ? trimmedOutputPath : null,
        );
        setLastValidatedInputFile(
          trimmedInputPath.length > 0 ? trimmedInputPath : null,
        );
      } else {
        setLastValidatedIfsFolder(null);
        setLastValidatedOutputDirectory(null);
        setLastValidatedInputFile(null);
      }
    } catch (err) {
      setError("Failed to validate the IFs folder. Please try again.");
    } finally {
      setLoading(false);
    }
  };

  const requestOutputDirectory = async () => {
    if (!nativeFolderPickerAvailable || !window.electron?.selectFolder) {
      throw new Error("Native folder browsing is only available in the desktop app.");
    }

    try {
      const selected = await window.electron.selectFolder(
        "output",
        outputDirectory ?? undefined,
      );
      if (selected) {
        setOutputDirectory(selected);
        if (selected !== lastValidatedOutputDirectory) {
          setResult(null);
          setLastValidatedOutputDirectory(null);
        }
        return selected;
      }
      return null;
    } catch (err) {
      throw new Error("Unable to open the folder picker. Please try again.");
    }
  };

  const handleChangeOutputDirectory = async () => {
    setError(null);

    if (!nativeFolderPickerAvailable || !window.electron?.selectFolder) {
      setInfo("Native folder browsing is only available in the desktop app.");
      return;
    }

    try {
      setInfo(null);
      await requestOutputDirectory();
    } catch (err) {
      const message =
        err instanceof Error
          ? err.message
          : "Unable to open the folder picker. Please try again.";
      setError(message);
    }
  };

  const handleBrowseInputFile = async () => {
    setError(null);

    if (!nativeFilePickerAvailable || !window.electron?.selectFile) {
      setInfo("Native file browsing is only available in the desktop app.");
      return;
    }

    try {
      setInfo(null);
      const selected = await window.electron.selectFile(
        inputFilePath && inputFilePath.length > 0 ? inputFilePath : undefined,
      );
      if (selected) {
        setInputFilePath(selected);
        if (selected !== lastValidatedInputFile) {
          setResult(null);
          setLastValidatedInputFile(null);
        }
      }
    } catch (err) {
      setError("Unable to open the file picker. Please try again.");
    }
  };

  const handleTuneClick = () => {
    setError(null);
    setInfo(null);

    if (!result?.valid) {
      setError("You must validate an IFs folder first.");
      return;
    }

    setView("tune");
  };

  const handleBaseYearChange = () => {
    setError(null);
    setInfo("Base year change functionality is coming soon.");
  };

  const missingFiles = useMemo(() => result?.missingFiles ?? [], [result]);
  const requirements = useMemo(() => result?.requirements ?? [], [result]);
  const hasValidResult = result?.valid === true;
  const outputTitle =
    outputDirectory && outputDirectory.length > 0
      ? outputDirectory
      : "No folder selected";
  const ifsFolderTitle =
    ifsFolderPath && ifsFolderPath.length > 0
      ? ifsFolderPath
      : "No folder selected";
  const inputFileTitle =
    inputFilePath && inputFilePath.length > 0
      ? inputFilePath
      : "No file selected";
  const pathChecks = result?.pathChecks;
  const ifsFolderCheck = pathChecks?.ifsFolder;
  const outputFolderCheck = pathChecks?.outputFolder;
  const inputFileCheck = pathChecks?.inputFile;
  const ifsFolderReady =
    Boolean(ifsFolderCheck?.exists) && (ifsFolderCheck?.readable ?? true);
  const outputFolderReady =
    Boolean(outputFolderCheck?.exists) && outputFolderCheck?.writable === true;
  const inputFileAvailable =
    Boolean(inputFileCheck?.exists) && Boolean(inputFileCheck?.readable);
  const sheetStatuses = REQUIRED_INPUT_SHEETS.map((name) => ({
    name,
    present: Boolean(inputFileCheck?.sheets?.[name]),
  }));
  const allSheetsPresent = sheetStatuses.every((sheet) => sheet.present);
  const missingSheetNames = inputFileCheck?.missingSheets ?? [];
  const sheetMessage =
    inputFileCheck?.exists &&
    inputFileCheck?.readable &&
    missingSheetNames.length > 0
      ? `Missing sheets: ${missingSheetNames.join(", ")}`
      : null;

  return (
    <div className="container">
      <header className="header">
        <h1>
          {view === "validate"
            ? "BIGPOPA - IFs Folder Check"
            : "BIGPOPA - Tune IFs"}
        </h1>
        <p className="subtitle">
          {view === "validate"
            ? "Browse to your IFs installation folder and validate it against the backend API."
            : "Configure and launch ML Optimization runs with live progress tracking."}
        </p>
      </header>

      {view === "validate" && (
        <>
          <form className="form" onSubmit={handleSubmit}>
            <div className="input-row">
              <button
                type="button"
                className="button"
                onClick={handleChangeIFsFolder}
              >
                Change IFs Folder
              </button>
              <input
                type="text"
                className="path-input"
                value={ifsFolderPath ?? ""}
                onChange={(e) => {
                  const nextValue = e.target.value;
                  setIfsFolderPath(nextValue);
                  if (nextValue !== lastValidatedIfsFolder) {
                    setResult(null);
                    setLastValidatedIfsFolder(null);
                  }
                }}
                spellCheck={false}
                placeholder="No folder selected"
                title={ifsFolderTitle}
              />
            </div>
            <div className="input-row">
              <button
                type="button"
                className="button"
                onClick={handleChangeOutputDirectory}
              >
                Change Output Folder
              </button>
              <input
                type="text"
                className="path-input"
                value={outputDirectory ?? ""}
                onChange={(e) => {
                  const nextValue = e.target.value;
                  setOutputDirectory(nextValue);
                  if (nextValue !== lastValidatedOutputDirectory) {
                    setResult(null);
                    setLastValidatedOutputDirectory(null);
                  }
                }}
                placeholder="No folder selected"
                spellCheck={false}
                title={outputTitle}
              />
            </div>
            <div className="input-row">
              <button
                type="button"
                className="button"
                onClick={handleBrowseInputFile}
              >
                Change Input Folder
              </button>
              <input
                type="text"
                className="path-input"
                value={inputFilePath}
                onChange={(e) => {
                  const nextValue = e.target.value;
                  setInputFilePath(nextValue);
                  if (nextValue !== lastValidatedInputFile) {
                    setResult(null);
                    setLastValidatedInputFile(null);
                  }
                }}
                placeholder="No file selected"
                spellCheck={false}
                title={inputFileTitle}
              />
            </div>
            <div className="button-row">
              <button type="submit" className="button">
                {loading ? "Validating..." : "Validate"}
              </button>
            </div>
            <div className="button-row multi">
              <button
                type="button"
                className="button"
                onClick={handleTuneClick}
                disabled={!hasValidResult}
              >
                Tune IFs
              </button>
              <button
                type="button"
                className="button"
                onClick={handleBaseYearChange}
                disabled={!hasValidResult}
              >
                Base Year Change
              </button>
            </div>
          </form>

          {info && <div className="alert alert-info">{info}</div>}
          {error && <div className="alert alert-error">{error}</div>}

          {result && (
            <section className="results">
              <h2>Validation Results</h2>
              <div className={result.valid ? "status success" : "status error"}>
                {result.valid ? "Valid" : "Invalid"}
              </div>
              {result.base_year != null && (
                <div className="base-year">Base year: {result.base_year}</div>
              )}

              {(ifsFolderCheck || outputFolderCheck || inputFileCheck) && (
                <div className="summary">
                  {ifsFolderCheck && (
                    <div
                      className={`summary-line ${
                        ifsFolderReady ? "success" : "error"
                      }`}
                    >
                      <span className="summary-label">
                        {ifsFolderReady
                          ? "IFs folder found:"
                          : "IFs folder missing."}
                      </span>
                      {ifsFolderReady && (
                        <span className="summary-value">
                          {ifsFolderCheck.displayPath ?? ifsFolderTitle}
                        </span>
                      )}
                      {!ifsFolderReady && ifsFolderCheck.message && (
                        <span className="summary-message">
                          {ifsFolderCheck.message}
                        </span>
                      )}
                    </div>
                  )}

                  {outputFolderCheck && (
                    <div
                      className={`summary-line ${
                        outputFolderReady ? "success" : "error"
                      }`}
                    >
                      <span className="summary-label">
                        {outputFolderReady
                          ? "Output folder ready:"
                          : outputFolderCheck.exists
                          ? "Output folder limited:"
                          : "Output folder missing."}
                      </span>
                      {outputFolderCheck.exists && (
                        <span className="summary-value">
                          {outputFolderCheck.displayPath ?? outputTitle}
                        </span>
                      )}
                      {outputFolderCheck.message && (
                        <span className="summary-message">
                          {outputFolderCheck.message}
                        </span>
                      )}
                    </div>
                  )}

                  {inputFileCheck && (
                    <>
                      <div
                        className={`summary-line ${
                          inputFileAvailable ? "success" : "error"
                        }`}
                      >
                        <span className="summary-label">
                          {inputFileAvailable
                            ? "Input file found:"
                            : "Input file missing."}
                        </span>
                        {inputFileAvailable && (
                          <span className="summary-value">
                            {inputFileCheck.displayPath ?? inputFileTitle}
                          </span>
                        )}
                        {!inputFileAvailable && inputFileCheck.message && (
                          <span className="summary-message">
                            {inputFileCheck.message}
                          </span>
                        )}
                      </div>
                      <div
                        className={`summary-line ${
                          allSheetsPresent ? "success" : "error"
                        }`}
                      >
                        <span className="summary-label">Sheets found:</span>
                        <span className="summary-value sheet-list">
                          {sheetStatuses.map(({ name, present }) => (
                            <span
                              key={name}
                              className={`sheet-status ${
                                present ? "present" : "missing"
                              }`}
                            >
                              {name} {present ? "[x]" : "[ ]"}
                            </span>
                          ))}
                        </span>
                        {sheetMessage && (
                          <span className="summary-message">{sheetMessage}</span>
                        )}
                      </div>
                    </>
                  )}
                </div>
              )}

              {requirements.length > 0 && (
                <div className="requirements">
                  <h3>Required files &amp; folders</h3>
                  <ul>
                    {requirements.map((item) => (
                      <li
                        key={item.file}
                        className={item.exists ? "item success" : "item error"}
                      >
                        <span className="icon">{item.exists ? "OK" : "X"}</span>
                        <span>{item.file}</span>
                      </li>
                    ))}
                  </ul>
                </div>
              )}

              {missingFiles.length > 0 && (
                <div className="requirements">
                  <h3>Missing files</h3>
                  <ul>
                    {missingFiles.map((file) => (
                      <li key={file} className="item error">
                        <span className="icon">X</span>
                        <span>{file}</span>
                      </li>
                    ))}
                  </ul>
                </div>
              )}
            </section>
          )}
        </>
      )}

      {view === "tune" && (result?.valid || mlJobStatus?.running || mlJobStatus?.ifsValidated) && (
        <TuneIFsPage
          onBack={() => setView("validate")}
          onMLJobStatusRefresh={refreshMLJobStatus}
          validatedPath={
            lastValidatedIfsFolder ?? ifsFolderPath?.trim() ?? ""
          }
          validatedInputPath={
            lastValidatedInputFile ?? inputFilePath?.trim() ?? ""
          }
          baseYear={result?.base_year ?? mlJobStatus?.runConfig?.baseYear ?? null}
          outputDirectory={outputDirectory ?? mlJobStatus?.outputDir ?? null}
          initialMLJobRunning={Boolean(mlJobStatus?.running)}
          initialMLJobProgress={mlJobStatus?.progress?.text ?? null}
          initialRunConfig={mlJobStatus?.runConfig ?? null}
          initialStopRequested={mlJobStatus?.stopRequested ?? false}
          initialStopAcknowledged={mlJobStatus?.stopAcknowledged ?? false}
          initialFinalResult={mlJobStatus?.finalResult ?? null}
          initialTerminationReason={mlJobStatus?.terminationReason ?? null}
        />
      )}

      {view === "tune" && !result?.valid && !mlJobStatus?.running && !mlJobStatus?.ifsValidated && (
        <div className="alert alert-error">
          <p className="alert-message">
            Validation is required before tuning IFs. Please return to the
            validation page.
          </p>
          <div className="alert-actions">
            <button
              type="button"
              className="button secondary"
              onClick={() => setView("validate")}
            >
              Back to Validation
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

export default App;
