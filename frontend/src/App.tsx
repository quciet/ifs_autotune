import {
  ChangeEvent,
  FormEvent,
  useEffect,
  useId,
  useMemo,
  useRef,
  useState,
} from "react";
import {
  getArtifactImagePreview,
  type InputProfileDetail,
  getDesktopCapabilities,
  getMLProgressHistory,
  getTrendDatasetOptions,
  modelSetup,
  openArtifactPath,
  runTrendAnalysis,
  subscribeToIFsProgress,
  validateIFsFolder,
  StageError,
  type ApiStage,
  type ApiStatus,
  type CheckResponse,
  type ArtifactRetentionMode,
  type IFsProgressEvent,
  type ModelSetupData,
  type MLDriverData,
  type MLProgressTrial,
  type TrendAnalysisData,
} from "./api";
import InputProfilesPanel from "./InputProfilesPanel";
import {
  MLProgressChart,
  appendNormalizedProgressTrials,
  normalizeProgressTrials,
  type ChartPoint,
} from "./MLProgressChart";

type View = "validate" | "setup" | "ml";

type StatusLevel = "info" | "success" | "error";

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
  inputProfileId: number | null;
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

type TrendPreviewType = "fit" | "parameter" | "coefficient";

const ARTIFACT_RETENTION_LABELS: Record<ArtifactRetentionMode, string> = {
  none: "No saved model folders",
  best_only: "Keep current best only",
  all: "Keep all model folders",
};

const ARTIFACT_RETENTION_ORDER: ArtifactRetentionMode[] = ["none", "best_only", "all"];

function formatTrendBestReference(summary: TrendAnalysisData["summary"]): string {
  if (
    typeof summary.best_run_index === "number" &&
    typeof summary.best_round_index === "number" &&
    typeof summary.best_trial_index === "number"
  ) {
    return `Run ${summary.best_run_index} (round ${summary.best_round_index}, trial ${summary.best_trial_index})`;
  }

  if (typeof summary.best_run_index === "number") {
    return `Run ${summary.best_run_index}`;
  }

  return "N/A";
}

function formatTrendBestReferenceInline(summary: TrendAnalysisData["summary"]): string {
  if (
    typeof summary.best_run_index === "number" &&
    typeof summary.best_round_index === "number" &&
    typeof summary.best_trial_index === "number"
  ) {
    return `Run ${summary.best_run_index}, round ${summary.best_round_index}, trial ${summary.best_trial_index}`;
  }

  if (typeof summary.best_run_index === "number") {
    return `Run ${summary.best_run_index}`;
  }

  return "N/A";
}

function clampPageIndex(index: number, pageCount: number): number {
  if (pageCount <= 0) {
    return 0;
  }

  return Math.min(Math.max(index, 0), pageCount - 1);
}

function resolvePagedArtifactPath(paths: string[], pageIndex: number): string | null {
  if (!Array.isArray(paths) || paths.length === 0) {
    return null;
  }

  return paths[clampPageIndex(pageIndex, paths.length)] ?? null;
}

function normalizeDatasetIdInput(value: string): string | null {
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function truncateMiddle(value: string, maxLength: number): string {
  if (value.length <= maxLength) {
    return value;
  }

  if (maxLength <= 3) {
    return value.slice(0, maxLength);
  }

  const ellipsis = "...";
  const visibleCharacters = maxLength - ellipsis.length;
  const startLength = Math.ceil(visibleCharacters * 0.55);
  const endLength = Math.max(1, visibleCharacters - startLength);
  return `${value.slice(0, startLength)}${ellipsis}${value.slice(-endLength)}`;
}

function OverflowAwareMiddleTruncate({
  value,
  className,
  title,
}: {
  value: string;
  className?: string;
  title?: string;
}) {
  const visibleRef = useRef<HTMLSpanElement | null>(null);
  const measureRef = useRef<HTMLSpanElement | null>(null);
  const [displayValue, setDisplayValue] = useState(value);

  useEffect(() => {
    let frameId: number | null = null;
    let resizeObserver: ResizeObserver | null = null;

    const updateDisplayValue = () => {
      const visible = visibleRef.current;
      const measure = measureRef.current;
      if (!visible || !measure) {
        return;
      }

      const availableWidth = visible.clientWidth;
      if (availableWidth <= 0) {
        setDisplayValue(value);
        return;
      }

      measure.textContent = value;
      const fullWidth = measure.getBoundingClientRect().width;
      if (fullWidth <= availableWidth + 0.5) {
        setDisplayValue(value);
        return;
      }

      let low = 4;
      let high = Math.max(4, value.length - 1);
      let bestFit = truncateMiddle(value, 4);

      while (low <= high) {
        const mid = Math.floor((low + high) / 2);
        const candidate = truncateMiddle(value, mid);
        measure.textContent = candidate;
        const candidateWidth = measure.getBoundingClientRect().width;

        if (candidateWidth <= availableWidth + 0.5) {
          bestFit = candidate;
          low = mid + 1;
        } else {
          high = mid - 1;
        }
      }

      setDisplayValue(bestFit);
    };

    const scheduleUpdate = () => {
      if (frameId != null) {
        window.cancelAnimationFrame(frameId);
      }
      frameId = window.requestAnimationFrame(updateDisplayValue);
    };

    scheduleUpdate();

    const handleResize = () => {
      scheduleUpdate();
    };

    window.addEventListener("resize", handleResize);

    if (typeof ResizeObserver !== "undefined") {
      resizeObserver = new ResizeObserver(() => {
        scheduleUpdate();
      });

      if (visibleRef.current) {
        resizeObserver.observe(visibleRef.current);
      }

      if (visibleRef.current?.parentElement) {
        resizeObserver.observe(visibleRef.current.parentElement);
      }
    }

    return () => {
      window.removeEventListener("resize", handleResize);
      if (frameId != null) {
        window.cancelAnimationFrame(frameId);
      }
      resizeObserver?.disconnect();
    };
  }, [value]);

  const combinedClassName = [className, "tune-meta-value"].filter(Boolean).join(" ");

  return (
    <>
      <span ref={visibleRef} className={combinedClassName} title={title}>
        {displayValue}
      </span>
      <span ref={measureRef} className={`${combinedClassName} tune-meta-measure`} aria-hidden="true" />
    </>
  );
}

function parseSequenceIndex(trial: MLProgressTrial): number | null {
  return typeof trial.sequence_index === "number" &&
    Number.isFinite(trial.sequence_index) &&
    trial.sequence_index > 0
    ? trial.sequence_index
    : null;
}

function parseProgressRowId(trial: MLProgressTrial): number | null {
  const rowId =
    typeof trial.run_id === "number" && Number.isFinite(trial.run_id) && trial.run_id > 0
      ? trial.run_id
      : typeof trial.progress_rowid === "number" &&
          Number.isFinite(trial.progress_rowid) &&
          trial.progress_rowid > 0
        ? trial.progress_rowid
        : null;
  return rowId != null
    ? rowId
    : null;
}

function canMergeIncrementalTrials(
  existing: ChartPoint[],
  incoming: MLProgressTrial[],
  sinceProgressRowId: number,
): boolean {
  if (incoming.length === 0) {
    return true;
  }

  const firstSequenceIndex = parseSequenceIndex(incoming[0]);
  if (firstSequenceIndex == null || firstSequenceIndex < 1 || firstSequenceIndex > existing.length + 1) {
    return false;
  }

  return incoming.every((trial, index) => {
    const sequenceIndex = parseSequenceIndex(trial);
      const progressRowId = parseProgressRowId(trial);
      return (
        sequenceIndex === firstSequenceIndex + index &&
        (progressRowId == null || progressRowId >= sinceProgressRowId)
      );
    });
}

function mergeIncrementalProgressTrials(
  existing: ChartPoint[],
  incoming: MLProgressTrial[],
): ChartPoint[] {
  if (incoming.length === 0) {
    return existing;
  }

  const firstSequenceIndex = parseSequenceIndex(incoming[0]);
  if (firstSequenceIndex == null || firstSequenceIndex < 1) {
    return normalizeProgressTrials(incoming);
  }

  const preservedPrefix = existing.slice(0, Math.max(0, firstSequenceIndex - 1));
  return appendNormalizedProgressTrials(preservedPrefix, incoming);
}

type LogStatus = ApiStatus | "info";

type LogEntry = {
  id: number;
  stage: ApiStage;
  message: string;
  status: LogStatus;
};

type TuneIFsPageProps = {
  pageMode: "setup" | "ml";
  onBackToValidation: () => void;
  onBackToSetup: () => void;
  onOpenMLPage: () => void;
  onMLJobStatusRefresh?: () => Promise<void>;
  validatedPath: string;
  validatedProfileId: number | null;
  baseYear?: number | null;
  outputDirectory: string | null;
  rollingWindow: number;
  rollingWindowInput: string;
  onRollingWindowInputChange: (value: string) => void;
  initialMLJobRunning?: boolean;
  initialMLJobProgress?: string | null;
  initialRunConfig?: MLJobStatus["runConfig"];
  initialStopRequested?: boolean;
  initialStopAcknowledged?: boolean;
  initialFinalResult?: MLFinalResult | null;
  initialTerminationReason?: MLTerminationReason | null;
};

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
  pageMode,
  onBackToValidation,
  onBackToSetup,
  onOpenMLPage,
  onMLJobStatusRefresh,
  validatedPath,
  validatedProfileId,
  baseYear,
  outputDirectory,
  rollingWindow,
  rollingWindowInput,
  onRollingWindowInputChange,
  initialMLJobRunning,
  initialMLJobProgress,
  initialRunConfig,
  initialStopRequested,
  initialStopAcknowledged,
  initialFinalResult,
  initialTerminationReason,
}: TuneIFsPageProps) {
  const isSetupPage = pageMode === "setup";
  const isMLPage = pageMode === "ml";
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
  const [currentModelProgress, setCurrentModelProgress] = useState<string | null>(null);
  const [progressTrials, setProgressTrials] = useState<ChartPoint[]>([]);
  const [progressLatestProgressRowId, setProgressLatestProgressRowId] = useState<number | null>(null);
  const [progressHistoryLoading, setProgressHistoryLoading] = useState(false);
  const [progressHistoryError, setProgressHistoryError] = useState<string | null>(null);
  const trendDatasetInputId = useId();
  const [trendDatasetOverride, setTrendDatasetOverride] = useState("");
  const [trendDatasetOptions, setTrendDatasetOptions] = useState<string[]>([]);
  const [trendDatasetRunCounts, setTrendDatasetRunCounts] = useState<Record<string, number>>({});
  const [trendDatasetOptionsLoading, setTrendDatasetOptionsLoading] = useState(false);
  const [trendDatasetOptionsError, setTrendDatasetOptionsError] = useState<string | null>(null);
  const [trendLatestDatasetId, setTrendLatestDatasetId] = useState<string | null>(null);
  const [trendLimitInput, setTrendLimitInput] = useState("");
  const [trendWindowInput, setTrendWindowInput] = useState("25");
  const [trendAnalysisRunning, setTrendAnalysisRunning] = useState(false);
  const [trendAnalysisError, setTrendAnalysisError] = useState<string | null>(null);
  const [trendAnalysisResult, setTrendAnalysisResult] = useState<TrendAnalysisData | null>(null);
  const [trendAnalysisMessage, setTrendAnalysisMessage] = useState<string | null>(null);
  const [activeTrendPreview, setActiveTrendPreview] = useState<TrendPreviewType | null>(null);
  const [fitTrendPreviewUrl, setFitTrendPreviewUrl] = useState<string | null>(null);
  const [fitTrendPreviewLoading, setFitTrendPreviewLoading] = useState(false);
  const [parameterTrendPageIndex, setParameterTrendPageIndex] = useState(0);
  const [parameterTrendPreviewUrl, setParameterTrendPreviewUrl] = useState<string | null>(null);
  const [parameterTrendPreviewLoading, setParameterTrendPreviewLoading] = useState(false);
  const [coefficientTrendPageIndex, setCoefficientTrendPageIndex] = useState(0);
  const [coefficientTrendPreviewUrl, setCoefficientTrendPreviewUrl] = useState<string | null>(null);
  const [coefficientTrendPreviewLoading, setCoefficientTrendPreviewLoading] = useState(false);
  const [trendAnalysisAvailable, setTrendAnalysisAvailable] = useState<boolean>(() =>
    typeof window !== "undefined" &&
    Boolean(window.electron?.runTrendAnalysis && window.electron?.getDesktopCapabilities),
  );
  const [trendAnalysisAvailabilityMessage, setTrendAnalysisAvailabilityMessage] =
    useState<string | null>(null);
  const [effectiveBaseYear, setEffectiveBaseYear] = useState<number | null>(
    typeof baseYear === "number" && Number.isFinite(baseYear) ? baseYear : null,
  );
  const [artifactRetentionMode, setArtifactRetentionMode] =
    useState<ArtifactRetentionMode>("none");
  const baseYearRef = useRef<number | null>(baseYear ?? null);
  const targetEndYearRef = useRef<number | null>(normalizedInitialEndYear);
  const parameterRef = useRef<Record<string, unknown>>({});
  const coefficientRef = useRef<Record<string, unknown>>({});
  const paramDimensionRef = useRef<Record<string, unknown>>({});
  const progressTrialsRef = useRef<ChartPoint[]>([]);
  const progressLatestProgressRowIdRef = useRef<number | null>(null);
  const trendPreviewCacheRef = useRef<Record<string, string>>({});
  const logIdRef = useRef(0);
  const previousInitialMLJobRunningRef = useRef(Boolean(initialMLJobRunning));
  const filteredTrendDatasetOptions = useMemo(() => {
    const normalizedQuery = trendDatasetOverride.trim().toLowerCase();
    if (!normalizedQuery) {
      return trendDatasetOptions;
    }

      return trendDatasetOptions.filter((datasetId) =>
        datasetId.toLowerCase().includes(normalizedQuery),
      );
  }, [trendDatasetOptions, trendDatasetOverride]);
  const selectedTrendDatasetId = useMemo(
    () => normalizeDatasetIdInput(trendDatasetOverride),
    [trendDatasetOverride],
  );
  const activeTrendDatasetId = selectedTrendDatasetId ?? trendLatestDatasetId ?? null;
  const activeTrendDatasetRunCount = useMemo(() => {
    if (!activeTrendDatasetId) {
      return null;
    }
    const runCount = trendDatasetRunCounts[activeTrendDatasetId];
    return typeof runCount === "number" && Number.isFinite(runCount) && runCount >= 0
      ? runCount
      : null;
  }, [activeTrendDatasetId, trendDatasetRunCounts]);
  const currentParameterPlotPath = useMemo(
    () =>
      trendAnalysisResult
        ? resolvePagedArtifactPath(
            trendAnalysisResult.parameter_plot_paths,
            parameterTrendPageIndex,
          )
        : null,
    [parameterTrendPageIndex, trendAnalysisResult],
  );
  const currentCoefficientPlotPath = useMemo(
    () =>
      trendAnalysisResult
        ? resolvePagedArtifactPath(
            trendAnalysisResult.coefficient_plot_paths,
            coefficientTrendPageIndex,
          )
        : null,
    [coefficientTrendPageIndex, trendAnalysisResult],
  );

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

  useEffect(() => {
    let cancelled = false;

    const refreshTrendAnalysisAvailability = async () => {
      if (typeof window === "undefined" || !window.electron) {
        if (!cancelled) {
          setTrendAnalysisAvailable(false);
          setTrendAnalysisAvailabilityMessage(
            "Trend Analysis is unavailable because the Electron desktop bridge did not load. Restart the app and try again.",
          );
        }
        return;
      }

      if (
        !window.electron.runTrendAnalysis ||
        !window.electron.getDesktopCapabilities
      ) {
        if (!cancelled) {
          setTrendAnalysisAvailable(false);
          setTrendAnalysisAvailabilityMessage(
            "Trend Analysis is unavailable in this desktop session. Restart the app to load the latest desktop handlers.",
          );
        }
        return;
      }

      try {
        const capabilities = await getDesktopCapabilities();
        if (cancelled) {
          return;
        }
        setTrendAnalysisAvailable(capabilities.trendAnalysis);
        setTrendAnalysisAvailabilityMessage(
          capabilities.trendAnalysis
            ? null
            : "Trend Analysis is unavailable in this desktop session. Restart the app to load the latest desktop handlers.",
        );
      } catch (error) {
        if (!cancelled) {
          setTrendAnalysisAvailable(false);
          setTrendAnalysisAvailabilityMessage(
            error instanceof Error && error.message.trim().length > 0
              ? error.message
              : "Unable to verify Trend Analysis availability. Restart the app and try again.",
          );
        }
      }
    };

    void refreshTrendAnalysisAvailability();

    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    let cancelled = false;

    setTrendDatasetOverride("");
    setTrendDatasetOptions([]);
    setTrendDatasetRunCounts({});
    setTrendDatasetOptionsError(null);
    setTrendLatestDatasetId(null);
    setTrendLimitInput("");
    setTrendAnalysisError(null);
    setTrendAnalysisMessage(null);
    setTrendAnalysisResult(null);
    setActiveTrendPreview(null);
    setFitTrendPreviewUrl(null);
    setFitTrendPreviewLoading(false);
    setParameterTrendPageIndex(0);
    setParameterTrendPreviewUrl(null);
    setParameterTrendPreviewLoading(false);
    setCoefficientTrendPageIndex(0);
    setCoefficientTrendPreviewUrl(null);
    setCoefficientTrendPreviewLoading(false);
    trendPreviewCacheRef.current = {};

    if (!outputDirectory) {
      setTrendDatasetOptionsLoading(false);
      return;
    }

    const loadTrendDatasetOptions = async () => {
      setTrendDatasetOptionsLoading(true);

      try {
        const response = await getTrendDatasetOptions(outputDirectory);
        if (cancelled) {
          return;
        }
        setTrendDatasetOptions(response.dataset_ids);
        setTrendDatasetRunCounts(response.dataset_run_counts);
        setTrendLatestDatasetId(response.latest_dataset_id);
        setTrendDatasetOptionsError(null);
      } catch (error) {
        if (cancelled) {
          return;
        }
        setTrendDatasetOptions([]);
        setTrendDatasetRunCounts({});
        setTrendLatestDatasetId(null);
        setTrendDatasetOptionsError(
          error instanceof Error ? error.message : "Unable to load dataset suggestions.",
        );
      } finally {
        if (!cancelled) {
          setTrendDatasetOptionsLoading(false);
        }
      }
    };

    void loadTrendDatasetOptions();

    return () => {
      cancelled = true;
    };
  }, [outputDirectory]);

  useEffect(() => {
    if (
      typeof activeTrendDatasetRunCount === "number" &&
      Number.isFinite(activeTrendDatasetRunCount) &&
      activeTrendDatasetRunCount > 0
    ) {
      setTrendLimitInput(String(activeTrendDatasetRunCount));
      return;
    }

    setTrendLimitInput("");
  }, [activeTrendDatasetRunCount]);

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

  const normalizeRestoredEndYear = (value: unknown) => {
    const parsed = Number(value);
    if (!Number.isFinite(parsed) || parsed <= 0) {
      return clampEndYear(DEFAULT_END_YEAR);
    }
    return clampEndYear(parsed);
  };

  useEffect(() => {
    const normalized =
      typeof baseYear === "number" && Number.isFinite(baseYear) ? baseYear : null;
    baseYearRef.current = normalized;
    setEffectiveBaseYear(normalized);
  }, [baseYear]);

  useEffect(() => {
    progressTrialsRef.current = progressTrials;
  }, [progressTrials]);

  useEffect(() => {
    progressLatestProgressRowIdRef.current = progressLatestProgressRowId;
  }, [progressLatestProgressRowId]);

  useEffect(() => {
    const nextRunning = Boolean(initialMLJobRunning);
    const wasRunning = previousInitialMLJobRunningRef.current;
    previousInitialMLJobRunningRef.current = nextRunning;

    if (nextRunning) {
      setRunning(true);
      setStatusMessage("Re-attached to running ML Optimization job.");
      setStatusLevel("info");
      if (initialMLJobProgress) {
        setCurrentModelProgress(initialMLJobProgress);
      }
      return;
    }

    if (wasRunning) {
      setRunning(false);
      setStopRequested(false);
      setStopAcknowledged(false);
      setCurrentModelProgress(null);

      if (initialRunResult) {
        const message =
          initialTerminationReason === "stopped_gracefully"
            ? "ML optimization stopped after the current run."
            : "ML optimization completed successfully.";
        setRunResult(initialRunResult);
        setStatusMessage(`[ml_driver] ${message}`);
        setStatusLevel("success");
      } else {
        setStatusMessage("Waiting to start.");
        setStatusLevel("info");
      }
    }
  }, [
    initialMLJobProgress,
    initialMLJobRunning,
    initialRunResult,
    initialTerminationReason,
  ]);

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
    const restoredEndYear = normalizeRestoredEndYear(initialRunConfig?.endYear);
    setModelSetupResult(null);
    setEndYear(restoredEndYear);
    setEndYearInput(String(restoredEndYear));
    targetEndYearRef.current = restoredEndYear;
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
    setProgressLatestProgressRowId(null);
    setRunResult(initialRunResult);
    setArtifactRetentionMode("none");
    setLogEntries([]);
    logIdRef.current = 0;
    setProgressYear(null);
    setProgressPercent(0);
    setError(null);
  }, [
    validatedPath,
    validatedProfileId,
    outputDirectory,
    initialRunConfig?.datasetId,
    initialRunConfig?.endYear,
    initialRunConfig?.initialModelId,
    minEndYear,
  ]);

  useEffect(() => {
    setEndYear((current) => {
      const restored = normalizeRestoredEndYear(initialRunConfig?.endYear);
      const fallback = Number.isFinite(current) ? (current as number) : restored;
      const next = clampEndYear(fallback);
      if (next !== current) {
        setEndYearInput(String(next));
      }
      targetEndYearRef.current = next;
      return next;
    });
  }, [initialRunConfig?.endYear, minEndYear]);

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
      const match = line.match(/\[(\d+)\/(\d+)\]/);
      if (match) {
        setCurrentModelProgress(`${match[1]}/${match[2]}`);
      }
    });

    return () => unsubscribe?.();
  }, []);

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
    if (!outputDirectory) {
      setProgressTrials([]);
      setProgressLatestProgressRowId(null);
      setProgressReferenceFitPooled(null);
      setProgressHistoryLoading(false);
      setProgressHistoryError("Choose an output folder to view ML progress.");
      return;
    }

    if (!progressDatasetId) {
      setProgressTrials([]);
      setProgressLatestProgressRowId(null);
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
        const sinceProgressRowId = progressLatestProgressRowIdRef.current;
        let history = await getMLProgressHistory(
          outputDirectory,
          progressDatasetId,
          progressReferenceModelId,
          sinceProgressRowId,
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

        const latestProgressRowId =
          typeof history.latest_run_id === "number" &&
          Number.isFinite(history.latest_run_id)
            ? history.latest_run_id
            : null;
        const shouldAppend =
          sinceProgressRowId != null && progressTrialsRef.current.length > 0;

        if (shouldAppend) {
          const incomingTrials = Array.isArray(history.trials) ? history.trials : [];
          const currentTrials = progressTrialsRef.current;
            const hasAppendContinuity = canMergeIncrementalTrials(
              currentTrials,
              incomingTrials,
              sinceProgressRowId,
            );

          if (hasAppendContinuity) {
            if (incomingTrials.length > 0) {
              setProgressTrials((current) => mergeIncrementalProgressTrials(current, incomingTrials));
            }
            setProgressLatestProgressRowId(latestProgressRowId);
            setProgressHistoryError(null);
            return;
          }

          history = await getMLProgressHistory(
            outputDirectory,
            progressDatasetId,
            progressReferenceModelId,
            null,
          );
          if (cancelled) {
            return;
          }
        }

        setProgressTrials(normalizeProgressTrials(history.trials));
        setProgressLatestProgressRowId(
          typeof history.latest_run_id === "number" &&
            Number.isFinite(history.latest_run_id)
            ? history.latest_run_id
            : null,
        );
        setProgressHistoryError(null);
      } catch (error) {
        if (cancelled) {
          return;
        }
        setProgressReferenceFitPooled(null);
        setProgressLatestProgressRowId(null);
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
  }, [outputDirectory, progressDatasetId, progressReferenceModelId, running]);

  useEffect(() => {
    let cancelled = false;

    const loadPreview = async (
      targetPath: string | null,
      setLoading: (value: boolean) => void,
      setPreview: (value: string | null) => void,
    ) => {
      if (!trendAnalysisResult?.output_dir || !targetPath) {
        setLoading(false);
        setPreview(null);
        return;
      }

      const cached = trendPreviewCacheRef.current[targetPath];
      if (cached) {
        setLoading(false);
        setPreview(cached);
        return;
      }

      setLoading(true);
      try {
        const preview = await getArtifactImagePreview(
          targetPath,
          trendAnalysisResult.output_dir,
        );
        if (cancelled) {
          return;
        }
        trendPreviewCacheRef.current[targetPath] = preview.dataUrl;
        setPreview(preview.dataUrl);
      } catch (error) {
        if (!cancelled) {
          setPreview(null);
          setTrendAnalysisError(
            error instanceof Error ? error.message : "Unable to load image preview.",
          );
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    };

    if (activeTrendPreview === "fit") {
      void loadPreview(
        trendAnalysisResult?.plot_path ?? null,
        setFitTrendPreviewLoading,
        setFitTrendPreviewUrl,
      );
      return () => {
        cancelled = true;
      };
    }

    setFitTrendPreviewLoading(false);
    setFitTrendPreviewUrl(null);

    return () => {
      cancelled = true;
    };
  }, [activeTrendPreview, trendAnalysisResult?.output_dir, trendAnalysisResult?.plot_path]);

  useEffect(() => {
    let cancelled = false;

    const loadPreview = async () => {
      if (!trendAnalysisResult?.output_dir || !currentParameterPlotPath) {
        setParameterTrendPreviewLoading(false);
        setParameterTrendPreviewUrl(null);
        return;
      }

      const cached = trendPreviewCacheRef.current[currentParameterPlotPath];
      if (cached) {
        setParameterTrendPreviewLoading(false);
        setParameterTrendPreviewUrl(cached);
        return;
      }

      setParameterTrendPreviewLoading(true);
      try {
        const preview = await getArtifactImagePreview(
          currentParameterPlotPath,
          trendAnalysisResult.output_dir,
        );
        if (cancelled) {
          return;
        }
        trendPreviewCacheRef.current[currentParameterPlotPath] = preview.dataUrl;
        setParameterTrendPreviewUrl(preview.dataUrl);
      } catch (error) {
        if (!cancelled) {
          setParameterTrendPreviewUrl(null);
          setTrendAnalysisError(
            error instanceof Error ? error.message : "Unable to load image preview.",
          );
        }
      } finally {
        if (!cancelled) {
          setParameterTrendPreviewLoading(false);
        }
      }
    };

    if (activeTrendPreview === "parameter") {
      void loadPreview();
      return () => {
        cancelled = true;
      };
    }

    setParameterTrendPreviewLoading(false);
    setParameterTrendPreviewUrl(null);

    return () => {
      cancelled = true;
    };
  }, [activeTrendPreview, currentParameterPlotPath, trendAnalysisResult?.output_dir]);

  useEffect(() => {
    let cancelled = false;

    const loadPreview = async () => {
      if (!trendAnalysisResult?.output_dir || !currentCoefficientPlotPath) {
        setCoefficientTrendPreviewLoading(false);
        setCoefficientTrendPreviewUrl(null);
        return;
      }

      const cached = trendPreviewCacheRef.current[currentCoefficientPlotPath];
      if (cached) {
        setCoefficientTrendPreviewLoading(false);
        setCoefficientTrendPreviewUrl(cached);
        return;
      }

      setCoefficientTrendPreviewLoading(true);
      try {
        const preview = await getArtifactImagePreview(
          currentCoefficientPlotPath,
          trendAnalysisResult.output_dir,
        );
        if (cancelled) {
          return;
        }
        trendPreviewCacheRef.current[currentCoefficientPlotPath] = preview.dataUrl;
        setCoefficientTrendPreviewUrl(preview.dataUrl);
      } catch (error) {
        if (!cancelled) {
          setCoefficientTrendPreviewUrl(null);
          setTrendAnalysisError(
            error instanceof Error ? error.message : "Unable to load image preview.",
          );
        }
      } finally {
        if (!cancelled) {
          setCoefficientTrendPreviewLoading(false);
        }
      }
    };

    if (activeTrendPreview === "coefficient") {
      void loadPreview();
      return () => {
        cancelled = true;
      };
    }

    setCoefficientTrendPreviewLoading(false);
    setCoefficientTrendPreviewUrl(null);

    return () => {
      cancelled = true;
    };
  }, [activeTrendPreview, currentCoefficientPlotPath, trendAnalysisResult?.output_dir]);

  const resetModelSetupState = () => {
    setModelSetupResult(null);
    setProgressDatasetId(null);
    setProgressReferenceModelId(null);
    setProgressReferenceFitPooled(null);
    setProgressLatestProgressRowId(null);
    setRunResult(null);
  };

  const handleTrendAnalysisRun = async () => {
    if (!outputDirectory || trendAnalysisRunning) {
      return;
    }

    const parsedLimit = Number(trendLimitInput);
    const parsedWindow = Number(trendWindowInput);
    if (!Number.isFinite(parsedLimit) || parsedLimit <= 0) {
      setTrendAnalysisError("Enter a valid positive number of latest runs to analyze.");
      setTrendAnalysisMessage(null);
      return;
    }
    if (!Number.isFinite(parsedWindow) || parsedWindow <= 0) {
      setTrendAnalysisError("Enter a valid positive rolling window size.");
      setTrendAnalysisMessage(null);
      return;
    }

    setTrendAnalysisRunning(true);
    setTrendAnalysisError(null);
    setTrendAnalysisMessage("Running trend analysis...");

    try {
      const clampedLimit =
        typeof activeTrendDatasetRunCount === "number" &&
        Number.isFinite(activeTrendDatasetRunCount) &&
        activeTrendDatasetRunCount > 0
          ? Math.min(Math.trunc(parsedLimit), activeTrendDatasetRunCount)
          : Math.trunc(parsedLimit);
      if (clampedLimit !== Math.trunc(parsedLimit)) {
        setTrendLimitInput(String(clampedLimit));
      }
      const result = await runTrendAnalysis(outputDirectory, {
        datasetId: selectedTrendDatasetId,
        limit: clampedLimit,
        window: Math.trunc(parsedWindow),
      });
      trendPreviewCacheRef.current = {};
      setParameterTrendPageIndex(0);
      setCoefficientTrendPageIndex(0);
      setFitTrendPreviewUrl(null);
      setParameterTrendPreviewUrl(null);
      setCoefficientTrendPreviewUrl(null);
      setTrendAnalysisResult(result);
      setTrendAnalysisMessage("Trend analysis completed successfully.");
    } catch (error) {
      setTrendAnalysisError(
        error instanceof Error ? error.message : "Trend analysis failed.",
      );
      setTrendAnalysisMessage(null);
    } finally {
      setTrendAnalysisRunning(false);
    }
  };

  const handleOpenArtifact = async (targetPath: string) => {
    try {
      await openArtifactPath(targetPath);
      setTrendAnalysisError(null);
    } catch (error) {
      setTrendAnalysisError(
        error instanceof Error ? error.message : "Unable to open analysis artifact.",
      );
    }
  };

  const cycleArtifactRetentionMode = () => {
    setArtifactRetentionMode((currentMode) => {
      const currentIndex = ARTIFACT_RETENTION_ORDER.indexOf(currentMode);
      const nextIndex =
        currentIndex >= 0
          ? (currentIndex + 1) % ARTIFACT_RETENTION_ORDER.length
          : 0;
      return ARTIFACT_RETENTION_ORDER[nextIndex] ?? "none";
    });
  };

  const openTrendPreview = (type: TrendPreviewType) => {
    setTrendAnalysisError(null);
    setActiveTrendPreview(type);
  };

  const closeTrendPreview = () => {
    setActiveTrendPreview(null);
  };

  const handleParameterTrendPageStep = (delta: number) => {
    setParameterTrendPageIndex((current) =>
      clampPageIndex(
        current + delta,
        trendAnalysisResult?.parameter_plot_paths.length ?? 0,
      ),
    );
  };

  const handleCoefficientTrendPageStep = (delta: number) => {
    setCoefficientTrendPageIndex((current) =>
      clampPageIndex(
        current + delta,
        trendAnalysisResult?.coefficient_plot_paths.length ?? 0,
      ),
    );
  };

  useEffect(() => {
    if (!activeTrendPreview) {
      return;
    }

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setActiveTrendPreview(null);
      }
    };

    window.addEventListener("keydown", handleKeyDown);
    return () => {
      window.removeEventListener("keydown", handleKeyDown);
    };
  }, [activeTrendPreview]);

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

    if (!validatedProfileId || validatedProfileId <= 0) {
      const message =
        "Select a valid input profile before running model setup.";
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
        inputProfileId: validatedProfileId,
        outputFolder: outputDirectory ?? null,
        artifactRetentionMode,
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
    updateStageStatus("ml_driver", "info", "Starting ML Optimization run...");
    setRunning(true);
    onOpenMLPage();

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
        inputProfileId: validatedProfileId,
        artifactRetentionMode,
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
      onBackToSetup();
    } finally {
      if (!shouldKeepRunning) {
        setRunning(false);
      }
      await refreshMLJobStatus();
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
  const stopButtonDisabled = !running || stopRequested;
  const stopButtonLabel = stopRequested
    ? "Stopping after current run..."
    : "Stop After Current Run";
  const latestBestFit =
    [...progressTrials]
      .reverse()
      .find((point) => typeof point.bestSoFar === "number")?.bestSoFar ?? null;
  const progressEmptyMessage = progressDatasetId
    ? `No runs found for dataset ${progressDatasetId} yet. Previous runs will appear here when they share this exact dataset.`
    : "No runs found for the current dataset yet.";
  const outputDirectoryDisplay = outputDirectory ?? "No folder selected";
  const trendSummary = trendAnalysisResult?.summary ?? null;
  const latestTrendDatasetLabel = trendLatestDatasetId ?? progressDatasetId ?? null;
  const trendLatestRunsHelper =
    typeof activeTrendDatasetRunCount === "number" && activeTrendDatasetRunCount > 0
      ? `Latest Runs defaults to and is capped at ${activeTrendDatasetRunCount} for the ${
          selectedTrendDatasetId ? "selected" : "latest"
        } dataset.`
      : "Latest Runs defaults to the total available runs for the selected or latest dataset.";
  const parameterPageCount = trendAnalysisResult?.parameter_plot_paths.length ?? 0;
  const coefficientPageCount = trendAnalysisResult?.coefficient_plot_paths.length ?? 0;
  const currentParameterPage = parameterPageCount > 0
    ? clampPageIndex(parameterTrendPageIndex, parameterPageCount) + 1
    : 0;
  const currentCoefficientPage = coefficientPageCount > 0
    ? clampPageIndex(coefficientTrendPageIndex, coefficientPageCount) + 1
    : 0;
  const activeTrendPreviewTitle =
    activeTrendPreview === "fit"
      ? "Fit Metric Trend"
      : activeTrendPreview === "parameter"
        ? "Parameter Trend"
        : activeTrendPreview === "coefficient"
          ? "Coefficient Trend"
          : "";
  const activeTrendPreviewSubtitle =
    activeTrendPreview === "fit"
      ? "Latest fit trend for the analyzed dataset."
      : activeTrendPreview === "parameter"
        ? parameterPageCount > 0
          ? `Page ${currentParameterPage} of ${parameterPageCount}`
          : "No parameter trend pages were generated."
        : activeTrendPreview === "coefficient"
          ? coefficientPageCount > 0
            ? `Page ${currentCoefficientPage} of ${coefficientPageCount}`
            : "No coefficient trend pages were generated."
          : "";
  const activeTrendPreviewUrl =
    activeTrendPreview === "fit"
      ? fitTrendPreviewUrl
      : activeTrendPreview === "parameter"
        ? parameterTrendPreviewUrl
        : activeTrendPreview === "coefficient"
          ? coefficientTrendPreviewUrl
          : null;
  const activeTrendPreviewLoading =
    activeTrendPreview === "fit"
      ? fitTrendPreviewLoading
      : activeTrendPreview === "parameter"
        ? parameterTrendPreviewLoading
        : activeTrendPreview === "coefficient"
          ? coefficientTrendPreviewLoading
          : false;
  const activeTrendPreviewPath =
    activeTrendPreview === "fit"
      ? trendAnalysisResult?.plot_path ?? null
      : activeTrendPreview === "parameter"
        ? currentParameterPlotPath
        : activeTrendPreview === "coefficient"
          ? currentCoefficientPlotPath
          : null;
  const trendAnalysisPanel = (
    <div className="trend-analysis-panel">
      <div className="trend-analysis-header">
        <h3 className="modal-subtitle">Trend Analysis</h3>
        <p className="trend-analysis-description">
          Analyze run history at any point during tuning and review the generated summary and trend plots here.
        </p>
      </div>
      <div className="trend-analysis-controls">
        <label className="label trend-analysis-field">
          Dataset ID Override
            <input
              type="text"
              list={trendDatasetInputId}
              className="path-input"
              value={trendDatasetOverride}
            onChange={(event) => setTrendDatasetOverride(event.target.value)}
            placeholder={latestTrendDatasetLabel ?? "Latest dataset"}
            disabled={trendAnalysisRunning || !trendAnalysisAvailable}
          />
          <datalist id={trendDatasetInputId}>
            {filteredTrendDatasetOptions.map((datasetId) => (
              <option key={datasetId} value={datasetId} />
            ))}
          </datalist>
        </label>
        <label className="label trend-analysis-field trend-analysis-field-small">
          Latest Runs
            <input
              type="number"
              min={1}
              max={
                typeof activeTrendDatasetRunCount === "number" && activeTrendDatasetRunCount > 0
                  ? activeTrendDatasetRunCount
                  : undefined
              }
              className="path-input"
              value={trendLimitInput}
              onChange={(event) => setTrendLimitInput(event.target.value)}
              disabled={trendAnalysisRunning || !trendAnalysisAvailable}
            />
        </label>
        <label className="label trend-analysis-field trend-analysis-field-small">
          Rolling Window
          <input
            type="number"
            min={1}
            className="path-input"
            value={trendWindowInput}
            onChange={(event) => setTrendWindowInput(event.target.value)}
            disabled={trendAnalysisRunning || !trendAnalysisAvailable}
          />
        </label>
        <button
          type="button"
          className="button trend-analysis-run"
          onClick={handleTrendAnalysisRun}
          disabled={
            !outputDirectory || trendAnalysisRunning || !trendAnalysisAvailable
          }
        >
          {trendAnalysisRunning ? "Analyzing..." : "Run Trend Analysis"}
        </button>
        </div>
        <div className="trend-analysis-helper-row">
          <span className="trend-analysis-helper">
            {selectedTrendDatasetId
              ? "Using selected dataset override."
              : latestTrendDatasetLabel
                ? `Using latest dataset by default: ${latestTrendDatasetLabel}`
                : "Leave Dataset ID Override blank to use the latest dataset."}
          </span>
          <span className="trend-analysis-helper">{trendLatestRunsHelper}</span>
          {trendDatasetOptionsLoading ? (
            <span className="trend-analysis-helper">Loading dataset suggestions...</span>
          ) : null}
        </div>
      {trendDatasetOptionsError ? (
        <div className="progress-text error">{trendDatasetOptionsError}</div>
      ) : null}
      {trendAnalysisAvailabilityMessage ? (
        <div className="progress-text error">{trendAnalysisAvailabilityMessage}</div>
      ) : null}
      {trendAnalysisMessage ? (
        <div className="progress-text">{trendAnalysisMessage}</div>
      ) : null}
      {trendAnalysisError ? (
        <div className="progress-text error">{trendAnalysisError}</div>
      ) : null}
      {trendAnalysisResult && trendSummary ? (
        <div className="trend-analysis-results">
          <div className="trend-analysis-summary">
            <div className="trend-analysis-summary-rows">
              <div className="trend-analysis-summary-row trend-analysis-summary-row-dataset">
                <span className="trend-analysis-summary-item">
                  <strong>Dataset:</strong> {trendAnalysisResult.dataset_id ?? "N/A"}
                </span>
              </div>
              <div className="trend-analysis-summary-row">
                <span className="trend-analysis-summary-item">
                  <strong>Run range:</strong> {trendSummary.latest_slice_run_start}-{trendSummary.latest_slice_run_end}
                </span>
                <span className="trend-analysis-summary-item">
                  <strong>Best run:</strong>{" "}
                  {formatTrendBestReferenceInline(trendSummary)}
                  {" "}
                  (
                  best fit:{" "}
                  {typeof trendSummary.best_fit === "number"
                    ? trendSummary.best_fit.toFixed(4)
                    : "N/A"}
                  )
                </span>
              </div>
              <div className="trend-analysis-summary-row">
                <span className="trend-analysis-summary-item">
                  <strong>Best run model id:</strong>{" "}
                  {trendSummary.best_model_id ?? "N/A"}
                </span>
              </div>
              <div className="trend-analysis-summary-row">
                <span className="trend-analysis-summary-item">
                  <strong>Parameters:</strong> {trendAnalysisResult.parameter_count}
                </span>
                <span className="trend-analysis-summary-item">
                  <strong>Coefficients:</strong> {trendAnalysisResult.coefficient_count}
                </span>
                <span className="trend-analysis-summary-item">
                  <strong>Output variables:</strong> {trendAnalysisResult.output_variable_count}
                </span>
              </div>
            </div>
          </div>
          <div className="trend-analysis-actions">
            <button
              type="button"
              className="button secondary"
              onClick={() => handleOpenArtifact(trendAnalysisResult.summary_path)}
            >
              Analysis Summary
            </button>
            <button
              type="button"
              className="button secondary"
              onClick={() => handleOpenArtifact(trendAnalysisResult.output_dir)}
            >
              Analysis Save Folder
            </button>
            <button
              type="button"
              className="button secondary"
              onClick={() => openTrendPreview("fit")}
            >
              Fit Metric Trend
            </button>
            <button
              type="button"
              className="button secondary"
              onClick={() => openTrendPreview("parameter")}
              disabled={parameterPageCount === 0}
            >
              Parameter Trend
            </button>
            <button
              type="button"
              className="button secondary"
              onClick={() => openTrendPreview("coefficient")}
              disabled={coefficientPageCount === 0}
            >
              Coefficient Trend
            </button>
          </div>
        </div>
      ) : null}
    </div>
  );

  const setupStatusMessage = running ? null : statusMessage;
  const setupStatusLevel = running ? "info" : statusLevel;
  const setupSummary = modelSetupResult ? (
    <div className="metadata-inline">
      <ul>
        {modelSetupResult.model_id ? (
          <li>
            <strong>Model ID:</strong> {modelSetupResult.model_id}
          </li>
        ) : null}
        {typeof modelSetupResult.ifs_id === "number" ? (
          <li>
            <strong>IFs ID:</strong> {modelSetupResult.ifs_id}
          </li>
        ) : null}
        {modelSetupResult.dataset_id ? (
          <li>
            <strong>Dataset ID:</strong> {modelSetupResult.dataset_id}
          </li>
        ) : null}
        {modelSetupResult.retained_artifact_dir ? (
          <li>
            <strong>Artifact folder:</strong> {modelSetupResult.retained_artifact_dir}
          </li>
        ) : null}
        {modelSetupResult.dataset_warning ? (
          <li>
            <strong>Dataset note:</strong> {modelSetupResult.dataset_warning}
          </li>
        ) : null}
      </ul>
    </div>
  ) : null;

  return (
    <section className="tune-container">
      <div className="tune-header">
        <p className="tune-description">
          {isSetupPage
            ? "Create or open an input profile, configure model setup, and prepare the run before starting ML optimization."
            : "Monitor the ML optimization run, review progress, and analyze trend outputs in one place."}
        </p>
        <div className="tune-meta" aria-label="Tuning configuration summary">
          <div className="tune-meta-row">
            {effectiveBaseYear != null ? (
              <div className="tune-meta-item tune-meta-item-base">
                <span className="tune-meta-label">Base year:</span>
                <span className="tune-meta-value">{effectiveBaseYear}</span>
              </div>
              ) : null}
              <div className="tune-meta-item tune-meta-item-path" title={outputDirectoryDisplay}>
                <span className="tune-meta-label">Output folder:</span>
                <OverflowAwareMiddleTruncate value={outputDirectoryDisplay} title={outputDirectoryDisplay} />
              </div>
              <div className="tune-meta-item tune-meta-item-retention">
                <span className="tune-meta-label">Artifacts:</span>
                <button
                  type="button"
                  className="tune-retention-toggle"
                  onClick={cycleArtifactRetentionMode}
                  disabled={modelSetupRunning || running || isMLPage}
                  title="Cycle model artifact retention mode"
                >
                  {ARTIFACT_RETENTION_LABELS[artifactRetentionMode]}
                </button>
              </div>
            </div>
          </div>
      </div>

      {isSetupPage ? (
        <>
          <div className="tune-controls">
            <div className="end-year-control">
              <div className="end-year-inputs">
                <div className="end-year-entry-row">
                  <label className="label end-year-label" htmlFor="end-year-input">
                    End Year
                  </label>
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
              title={runButtonLabel}
            >
              Start ML Optimization
            </button>
            <button
              type="button"
              className="button secondary"
              onClick={onBackToValidation}
              disabled={running}
            >
              Back to Validation
            </button>
          </div>

          <div className="progress-wrapper">
            {setupStatusMessage ? (
              <div className={`progress-text ${setupStatusLevel}`}>
                {setupStatusMessage}
              </div>
            ) : null}
            {error && <div className="progress-text error">{error}</div>}
            {setupSummary}
          </div>
        </>
      ) : (
        <>
          <div className="tune-actions">
            <button
              type="button"
              className="button"
              onClick={handleRunClick}
              disabled={stopButtonDisabled}
            >
              {stopButtonLabel}
            </button>
            <button
              type="button"
              className="button secondary"
              onClick={onBackToSetup}
              disabled={running}
            >
              Back to Model Setup
            </button>
            <button
              type="button"
              className="button secondary"
              onClick={onBackToValidation}
              disabled={running}
            >
              Back to Validation
            </button>
          </div>

          <div className="progress-wrapper">
            {running ? (
              runProgressLabel ? (
                <div className="progress-text">{runProgressLabel}</div>
              ) : null
            ) : statusMessage ? (
              <div className={`progress-text ${statusLevel}`}>
                {statusMessage}
              </div>
            ) : null}

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
                  {runResult.model_id ? (
                    <li>
                      <strong>Model ID:</strong> {runResult.model_id}
                    </li>
                  ) : null}
                  {typeof runResult.ifs_id === "number" ? (
                    <li>
                      <strong>IFs ID:</strong> {runResult.ifs_id}
                    </li>
                  ) : null}
                  {runResult.best_model_id ? (
                    <li>
                      <strong>Best model ID:</strong> {runResult.best_model_id}
                    </li>
                  ) : null}
                  {typeof runResult.best_fit_pooled === "number" ? (
                    <li>
                      <strong>Best pooled fit:</strong>{" "}
                      {runResult.best_fit_pooled.toFixed(4)}
                    </li>
                  ) : null}
                  {typeof runResult.iterations === "number" ? (
                    <li>
                      <strong>Iterations:</strong> {runResult.iterations}
                    </li>
                  ) : null}
                  {wgdDisplay ? (
                    <li>
                      <strong>World GDP (WGDP):</strong> {wgdDisplay}
                    </li>
                  ) : null}
                  {runResult.run_folder ? (
                    <li>
                      <strong>Run folder:</strong> {runResult.run_folder}
                    </li>
                  ) : null}
                  {runResult.output_file ? (
                    <li>
                      <strong>Run database:</strong> {runResult.output_file}
                    </li>
                  ) : null}
                </ul>
              </div>
            ) : (
              setupSummary
            )}
          </div>

          <div className="ml-lower-panel">
            <div className="ml-progress-panel">
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
                  rollingWindow={rollingWindow}
                  rollingWindowInput={rollingWindowInput}
                  onRollingWindowInputChange={onRollingWindowInputChange}
                />
              )}
            </div>
          </div>

          {trendAnalysisPanel}
        </>
      )}

      {activeTrendPreview ? (
        <div
          className="modal-backdrop"
          onClick={closeTrendPreview}
          role="presentation"
        >
          <div
            className="modal-content ml-progress-modal trend-preview-modal"
            onClick={(event) => event.stopPropagation()}
            role="dialog"
            aria-modal="true"
            aria-label={activeTrendPreviewTitle}
          >
            <div className="trend-preview-modal-header">
              <div>
                <h3 className="modal-title">{activeTrendPreviewTitle}</h3>
                <p className="modal-subtitle trend-preview-modal-subtitle">
                  {activeTrendPreviewSubtitle}
                </p>
              </div>
              <div className="trend-analysis-page-controls trend-preview-modal-actions">
                {activeTrendPreview === "parameter" ? (
                  <>
                    <button
                      type="button"
                      className="button secondary trend-analysis-page-button"
                      onClick={() => handleParameterTrendPageStep(-1)}
                      disabled={parameterPageCount <= 1 || currentParameterPage <= 1}
                    >
                      Previous
                    </button>
                    <button
                      type="button"
                      className="button secondary trend-analysis-page-button"
                      onClick={() => activeTrendPreviewPath && handleOpenArtifact(activeTrendPreviewPath)}
                      disabled={!activeTrendPreviewPath}
                    >
                      Open Current Page
                    </button>
                    <button
                      type="button"
                      className="button secondary trend-analysis-page-button"
                      onClick={() => handleParameterTrendPageStep(1)}
                      disabled={parameterPageCount <= 1 || currentParameterPage >= parameterPageCount}
                    >
                      Next
                    </button>
                  </>
                ) : null}

                {activeTrendPreview === "coefficient" ? (
                  <>
                    <button
                      type="button"
                      className="button secondary trend-analysis-page-button"
                      onClick={() => handleCoefficientTrendPageStep(-1)}
                      disabled={coefficientPageCount <= 1 || currentCoefficientPage <= 1}
                    >
                      Previous
                    </button>
                    <button
                      type="button"
                      className="button secondary trend-analysis-page-button"
                      onClick={() => activeTrendPreviewPath && handleOpenArtifact(activeTrendPreviewPath)}
                      disabled={!activeTrendPreviewPath}
                    >
                      Open Current Page
                    </button>
                    <button
                      type="button"
                      className="button secondary trend-analysis-page-button"
                      onClick={() => handleCoefficientTrendPageStep(1)}
                      disabled={
                        coefficientPageCount <= 1 || currentCoefficientPage >= coefficientPageCount
                      }
                    >
                      Next
                    </button>
                  </>
                ) : null}

                {activeTrendPreview === "fit" ? (
                  <button
                    type="button"
                    className="button secondary trend-analysis-page-button"
                    onClick={() => activeTrendPreviewPath && handleOpenArtifact(activeTrendPreviewPath)}
                    disabled={!activeTrendPreviewPath}
                  >
                    Open File
                  </button>
                ) : null}

                <button
                  type="button"
                  className="button secondary trend-preview-close"
                  onClick={closeTrendPreview}
                >
                  Close
                </button>
              </div>
            </div>

            {activeTrendPreviewLoading ? (
              <div className="trend-analysis-preview-empty trend-preview-modal-body">
                Loading trend preview...
              </div>
            ) : activeTrendPreviewUrl ? (
              <div className="trend-preview-modal-body">
                <img
                  src={activeTrendPreviewUrl}
                  alt={activeTrendPreviewTitle}
                  className="trend-preview-modal-image"
                />
              </div>
            ) : (
              <div className="trend-analysis-preview-empty trend-preview-modal-body">
                No trend preview is available yet.
              </div>
            )}
          </div>
        </div>
      ) : null}

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
  const [selectedProfileId, setSelectedProfileId] = useState<number | null>(null);
  const [lastValidatedProfileId, setLastValidatedProfileId] =
    useState<number | null>(null);
  const [profileDetail, setProfileDetail] = useState<InputProfileDetail | null>(null);
  const [result, setResult] = useState<CheckResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [view, setView] = useState<View>("validate");
  const [vizRollingWindow, setVizRollingWindow] = useState(50);
  const [vizRollingWindowInput, setVizRollingWindowInput] = useState("50");
  const [mlJobStatus, setMLJobStatus] = useState<MLJobStatus | null>(null);
  const [profileEditorOpen, setProfileEditorOpen] = useState(false);
  const [info, setInfo] = useState<string | null>(null);
  const [nativeFolderPickerAvailable, setNativeFolderPickerAvailable] =
    useState<boolean>(() =>
      typeof window !== "undefined" && Boolean(window.electron?.selectFolder),
    );
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
      if (typeof status?.inputProfileId === "number") {
        setSelectedProfileId(status.inputProfileId);
      }
      if (status?.ifsValidated) {
        setLastValidatedIfsFolder(status.ifsPath ?? null);
        setLastValidatedOutputDirectory(status.outputDir ?? null);
        setLastValidatedProfileId(status.inputProfileId ?? null);
      }
      if (status?.ifsValidated && status?.running) {
        setView("ml");
        setProfileEditorOpen(false);
      }
    } catch (err) {
      console.warn("Unable to load ML job status:", err);
    }
  };

  useEffect(() => {
    if (typeof window !== "undefined") {
      setNativeFolderPickerAvailable(Boolean(window.electron?.selectFolder));
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
          setLastValidatedProfileId(null);
          setProfileDetail(null);
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
        inputProfileId: null,
      });
      setIfsFolderPath(trimmedIfsPath);
      setOutputDirectory(
        trimmedOutputPath.length > 0 ? trimmedOutputPath : null,
      );
      setResult(res);

      if (res.valid) {
        setLastValidatedIfsFolder(trimmedIfsPath);
        setLastValidatedOutputDirectory(
          trimmedOutputPath.length > 0 ? trimmedOutputPath : null,
        );
        setLastValidatedProfileId(null);
      } else {
        setLastValidatedIfsFolder(null);
        setLastValidatedOutputDirectory(null);
        setLastValidatedProfileId(null);
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
          setLastValidatedProfileId(null);
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

  const handleModelSetupClick = () => {
    setError(null);
    setInfo(null);

    if (!result?.valid) {
      setError("You must validate an IFs folder first.");
      return;
    }

    setView("setup");
    setProfileEditorOpen(false);
  };

  const handleQuitClick = async () => {
    setError(null);
    setInfo(null);

    try {
      if (window.electron?.invoke) {
        await window.electron.invoke("app:quit");
        return;
      }
    } catch (err) {
      console.warn("Unable to quit through Electron IPC, falling back to window.close().", err);
    }

    if (typeof window !== "undefined" && typeof window.close === "function") {
      window.close();
      return;
    }

    setError("Unable to quit the app right now.");
  };

  const missingFiles = useMemo(() => result?.missingFiles ?? [], [result]);
  const infoMessages = useMemo(() => result?.infoMessages ?? [], [result]);
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
  const pathChecks = result?.pathChecks;
  const ifsFolderCheck = pathChecks?.ifsFolder;
  const outputFolderCheck = pathChecks?.outputFolder;
  const inputProfileCheck = pathChecks?.inputProfile;
  const dbMigration = result?.dbMigration;
  const ifsFolderReady =
    Boolean(ifsFolderCheck?.exists) && (ifsFolderCheck?.readable ?? true);
  const outputFolderReady =
    Boolean(outputFolderCheck?.exists) && outputFolderCheck?.writable === true;
  const inputProfileReady = Boolean(inputProfileCheck?.valid);
  const bigpopaDbDisplayPath = "desktop/output/bigpopa.db";
  const bigpopaDbReady = Boolean(dbMigration);
  const bigpopaDbMessage = !dbMigration
    ? "Validation did not return BIGPOPA working database status."
    : dbMigration?.performed
      ? `Working BIGPOPA database ready. Legacy schema upgraded${
          dbMigration.backup_path ? ` with backup at ${dbMigration.backup_path}.` : "."
        }`
      : typeof dbMigration.message === "string" && dbMigration.message.trim().length > 0
        ? dbMigration.message
        : "Working BIGPOPA database is ready.";
  const canOpenSetup = hasValidResult;
  const windowTitle =
    view === "validate"
      ? "BIGPOPA - IFs Folder Check - Browse to your IFs folder"
      : view === "setup"
        ? "BIGPOPA - Model Setup - Configure profile and runtime settings"
        : "BIGPOPA - ML Monitor - Track optimization progress";

  const handleVizRollingWindowInputChange = (value: string) => {
    setVizRollingWindowInput(value);
    const trimmed = value.trim();
    if (!/^\d+$/.test(trimmed)) {
      return;
    }

    const parsed = Number(trimmed);
    if (Number.isFinite(parsed) && parsed > 0) {
      setVizRollingWindow(parsed);
    }
  };

  useEffect(() => {
    if (typeof document !== "undefined") {
      document.title = windowTitle;
    }
  }, [windowTitle]);

  return (
    <div className="container">
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
            <div className="button-row validation-actions">
              <button type="submit" className="button">
                {loading ? "Validating..." : "Validate"}
              </button>
              <button
                type="button"
                className="button"
                onClick={handleModelSetupClick}
                disabled={!canOpenSetup}
              >
                Model Setup
              </button>
              <button
                type="button"
                className="button secondary"
                onClick={handleQuitClick}
              >
                Quit
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

              {infoMessages.length > 0 && (
                <div className="requirements">
                  <h3>Info</h3>
                  <ul>
                    {infoMessages.map((message) => (
                      <li key={message} className="item success">
                        <span className="icon">OK</span>
                        <span>{message}</span>
                      </li>
                    ))}
                  </ul>
                </div>
              )}

              {(ifsFolderCheck || outputFolderCheck || inputProfileCheck || dbMigration) && (
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

                  <div
                    className={`summary-line ${
                      bigpopaDbReady ? "success" : "error"
                    }`}
                  >
                    <span className="summary-label">
                      {bigpopaDbReady
                        ? "BIGPOPA database ready:"
                        : "BIGPOPA database pending:"}
                    </span>
                    {bigpopaDbDisplayPath ? (
                      <span className="summary-value">{bigpopaDbDisplayPath}</span>
                    ) : null}
                    <span className="summary-message">{bigpopaDbMessage}</span>
                  </div>

                  {inputProfileCheck && result?.profileReady && (
                    <div
                      className={`summary-line ${
                        inputProfileReady ? "success" : "error"
                      }`}
                    >
                      <span className="summary-label">
                        {inputProfileReady
                          ? "Input profile ready:"
                          : "Input profile pending:"}
                      </span>
                      {inputProfileCheck.displayPath && (
                        <span className="summary-value">
                          {inputProfileCheck.displayPath}
                        </span>
                      )}
                      {inputProfileCheck.message && (
                        <span className="summary-message">
                          {inputProfileCheck.message}
                        </span>
                      )}
                      {inputProfileCheck.errors?.length ? (
                        <span className="summary-message">
                          {inputProfileCheck.errors.join(" ")}
                        </span>
                      ) : null}
                    </div>
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

      {(view === "setup" || view === "ml") &&
      (result?.valid || mlJobStatus?.running || mlJobStatus?.ifsValidated) ? (
        <>
          {view === "setup" ? (
            <InputProfilesPanel
              key="profiles"
              ifsRoot={ifsFolderPath}
              outputDirectory={outputDirectory}
              ifsStaticId={result?.ifs_static_id ?? null}
              selectedProfileId={selectedProfileId}
              onSelectedProfileIdChange={setSelectedProfileId}
              onProfileDetailChange={setProfileDetail}
              onEditorActiveChange={setProfileEditorOpen}
            />
          ) : null}
          {(view === "ml" || !profileEditorOpen) ? (
            <TuneIFsPage
              key="workflow"
              pageMode={view === "setup" ? "setup" : "ml"}
              onBackToValidation={() => {
                setView("validate");
                setProfileEditorOpen(false);
              }}
              onBackToSetup={() => {
                setView("setup");
                setProfileEditorOpen(false);
              }}
              onOpenMLPage={() => {
                setView("ml");
                setProfileEditorOpen(false);
              }}
              onMLJobStatusRefresh={refreshMLJobStatus}
              validatedPath={
                lastValidatedIfsFolder ?? ifsFolderPath?.trim() ?? ""
              }
              validatedProfileId={
                profileDetail?.validation.valid
                  ? selectedProfileId ?? lastValidatedProfileId ?? null
                  : null
              }
              baseYear={result?.base_year ?? mlJobStatus?.runConfig?.baseYear ?? null}
              outputDirectory={outputDirectory ?? mlJobStatus?.outputDir ?? null}
              rollingWindow={vizRollingWindow}
              rollingWindowInput={vizRollingWindowInput}
              onRollingWindowInputChange={handleVizRollingWindowInputChange}
              initialMLJobRunning={Boolean(mlJobStatus?.running)}
              initialMLJobProgress={mlJobStatus?.progress?.text ?? null}
              initialRunConfig={mlJobStatus?.runConfig ?? null}
              initialStopRequested={mlJobStatus?.stopRequested ?? false}
              initialStopAcknowledged={mlJobStatus?.stopAcknowledged ?? false}
              initialFinalResult={mlJobStatus?.finalResult ?? null}
              initialTerminationReason={mlJobStatus?.terminationReason ?? null}
            />
          ) : null}
        </>
      ) : null}

      {(view === "setup" || view === "ml") &&
      !result?.valid &&
      !mlJobStatus?.running &&
      !mlJobStatus?.ifsValidated ? (
        <div className="alert alert-error">
          <p className="alert-message">
            Validation is required before continuing to model setup or ML monitoring.
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
      ) : null}
    </div>
  );
}

export default App;
