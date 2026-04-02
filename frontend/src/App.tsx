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
import {
  MLProgressChart,
  appendNormalizedProgressTrials,
  normalizeProgressTrials,
  type ChartPoint,
} from "./MLProgressChart";

const REQUIRED_INPUT_SHEETS = ["AnalFunc", "TablFunc", "IFsVar", "DataDict"];

type View = "validate" | "tune";

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
  return typeof trial.progress_rowid === "number" &&
    Number.isFinite(trial.progress_rowid) &&
    trial.progress_rowid > 0
    ? trial.progress_rowid
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
  onBack: () => void;
  onMLJobStatusRefresh?: () => Promise<void>;
  validatedPath: string;
  validatedInputPath: string;
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
  onBack,
  onMLJobStatusRefresh,
  validatedPath,
  validatedInputPath,
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
  const [effectiveBaseYear, setEffectiveBaseYear] = useState<number | null>(
    typeof baseYear === "number" && Number.isFinite(baseYear) ? baseYear : null,
  );
  const baseYearRef = useRef<number | null>(baseYear ?? null);
  const targetEndYearRef = useRef<number | null>(normalizedInitialEndYear);
  const parameterRef = useRef<Record<string, unknown>>({});
  const coefficientRef = useRef<Record<string, unknown>>({});
  const paramDimensionRef = useRef<Record<string, unknown>>({});
  const progressTrialsRef = useRef<ChartPoint[]>([]);
  const progressLatestProgressRowIdRef = useRef<number | null>(null);
  const logIdRef = useRef(0);
  const previousInitialMLJobRunningRef = useRef(Boolean(initialMLJobRunning));

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
    setLogEntries([]);
    logIdRef.current = 0;
    setProgressYear(null);
    setProgressPercent(0);
    setError(null);
  }, [
    validatedPath,
    validatedInputPath,
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
          typeof history.latest_progress_rowid === "number" &&
          Number.isFinite(history.latest_progress_rowid)
            ? history.latest_progress_rowid
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
          typeof history.latest_progress_rowid === "number" &&
            Number.isFinite(history.latest_progress_rowid)
            ? history.latest_progress_rowid
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

  const resetModelSetupState = () => {
    setModelSetupResult(null);
    setProgressDatasetId(null);
    setProgressReferenceModelId(null);
    setProgressReferenceFitPooled(null);
    setProgressLatestProgressRowId(null);
    setRunResult(null);
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
  const outputDirectoryDisplay = outputDirectory ?? "No folder selected";

  return (
    <section className="tune-container">
      <div className="tune-header">
        <p className="tune-description">
          Launch an IFs simulation and monitor its progress in real time.
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
            </div>
          </div>
      </div>

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
  const [vizRollingWindow, setVizRollingWindow] = useState(50);
  const [vizRollingWindowInput, setVizRollingWindowInput] = useState("50");
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
  const windowTitle =
    view === "validate"
      ? "BIGPOPA - IFs Folder Check - Browse to your IFs folder"
      : "BIGPOPA - Tune IFs - Configure and run ML optimization";

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
