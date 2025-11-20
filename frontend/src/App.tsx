import {
  ChangeEvent,
  FormEvent,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import {
  modelSetup,
  runML,
  subscribeToIFsProgress,
  validateIFsFolder,
  StageError,
  type ApiStage,
  type ApiStatus,
  type CheckResponse,
  type IFsProgressEvent,
  type ModelSetupData,
  type MLDriverData,
} from "./api";

const REQUIRED_INPUT_SHEETS = ["AnalFunc", "TablFunc", "IFsVar", "DataDict"];

type View = "validate" | "tune";

type StatusLevel = "info" | "success" | "error";

type LogStatus = ApiStatus | "info";

type LogEntry = {
  id: number;
  stage: ApiStage;
  message: string;
  status: LogStatus;
};

type TuneIFsPageProps = {
  onBack: () => void;
  validatedPath: string;
  validatedInputPath: string;
  baseYear?: number | null;
  outputDirectory: string | null;
  requestOutputDirectory: () => Promise<string | null>;
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
  validatedPath,
  validatedInputPath,
  baseYear,
  outputDirectory,
  requestOutputDirectory,
}: TuneIFsPageProps) {
  const DEFAULT_END_YEAR = 2050;
  const MAX_END_YEAR = 2150;
  const FALLBACK_MIN_END_YEAR = 1900;

  const [endYearInput, setEndYearInput] = useState("2050");
  const [endYear, setEndYear] = useState<number>(DEFAULT_END_YEAR);
  const [running, setRunning] = useState(false);
  const [modelSetupRunning, setModelSetupRunning] = useState(false);
  const [modelSetupResult, setModelSetupResult] =
    useState<ModelSetupData | null>(null);
  const [progressYear, setProgressYear] = useState<number | null>(null);
  const [progressPercent, setProgressPercent] = useState(0);
  const [runResult, setRunResult] = useState<MLDriverData | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [statusMessage, setStatusMessage] = useState("Waiting to start.");
  const [statusLevel, setStatusLevel] = useState<StatusLevel>("info");
  const [logEntries, setLogEntries] = useState<LogEntry[]>([]);
  const [mlLogEntries, setMLLogEntries] = useState<string[]>([]);
  const [effectiveBaseYear, setEffectiveBaseYear] = useState<number | null>(
    typeof baseYear === "number" && Number.isFinite(baseYear) ? baseYear : null,
  );
  const baseYearRef = useRef<number | null>(baseYear ?? null);
  const targetEndYearRef = useRef<number | null>(DEFAULT_END_YEAR);
  const parameterRef = useRef<Record<string, unknown>>({});
  const coefficientRef = useRef<Record<string, unknown>>({});
  const paramDimensionRef = useRef<Record<string, unknown>>({});
  const logIdRef = useRef(0);

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
        return "IFs run completed successfully.";
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
    setStatusMessage("Waiting to start.");
    setStatusLevel("info");
    setModelSetupResult(null);
    setRunResult(null);
    setLogEntries([]);
    logIdRef.current = 0;
    setProgressYear(null);
    setProgressPercent(0);
    setError(null);
  }, [validatedPath, validatedInputPath, outputDirectory]);

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
    if (!window.electron?.onMLProgress) {
      return;
    }

    const unsubscribe = window.electron.onMLProgress((line: string) => {
      setMLLogEntries((prev) => [...prev, line]);
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

  const resetModelSetupState = () => {
    setModelSetupResult(null);
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

  const handleChangeOutputDirectory = async () => {
    if (running || modelSetupRunning) {
      return;
    }

    try {
      setError(null);
      await requestOutputDirectory();
      resetModelSetupState();
    } catch (err) {
      const message =
        err instanceof Error
          ? err.message
          : "Unable to update the output directory.";
      setError(message);
    }
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
      const successMessage = resolveSuccessMessage(
        response.stage,
        response.message,
      );
      updateStageStatus(response.stage, "success", successMessage);
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
    setError(null);
    setRunResult(null);
    setProgressYear(null);
    setProgressPercent(0);
    setMLLogEntries([]);

    const parsedEndYear = Number(endYearInput);
    if (!Number.isFinite(parsedEndYear) || parsedEndYear <= 0) {
      const message = "Please enter a valid end year.";
      updateStageStatus("ml_driver", "error", message);
      setError(message);
      return;
    }

    if (!outputDirectory) {
      const message = "Please choose an output folder before running IFs.";
      updateStageStatus("ml_driver", "error", message);
      setError(message);
      return;
    }

    if (!modelSetupResult || modelSetupRunning) {
      const message = "Please run model setup before starting IFs.";
      updateStageStatus("ml_driver", "error", message);
      setError(message);
      return;
    }

    const clampedEndYear = clampEndYear(parsedEndYear);
    targetEndYearRef.current = clampedEndYear;
    setEndYear(clampedEndYear);
    setEndYearInput(String(clampedEndYear));
    setModelSetupResult(null);
    updateStageStatus("ml_driver", "info", "Starting IFs run...");
    setRunning(true);

    try {
      appendLog("ml_driver", "info", "Submitting IFs run request.");

      const response = await runML({
        validatedPath,
        endYear: clampedEndYear,
        baseYear: baseYearRef.current,
        outputDirectory,
        modelId: modelSetupResult.model_id,
        ifsId: modelSetupResult.ifs_id,
      });

      setError(null);

      const successMessage = resolveSuccessMessage(
        response.stage,
        response.message,
      );

      setRunResult(response.data);
      updateStageStatus(response.stage, "success", successMessage);

      const endYearFromResponse =
        typeof response.data.end_year === "number"
          ? response.data.end_year
          : clampedEndYear;
      setProgressYear(endYearFromResponse);
      setProgressPercent(100);
      targetEndYearRef.current = endYearFromResponse;

      if (typeof response.data.base_year === "number") {
        baseYearRef.current = response.data.base_year;
        setEffectiveBaseYear(response.data.base_year);
      }
    } catch (err) {
      const { stage, message } = resolveStageError(err, "ml_driver");
      setError(message);
      setRunResult(null);
      updateStageStatus(stage, "error", message);
    } finally {
      setRunning(false);
    }
  };

  const displayPercent = Math.min(100, Math.max(0, progressPercent));
  const formattedPercent = `${displayPercent.toFixed(1)}%`;

  const runProgressLabel = running
    ? progressYear != null
      ? `Running IFs… Last reported year: ${progressYear} (${formattedPercent})`
      : "Running IFs…"
    : runResult
    ? null
    : progressYear != null
    ? `Last reported year: ${progressYear} (${formattedPercent})`
    : null;

  const wgdDisplay =
    runResult && typeof runResult.w_gdp === "number"
      ? runResult.w_gdp.toLocaleString(undefined, { maximumFractionDigits: 2 })
      : null;

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

      <div className="tune-actions four-buttons">
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
          disabled={
            running ||
            modelSetupRunning ||
            !outputDirectory ||
            !modelSetupResult
          }
        >
          {running ? "Running..." : "Run IFs"}
        </button>
        <button
          type="button"
          className="button secondary"
          onClick={handleChangeOutputDirectory}
          disabled={running || modelSetupRunning}
        >
          Change output folder
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

        <progress
          className="progress-indicator"
          max={100}
          value={displayPercent}
        />

        {runResult ? (
          <div className="metadata-inline">
            <div className="run-status success">Run completed</div>
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
                  <strong>Best pooled fit (MSE):</strong>{" "}
                  {runResult.best_fit_pooled.toFixed(4)}
                </li>
              )}
              {typeof runResult.iterations === "number" && (
                <li>
                  <strong>Iterations:</strong> {runResult.iterations}
                </li>
              )}
              {typeof runResult.base_year === "number" && (
                <li>
                  <strong>Base year:</strong> {runResult.base_year}
                </li>
              )}
              {typeof runResult.end_year === "number" && (
                <li>
                  <strong>End year:</strong> {runResult.end_year}
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

      <div className="ml-console">
        <div className="ml-console-title">ML Optimization Log</div>
        <div className="ml-console-body">
          {mlLogEntries.length === 0 ? (
            <div className="progress-text">Waiting for ML output...</div>
          ) : (
            mlLogEntries.map((entry, index) => (
              <div key={`${index}-${entry.slice(0, 12)}`}>{entry}</div>
            ))
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
          console.warn("Default input path was empty — check Electron handler.");
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
        setView("validate");
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
            : "Configure and launch IFs runs with live progress tracking."}
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
                {result.valid ? "Valid ✅" : "Invalid ❌"}
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
                              {name} {present ? "✓" : "✗"}
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
                        <span className="icon">{item.exists ? "✅" : "❌"}</span>
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
                        <span className="icon">❌</span>
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

      {view === "tune" && result?.valid && (
        <TuneIFsPage
          onBack={() => setView("validate")}
          validatedPath={
            lastValidatedIfsFolder ?? ifsFolderPath?.trim() ?? ""
          }
          validatedInputPath={
            lastValidatedInputFile ?? inputFilePath?.trim() ?? ""
          }
          baseYear={result?.base_year}
          outputDirectory={outputDirectory}
          requestOutputDirectory={requestOutputDirectory}
        />
      )}

      {view === "tune" && !result?.valid && (
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
