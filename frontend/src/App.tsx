import {
  ChangeEvent,
  FormEvent,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import {
  runIFs,
  subscribeToIFsProgress,
  validateIFsFolder,
  type CheckResponse,
  type IFsProgressEvent,
  type RunIFsSuccess,
} from "./api";

const REQUIRED_INPUT_SHEETS = ["AnalFunc", "TablFunc", "IFsVar", "DataDict"];
const DEFAULT_INPUT_FILE = "./input/StartingPointTable.xlsx";

type View = "validate" | "tune";

type RunConfigModalProps = {
  isOpen: boolean;
  onClose: () => void;
  onSubmit: () => void;
  endYearInput: string;
  onEndYearChange: (event: ChangeEvent<HTMLInputElement>) => void;
  running: boolean;
  baseYear: number | null;
};

type TuneIFsPageProps = {
  onBack: () => void;
  validatedPath: string;
  baseYear?: number | null;
  outputDirectory: string | null;
  requestOutputDirectory: () => Promise<string | null>;
  runModalTrigger: number;
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

function RunConfigModal({
  isOpen,
  onClose,
  onSubmit,
  endYearInput,
  onEndYearChange,
  running,
  baseYear,
}: RunConfigModalProps) {
  if (!isOpen) {
    return null;
  }

  return (
    <div className="modal-backdrop" role="dialog" aria-modal="true">
      <div className="modal-content">
        <h3 className="modal-title">Configure IFs Run</h3>
        <p className="modal-subtitle">
          Select the target end year and launch the simulation.
        </p>
        <p className="modal-base-year">
          <strong>Base year:</strong>{" "}
          {typeof baseYear === "number" && Number.isFinite(baseYear)
            ? baseYear
            : "Unknown"}
        </p>
        <label className="label" htmlFor="modal-end-year">
          End Year
        </label>
        <input
          id="modal-end-year"
          type="number"
          className="path-input"
          value={endYearInput}
          onChange={onEndYearChange}
          disabled={running}
          min={baseYear ?? undefined}
        />
        <div className="modal-actions">
          <button
            type="button"
            className="button secondary"
            onClick={onClose}
            disabled={running}
          >
            Cancel
          </button>
          <button
            type="button"
            className="button"
            onClick={onSubmit}
            disabled={running}
          >
            {running ? "Running..." : "Run"}
          </button>
        </div>
      </div>
    </div>
  );
}

function TuneIFsPage({
  onBack,
  validatedPath,
  baseYear,
  outputDirectory,
  requestOutputDirectory,
  runModalTrigger,
}: TuneIFsPageProps) {
  const [endYearInput, setEndYearInput] = useState("2050");
  const [running, setRunning] = useState(false);
  const [progressYear, setProgressYear] = useState<number | null>(null);
  const [progressPercent, setProgressPercent] = useState(0);
  const [metadata, setMetadata] = useState<RunIFsSuccess | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [showRunModal, setShowRunModal] = useState(false);
  const [lastModalTrigger, setLastModalTrigger] = useState<number | null>(null);
  const baseYearRef = useRef<number | null>(baseYear ?? null);
  const targetEndYearRef = useRef<number | null>(null);

  useEffect(() => {
    baseYearRef.current = baseYear ?? null;
  }, [baseYear]);

  useEffect(() => {
    if (runModalTrigger && runModalTrigger !== lastModalTrigger) {
      setShowRunModal(true);
      setLastModalTrigger(runModalTrigger);
    }
  }, [runModalTrigger, lastModalTrigger]);

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
    if (metadata && typeof metadata.base_year === "number") {
      baseYearRef.current = metadata.base_year;
    }
  }, [metadata]);

  const handleEndYearChange = (event: ChangeEvent<HTMLInputElement>) => {
    setEndYearInput(event.target.value);
  };

  const openRunModal = () => {
    setShowRunModal(true);
  };

  const closeRunModal = () => {
    if (!running) {
      setShowRunModal(false);
    }
  };

  const handleChangeOutputDirectory = async () => {
    if (running) {
      return;
    }

    try {
      setError(null);
      await requestOutputDirectory();
    } catch (err) {
      const message =
        err instanceof Error
          ? err.message
          : "Unable to update the output directory.";
      setError(message);
    }
  };

  const handleRunClick = async () => {
    setError(null);

    const parsedEndYear = Number(endYearInput);
    if (!Number.isFinite(parsedEndYear) || parsedEndYear <= 0) {
      setError("Please enter a valid end year.");
      return;
    }

    if (!outputDirectory) {
      setError("Please choose an output folder before running IFs.");
      return;
    }

    targetEndYearRef.current = parsedEndYear;
    setShowRunModal(false);
    setRunning(true);
    setMetadata(null);
    setProgressYear(null);
    setProgressPercent(0);

    const response = await runIFs({
      endYear: parsedEndYear,
      baseYear: baseYearRef.current,
      outputDirectory,
    });

    if (response.status === "success") {
      setMetadata(response);
      setProgressYear(response.end_year);
      setProgressPercent(100);
      targetEndYearRef.current = response.end_year;
      if (typeof response.base_year === "number") {
        baseYearRef.current = response.base_year;
      }
    } else {
      setError(response.message);
    }

    setRunning(false);
  };

  const displayPercent = Math.min(100, Math.max(0, progressPercent));
  const formattedPercent = `${displayPercent.toFixed(1)}%`;

  const progressLabel =
    progressYear != null
      ? `Last reported year: ${progressYear} (${formattedPercent})`
      : running
      ? "Starting IFs run..."
      : metadata
      ? "Run completed."
      : "Waiting to start.";

  const wgdDisplay = metadata
    ? metadata.w_gdp.toLocaleString(undefined, { maximumFractionDigits: 2 })
    : "";

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
        {baseYearRef.current != null && (
          <p className="tune-base">Base year detected: {baseYearRef.current}</p>
        )}
        <p className="tune-output">
          <span className="label">Output folder:</span>{" "}
          {outputDirectory ?? "No folder selected"}
        </p>
      </div>

      <div className="tune-actions">
        <button
          type="button"
          className="button"
          onClick={openRunModal}
          disabled={running || !outputDirectory}
        >
          {running ? "Running..." : "Run IFs"}
        </button>
        <button
          type="button"
          className="button secondary"
          onClick={handleChangeOutputDirectory}
          disabled={running}
        >
          Change output folder
        </button>
      </div>

      <div className="progress-wrapper">
        <div className="progress-text">{progressLabel}</div>
        <progress
          className="progress-indicator"
          max={100}
          value={displayPercent}
        />
      </div>

      {error && <div className="alert alert-error">{error}</div>}

      {metadata && (
        <div className="metadata">
          <div className="run-status success">Run successful</div>
          <ul>
            <li>
              <strong>Model ID:</strong> {metadata.model_id}
            </li>
            <li>
              <strong>Base year:</strong>{" "}
              {metadata.base_year != null ? metadata.base_year : "Unknown"}
            </li>
            <li>
              <strong>End year:</strong> {metadata.end_year}
            </li>
            <li>
              <strong>World GDP (WGDP):</strong> {wgdDisplay}
            </li>
            <li>
              <strong>Saved file:</strong> {metadata.output_file}
            </li>
            <li>
              <strong>Metadata file:</strong> {metadata.metadata_file}
            </li>
          </ul>
        </div>
      )}

      <div className="tune-footer">
        <button
          type="button"
          className="button secondary"
          onClick={onBack}
          disabled={running}
        >
          Back to Validation
        </button>
      </div>

      <RunConfigModal
        isOpen={showRunModal}
        onClose={closeRunModal}
        onSubmit={handleRunClick}
        endYearInput={endYearInput}
        onEndYearChange={handleEndYearChange}
        running={running}
        baseYear={baseYearRef.current}
      />
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
  const [inputFilePath, setInputFilePath] = useState<string>(DEFAULT_INPUT_FILE);
  const [lastValidatedInputFile, setLastValidatedInputFile] =
    useState<string | null>(null);
  const [result, setResult] = useState<CheckResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [view, setView] = useState<View>("validate");
  const [runModalTrigger, setRunModalTrigger] = useState(0);
  const [info, setInfo] = useState<string | null>(null);
  const [nativeFolderPickerAvailable, setNativeFolderPickerAvailable] =
    useState<boolean>(() =>
      typeof window !== "undefined" && Boolean(window.electron?.selectFolder),
    );
  const [nativeFilePickerAvailable, setNativeFilePickerAvailable] =
    useState<boolean>(() =>
      typeof window !== "undefined" && Boolean(window.electron?.selectFile),
    );

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
        inputFilePath && inputFilePath.length > 0
          ? inputFilePath
          : DEFAULT_INPUT_FILE,
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
    setRunModalTrigger((prev) => prev + 1);
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
          validatedPath={ifsFolderPath?.trim() ?? ""}
          baseYear={result?.base_year}
          outputDirectory={outputDirectory}
          requestOutputDirectory={requestOutputDirectory}
          runModalTrigger={runModalTrigger}
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
