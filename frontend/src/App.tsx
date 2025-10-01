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

type View = "validate" | "tune";

type TuneIFsPageProps = {
  onBack: () => void;
  validatedPath: string;
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
  const [progressYear, setProgressYear] = useState<number | null>(null);
  const [progressPercent, setProgressPercent] = useState(0);
  const [metadata, setMetadata] = useState<RunIFsSuccess | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [effectiveBaseYear, setEffectiveBaseYear] = useState<number | null>(
    typeof baseYear === "number" && Number.isFinite(baseYear) ? baseYear : null,
  );
  const baseYearRef = useRef<number | null>(baseYear ?? null);
  const targetEndYearRef = useRef<number | null>(DEFAULT_END_YEAR);

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
    if (metadata && typeof metadata.base_year === "number") {
      baseYearRef.current = metadata.base_year;
      setEffectiveBaseYear(metadata.base_year);
    }
  }, [metadata]);

  const handleEndYearInputChange = (event: ChangeEvent<HTMLInputElement>) => {
    const value = event.target.value;
    setEndYearInput(value);

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
  };

  const handleSliderChange = (event: ChangeEvent<HTMLInputElement>) => {
    const parsed = Number(event.target.value);
    const clamped = clampEndYear(parsed);
    setEndYear(clamped);
    setEndYearInput(String(clamped));
    targetEndYearRef.current = clamped;
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

    const clampedEndYear = clampEndYear(parsedEndYear);
    targetEndYearRef.current = clampedEndYear;
    setEndYear(clampedEndYear);
    setEndYearInput(String(clampedEndYear));
    setRunning(true);
    setMetadata(null);
    setProgressYear(null);
    setProgressPercent(0);

    const response = await runIFs({
      endYear: clampedEndYear,
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
        setEffectiveBaseYear(response.base_year);
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
          onClick={handleRunClick}
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

    </section>
  );
}

function App() {
  const [ifsFolderPath, setIfsFolderPath] = useState<string | null>(null);
  const [lastValidatedIfsFolder, setLastValidatedIfsFolder] =
    useState<string | null>(null);
  const [outputDirectory, setOutputDirectory] = useState<string | null>(null);
  const [result, setResult] = useState<CheckResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [view, setView] = useState<View>("validate");
  const [info, setInfo] = useState<string | null>(null);
  const [nativeFolderPickerAvailable, setNativeFolderPickerAvailable] =
    useState<boolean>(() =>
      typeof window !== "undefined" && Boolean(window.electron?.selectFolder),
    );

  useEffect(() => {
    if (typeof window !== "undefined") {
      setNativeFolderPickerAvailable(Boolean(window.electron?.selectFolder));
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
    const trimmedPath = ifsFolderPath?.trim() ?? "";
    if (!trimmedPath) {
      setError("Please select an IFs folder before validating.");
      setResult(null);
      return;
    }

    setLoading(true);
    setError(null);
    setInfo(null);
    setResult(null);

    try {
      const res = await validateIFsFolder(trimmedPath);
      setIfsFolderPath(trimmedPath);
      setResult(res);
      setLastValidatedIfsFolder(res.valid ? trimmedPath : null);
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
                onChange={(e) => setOutputDirectory(e.target.value)}
                placeholder="No folder selected"
                spellCheck={false}
                title={outputTitle}
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
