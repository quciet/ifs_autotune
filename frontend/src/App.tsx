import { ChangeEvent, FormEvent, useEffect, useMemo, useState } from "react";
import {
  runIFs,
  subscribeToIFsProgress,
  validateIFsFolder,
  type CheckResponse,
  type RunIFsSuccess,
} from "./api";

type View = "validate" | "tune";

type TuneIFsPageProps = {
  onBack: () => void;
  validatedPath: string;
  baseYear?: number | null;
};

function TuneIFsPage({ onBack, validatedPath, baseYear }: TuneIFsPageProps) {
  const [endYearInput, setEndYearInput] = useState("2050");
  const [running, setRunning] = useState(false);
  const [progressYear, setProgressYear] = useState<number | null>(null);
  const [metadata, setMetadata] = useState<RunIFsSuccess | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const unsubscribe = subscribeToIFsProgress((year) => {
      setProgressYear(year);
    });

    return () => {
      unsubscribe();
    };
  }, []);

  const handleEndYearChange = (event: ChangeEvent<HTMLInputElement>) => {
    setEndYearInput(event.target.value);
  };

  const handleRunClick = async () => {
    setError(null);
    setMetadata(null);
    setProgressYear(null);

    const parsedEndYear = Number(endYearInput);
    if (!Number.isFinite(parsedEndYear) || parsedEndYear <= 0) {
      setError("Please enter a valid end year.");
      return;
    }

    setRunning(true);

    const response = await runIFs(parsedEndYear);
    if (response.status === "ok") {
      setMetadata(response);
      setProgressYear(response.end_year);
    } else {
      setError(response.message);
    }

    setRunning(false);
  };

  const numericEndYear = Number(endYearInput);
  const progressMax =
    Number.isFinite(numericEndYear) && numericEndYear > 0
      ? numericEndYear
      : undefined;
  const isRunDisabled = running || !progressMax;

  const progressLabel = running
    ? progressYear != null
      ? `Current simulation year: ${progressYear}`
      : "Starting IFs run..."
    : progressYear != null
    ? `Last reported year: ${progressYear}`
    : "Waiting to start.";

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
        {baseYear != null && (
          <p className="tune-base">Base year detected: {baseYear}</p>
        )}
      </div>

      <div className="tune-controls">
        <label className="label" htmlFor="end-year-input">
          End Year
        </label>
        <input
          id="end-year-input"
          type="number"
          className="path-input"
          value={endYearInput}
          onChange={handleEndYearChange}
          disabled={running}
          min={baseYear ?? undefined}
        />
        <button
          type="button"
          className="button"
          onClick={handleRunClick}
          disabled={isRunDisabled}
        >
          {running ? "Running..." : "Run IFs"}
        </button>
      </div>

      <div className="progress-wrapper">
        <div className="progress-text">{progressLabel}</div>
        <progress
          className="progress-indicator"
          max={progressMax}
          value={progressYear ?? undefined}
        />
      </div>

      {error && <div className="alert alert-error">{error}</div>}

      {metadata && (
        <div className="metadata">
          <h3>Run Metadata</h3>
          <ul>
            <li>
              <strong>Status:</strong> {metadata.status}
            </li>
            <li>
              <strong>End year:</strong> {metadata.end_year}
            </li>
            <li>
              <strong>Log file:</strong> {metadata.log}
            </li>
            <li>
              <strong>Session ID:</strong> {metadata.session_id}
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
  const [path, setPath] = useState("");
  const [result, setResult] = useState<CheckResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [view, setView] = useState<View>("validate");

  const handleBrowseClick = async () => {
    setError(null);

    if (!window.electron?.selectFolder) {
      setError("Native folder browsing is only available in the desktop app.");
      return;
    }

    try {
      const selectedPath = await window.electron.selectFolder();
      if (selectedPath) {
        setPath(selectedPath);
        setResult(null);
      }
    } catch (err) {
      setError("Unable to open the folder picker. Please try again.");
    }
  };

  const handlePathInputChange = (event: ChangeEvent<HTMLInputElement>) => {
    setPath(event.target.value);
    setResult(null);
    setError(null);
    setView("validate");
  };

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!path.trim()) {
      setError("Please select an IFs folder before validating.");
      setResult(null);
      return;
    }

    setLoading(true);
    setError(null);
    setResult(null);

    try {
      const res = await validateIFsFolder(path.trim());
      setResult(res);
    } catch (err) {
      setError("Failed to validate the IFs folder. Please try again.");
    } finally {
      setLoading(false);
    }
  };

  const missingFiles = useMemo(() => result?.missingFiles ?? [], [result]);
  const requirements = useMemo(() => result?.requirements ?? [], [result]);

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
            <label className="label">IFs folder</label>
            <div className="input-row">
              <button
                type="button"
                className="button"
                onClick={handleBrowseClick}
              >
                Browse
              </button>
              <div className="actions">
                <input
                  type="text"
                  className="path-input"
                  placeholder="Enter or paste a folder path"
                  value={path}
                  onChange={handlePathInputChange}
                  spellCheck={false}
                />
                <button type="submit" className="button" disabled={loading}>
                  {loading ? "Validating..." : "Validate"}
                </button>
              </div>
            </div>
          </form>

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

              <div className="view-actions">
                <button
                  type="button"
                  className="button secondary"
                  onClick={() => setView("tune")}
                  disabled={!result.valid}
                >
                  Tune IFs
                </button>
              </div>
            </section>
          )}
        </>
      )}

      {view === "tune" && result?.valid && (
        <TuneIFsPage
          onBack={() => setView("validate")}
          validatedPath={path.trim()}
          baseYear={result?.base_year}
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
