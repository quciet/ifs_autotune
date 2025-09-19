import { ChangeEvent, FormEvent, useState } from "react";
import { checkIFsFolder, type CheckResponse } from "./api";

function App() {
  const [path, setPath] = useState("");
  const [result, setResult] = useState<CheckResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

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
      const res = await checkIFsFolder(path.trim());
      setResult(res);
    } catch (err) {
      setError("Failed to reach backend. Ensure it is running on port 8000.");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="container">
      <header className="header">
        <h1>BIGPOPA - IFs Folder Check</h1>
        <p className="subtitle">
          Browse to your IFs installation folder and validate it against the backend API.
        </p>
      </header>

      <form className="form" onSubmit={handleSubmit}>
        <label className="label">IFs folder</label>
        <div className="input-row">
          <button type="button" className="button" onClick={handleBrowseClick}>
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

          <div className="requirements">
            <h3>Required files &amp; folders</h3>
            <ul>
              {result.requirements.map((item) => (
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
        </section>
      )}
    </div>
  );
}

export default App;
