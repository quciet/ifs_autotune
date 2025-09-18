import { FormEvent, useState } from "react";
import { checkIFsFolder } from "./api";

type CheckResult = {
  valid: boolean;
  base_year?: number | null;
  missing?: string[];
};

function App() {
  const [path, setPath] = useState("");
  const [result, setResult] = useState<CheckResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleSubmit = async (event: FormEvent<HTMLFormElement> | MouseEvent) => {
    event.preventDefault?.();
    if (!path.trim()) {
      setError("Please provide a folder path.");
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
          Enter the path to your IFs installation and validate it against the backend API.
        </p>
      </header>

      <form className="form" onSubmit={handleSubmit as (e: FormEvent<HTMLFormElement>) => void}>
        <label htmlFor="path" className="label">
          IFs folder path
        </label>
        <div className="input-row">
          <input
            id="path"
            type="text"
            value={path}
            onChange={(e) => setPath(e.target.value)}
            placeholder="C:\\Program Files\\IFs"
            className="text-input"
          />
          <button type="submit" className="button" disabled={loading}>
            {loading ? "Validating..." : "Validate"}
          </button>
        </div>
      </form>

      {error && <div className="alert alert-error">{error}</div>}

      {result && (
        <section className="results">
          <h2>Validation Results</h2>
          <div className="result-grid">
            <div>
              <span className="label">Valid</span>
              <span className={result.valid ? "value success" : "value error"}>
                {String(result.valid)}
              </span>
            </div>
            <div>
              <span className="label">Base Year</span>
              <span className="value">{result.base_year ?? "N/A"}</span>
            </div>
          </div>

          {result.missing && result.missing.length > 0 ? (
            <div className="missing">
              <span className="label">Missing files</span>
              <ul>
                {result.missing.map((file) => (
                  <li key={file}>{file}</li>
                ))}
              </ul>
            </div>
          ) : (
            <p className="label">No missing files reported.</p>
          )}
        </section>
      )}
    </div>
  );
}

export default App;
