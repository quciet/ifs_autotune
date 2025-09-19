import { ChangeEvent, FormEvent, useState } from "react";
import { checkIFsFolder } from "./api";

type CheckResult = {
  valid: boolean;
  base_year?: number | null;
  missing?: string[];
};

type Requirement = {
  id: string;
  label: string;
};

const REQUIREMENTS: Requirement[] = [
  { id: "ifs.exe", label: "ifs.exe" },
  { id: "IFsInit.db", label: "IFsInit.db" },
  { id: "net8", label: "net8/" },
  { id: "RUNFILES", label: "RUNFILES/" },
  { id: "Scenario", label: "Scenario/" },
  { id: "DATA", label: "DATA/" },
  { id: "DATA/SAMBase.db", label: "DATA/SAMBase.db" },
  { id: "DATA/DataDict.db", label: "DATA/DataDict.db" },
  { id: "DATA/IFsHistSeries.db", label: "DATA/IFsHistSeries.db" },
];

function App() {
  const [path, setPath] = useState("");
  const [result, setResult] = useState<CheckResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleFolderChange = (event: ChangeEvent<HTMLInputElement>) => {
    const files = event.target.files;
    if (!files || files.length === 0) {
      setPath("");
      return;
    }

    const firstFile = files[0] as File & {
      path?: string;
      webkitRelativePath?: string;
    };

    const filePath = firstFile.path ?? "";
    const relativePath = firstFile.webkitRelativePath ?? "";
    let folderPath = "";

    if (filePath && relativePath) {
      folderPath = filePath.slice(0, filePath.length - relativePath.length);
    } else if (filePath) {
      folderPath = filePath.replace(/[\\/][^\\/]*$/, "");
    }

    folderPath = folderPath.replace(/[\\/]+$/, "");

    if (!folderPath) {
      setPath("");
      setError("Unable to read the folder path. Please try again in Chrome or Edge.");
      event.target.value = "";
      return;
    }

    setPath(folderPath);
    setResult(null);
    setError(null);
    event.target.value = "";
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
        <label htmlFor="path-input" className="label">
          IFs folder
        </label>
        <div className="input-row">
          <label className="file-picker">
            <span>Select folder</span>
            <input
              type="file"
              className="file-input"
              webkitdirectory="true"
              onChange={handleFolderChange}
            />
          </label>
          <input
            id="path-input"
            type="text"
            className="text-input"
            value={path}
            onChange={handlePathInputChange}
            placeholder="Type or paste your IFs folder path"
            autoComplete="off"
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
          <div className={result.valid ? "status success" : "status error"}>
            {result.valid ? "Valid ✅" : "Invalid ❌"}
          </div>
          <div className="base-year">
            Base year: {result.base_year ?? "N/A"}
          </div>

          <div className="requirements">
            <h3>Required files &amp; folders</h3>
            <ul>
              {REQUIREMENTS.map((item) => {
                const isMissing = result.missing?.includes(item.id);
                return (
                  <li key={item.id} className={isMissing ? "item error" : "item success"}>
                    <span className="icon">{isMissing ? "❌" : "✅"}</span>
                    <span>{item.label}</span>
                  </li>
                );
              })}
            </ul>
          </div>
        </section>
      )}
    </div>
  );
}

export default App;
