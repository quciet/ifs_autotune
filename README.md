# BIGPOPA (Desktop App)

Local-first desktop application for automating and optimizing IFs model runs.  
Runs fully offline: React/Electron frontend + Python backend, with Electron acting as the bridge.

---
## How it Works

BIGPOPA is a desktop tool for automating and optimizing International Futures (IFs) model runs. The application guides the user through the following workflow:

1.  **Validation:** The user selects their IFs installation folder, an output directory, and an input Excel file (`StartingPointTable.xlsx`). The app validates that all required files and folders are present and readable.
2.  **Model Setup:** The user configures the simulation end year. The backend then prepares the IFs model based on the settings defined in the `StartingPointTable.xlsx` file.
3.  **ML Optimization:** The app runs an active learning loop to find the best model parameters. It repeatedly executes the IFs model, extracts results, and compares them against historical data to calculate a fitness score.
4.  **Results:** The best model configuration is identified, and the results are displayed in the user interface. All runs and their outputs are stored in a SQLite database for review and reuse.

## Working Files

-   **`StartingPointTable.xlsx`**: This is the primary input file for configuring the model and the machine learning optimization. Users can define model parameters, coefficients, and other settings in the various sheets of this Excel workbook.
-   **`bigpopa.db`**: This is a SQLite database that is created in the user-specified output directory. It stores all model configurations, run artifacts, and final results. It also functions as a cache, allowing the optimization loop to reuse results from previously evaluated model configurations, which significantly speeds up the process.

## Run the app locally (development)

1. **Clone the repository**
   ```bash
   git clone https://github.com/<your-org>/ifs_autotune.git
   cd ifs_autotune
   ```

2. **(Optional) Set up a Python virtual environment**
   - Using the built-in `venv` module:
     ```bash
     cd backend
     python -m venv .venv
     .venv\Scripts\activate    # On Windows
     # or: source .venv/bin/activate  # On Linux/Mac
     ```
   - Exit the environment with `deactivate` when done.
   - Using Conda or another manager also works.

3. **Install backend in editable mode**
   - Ensure you have Python 3.11 or later installed.
   - Starting from root directory ifs_autotune/
     ```bash
     cd backend
     pip install -e .
     cd ..
     ```

4. **Install frontend + desktop dependencies**
   - Ensure you have Node.js 18+ and npm installed.
   - Install packages for both frontend and Electron
   - Starting from the root directory ifs_autotune/
     ```bash
     cd frontend
     npm install
     cd ../desktop
     npm install
     cd ..
     ```

5. **Place the default input workbook (optional but recommended)**
   - Create an `input` folder inside `desktop/` if it does not already exist.
   - Copy your `StartingPointTable.xlsx` into `desktop/input/StartingPointTable.xlsx`.
   - The desktop app will automatically point the file picker at this location when it launches.

6. **Launch the desktop app (dev mode)**
   - From the `desktop/` folder:
     ```bash
     npm run electron-dev
     ```
   - This will:
     - Start the React frontend with hot reload (Vite).
     - Start Electron and open a desktop window.
   - Use the **Browse** button (native folder picker) or paste a path to select your IFs installation folder, then click **Validate**.
   - Validation results display:
     - ✅ or ❌ for required files/folders (`IFsInit.db`, `DATA/SAMBase.db`, `RUNFILES/DataDict.db`, `RUNFILES/IFsHistSeries.db`, `net8/ifs.exe`, `RUNFILES/`, `Scenario/`).
     - Extracted base year from `IFsInit.db`.

---

