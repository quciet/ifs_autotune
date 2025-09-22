export type RequirementCheck = {
  file: string;
  exists: boolean;
};

export type CheckResponse = {
  valid: boolean;
  base_year?: number | null;
  requirements?: RequirementCheck[];
  missingFiles?: string[];
};

export type RunIFsSuccess = {
  status: "success";
  end_year: number;
  w_gdp: number;
};

export type RunIFsError = {
  status: "error";
  message: string;
};

export type RunIFsResponse = RunIFsSuccess | RunIFsError;

const FALLBACK_RESPONSE: CheckResponse = {
  valid: false,
  base_year: null,
  requirements: [],
  missingFiles: ["Electron/Python IPC call failed"],
};

export async function validateIFsFolder(folderPath: string): Promise<CheckResponse> {
  if (!window.electron?.invoke) {
    return { ...FALLBACK_RESPONSE };
  }

  try {
    const result = await window.electron.invoke("validate-ifs-folder", folderPath);
    if (result && typeof result === "object") {
      return result as CheckResponse;
    }
    return { ...FALLBACK_RESPONSE };
  } catch (error) {
    return { ...FALLBACK_RESPONSE };
  }
}

export async function runIFs(endYear: number): Promise<RunIFsResponse> {
  if (!window.electron?.invoke) {
    return {
      status: "error",
      message: "Electron bridge is unavailable.",
    };
  }

  try {
    const payload = await window.electron.invoke("run-ifs", { end_year: endYear });
    if (payload && typeof payload === "object" && "status" in payload) {
      const typed = payload as RunIFsResponse;
      if (typed.status === "success") {
        return typed;
      }

      if (typed.status === "error") {
        return typed;
      }
    }

    return {
      status: "error",
      message: "Unexpected response from IFs runner.",
    };
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Unable to start the IFs run.";
    return { status: "error", message };
  }
}

export function subscribeToIFsProgress(
  callback: (year: number) => void,
): () => void {
  if (!window.electron?.on) {
    return () => undefined;
  }

  const handler = (value: unknown) => {
    if (typeof value === "number" && Number.isFinite(value)) {
      callback(value);
    }
  };

  return window.electron.on("ifs-progress", handler);
}
