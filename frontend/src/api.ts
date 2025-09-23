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
  model_id: string;
  base_year: number | null;
  end_year: number;
  w_gdp: number;
  output_file: string;
  metadata_file: string;
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

export type RunIFsParams = {
  endYear: number;
  baseYear: number | null | undefined;
  outputDirectory: string;
};

export type IFsProgressEvent = {
  year: number;
  percent?: number;
};

export async function runIFs({
  endYear,
  baseYear,
  outputDirectory,
}: RunIFsParams): Promise<RunIFsResponse> {
  if (!window.electron?.invoke) {
    return {
      status: "error",
      message: "Electron bridge is unavailable.",
    };
  }

  try {
    const payload = await window.electron.invoke("run-ifs", {
      end_year: endYear,
      base_year: baseYear ?? null,
      output_dir: outputDirectory,
    });
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
  callback: (event: IFsProgressEvent) => void,
): () => void {
  if (!window.electron?.on) {
    return () => undefined;
  }

  const handler = (value: unknown) => {
    if (typeof value === "number" && Number.isFinite(value)) {
      callback({ year: value });
      return;
    }

    if (value && typeof value === "object") {
      const maybeEvent = value as { year?: unknown; percent?: unknown };
      const { year } = maybeEvent;
      if (typeof year === "number" && Number.isFinite(year)) {
        const percent = maybeEvent.percent;
        callback({
          year,
          percent:
            typeof percent === "number" && Number.isFinite(percent)
              ? percent
              : undefined,
        });
      }
    }
  };

  return window.electron.on("ifs-progress", handler);
}
