export type RequirementCheck = {
  file: string;
  exists: boolean;
};

export type CheckResponse = {
  valid: boolean;
  base_year?: number | null;
  requirements?: RequirementCheck[];
  missingFiles?: string[];
  pathChecks?: {
    ifsFolder?: ValidationPathCheck;
    outputFolder?: ValidationPathCheck;
    inputFile?: ValidationInputFileCheck;
  };
};

export type ValidationPathCheck = {
  displayPath: string | null;
  exists: boolean;
  readable?: boolean;
  writable?: boolean | null;
  message?: string | null;
};

export type ValidationInputFileCheck = ValidationPathCheck & {
  sheets?: Record<string, boolean>;
  missingSheets?: string[];
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

export type ModelSetupSuccess = {
  status: "success";
  sce_id: string;
  sce_file: string;
};

export type ModelSetupError = {
  status: "error";
  message: string;
};

export type ModelSetupResponse = ModelSetupSuccess | ModelSetupError;

const FALLBACK_RESPONSE: CheckResponse = {
  valid: false,
  base_year: null,
  requirements: [],
  missingFiles: ["Electron/Python IPC call failed"],
  pathChecks: {},
};

export type ValidateIFsPayload = {
  ifsPath: string;
  outputPath?: string | null;
  inputFilePath?: string | null;
};

export async function validateIFsFolder({
  ifsPath,
  outputPath,
  inputFilePath,
}: ValidateIFsPayload): Promise<CheckResponse> {
  if (!window.electron?.invoke) {
    return { ...FALLBACK_RESPONSE };
  }

  try {
    const payload = {
      ifsPath,
      outputPath: outputPath ?? null,
      inputFilePath: inputFilePath ?? null,
    };
    const result = await window.electron.invoke("validate-ifs-folder", payload);
    if (result && typeof result === "object") {
      return result as CheckResponse;
    }
    return { ...FALLBACK_RESPONSE };
  } catch (error) {
    return { ...FALLBACK_RESPONSE };
  }
}

export type RunIFsParams = {
  validatedPath: string;
  endYear: number;
  baseYear: number | null | undefined;
  outputDirectory: string;
  sceId?: string | null;
  sceFile?: string | null;
};

export type IFsProgressEvent = {
  year: number;
  percent?: number;
};

export async function modelSetup({
  baseYear,
  endYear,
  parameters,
  coefficients,
  paramDim,
  validatedPath,
  inputFilePath,
}: {
  baseYear: number | null | undefined;
  endYear: number;
  parameters?: Record<string, unknown>;
  coefficients?: Record<string, unknown>;
  paramDim?: Record<string, unknown>;
  validatedPath: string;
  inputFilePath: string;
}): Promise<ModelSetupResponse> {
  if (!window.electron?.invoke) {
    return {
      status: "error",
      message: "Electron bridge is unavailable.",
    };
  }

  try {
    const payload = {
      baseYear: baseYear ?? null,
      endYear,
      parameters: parameters ?? {},
      coefficients: coefficients ?? {},
      param_dim_dict: paramDim ?? {},
      validatedPath,
      inputFilePath,
    };
    const result = await window.electron.invoke("model_setup", payload);
    if (result && typeof result === "object" && "status" in result) {
      return result as ModelSetupResponse;
    }

    return {
      status: "error",
      message: "Unexpected response from model setup.",
    };
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Unable to complete model setup.";
    return { status: "error", message };
  }
}

export async function runIFs({
  validatedPath,
  endYear,
  baseYear,
  outputDirectory,
  sceId,
  sceFile,
}: RunIFsParams): Promise<RunIFsResponse> {
  if (!window.electron?.invoke) {
    return {
      status: "error",
      message: "Electron bridge is unavailable.",
    };
  }

  try {
    const payload = await window.electron.invoke("run_ifs", {
      validatedPath,
      endYear,
      baseYear: baseYear ?? null,
      outputDirectory,
      end_year: endYear,
      base_year: baseYear ?? null,
      output_dir: outputDirectory,
      sce_id: sceId ?? null,
      sce_file: sceFile ?? null,
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
