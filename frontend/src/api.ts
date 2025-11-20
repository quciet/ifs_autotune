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

export type ApiStage = "model_setup" | "run_ifs" | "ml_driver";

export type ApiStatus = "success" | "error";

export type StageSuccess<TStage extends ApiStage, TData> = {
  status: "success";
  stage: TStage;
  message: string;
  data: TData;
};

export class StageError extends Error {
  stage: ApiStage;

  constructor(stage: ApiStage, message: string) {
    super(message);
    this.name = "StageError";
    this.stage = stage;
  }
}

export type ModelSetupData = {
  ifs_id: number;
  model_id: string;
};

export type RunIFsData = {
  ifs_id: number;
  model_id: string;
  run_folder: string;
  base_year?: number | null;
  end_year?: number | null;
  w_gdp?: number | null;
  output_file?: string;
  metadata_file?: string;
};

export type MLDriverData = {
  best_model_id?: string | null;
  best_fit_pooled?: number | null;
  iterations?: number | null;
  ifs_id?: number;
  model_id?: string;
  run_folder?: string;
  w_gdp?: number | null;
  output_file?: string;
  metadata_file?: string;
  base_year?: number | null;
  end_year?: number | null;
};

type RawStageResponse = {
  status?: unknown;
  stage?: unknown;
  message?: unknown;
  data?: unknown;
};

function normalizeStageResponse<
  TExpected extends ApiStage,
  TAllowed extends ApiStage,
  TData,
>(
  raw: unknown,
  expectedStage: TExpected,
  allowedStages: TAllowed[] = [],
): StageSuccess<TExpected | TAllowed, TData> {
  if (!raw || typeof raw !== "object") {
    throw new StageError(
      expectedStage,
      "Unexpected response structure received from backend.",
    );
  }

  const typed = raw as RawStageResponse;
  const stageName =
    typeof typed.stage === "string" && typed.stage.length > 0
      ? (typed.stage as ApiStage)
      : expectedStage;
  const message =
    typeof typed.message === "string" && typed.message.trim().length > 0
      ? typed.message
      : "Operation failed.";

  if (typed.status !== "success") {
    throw new StageError(stageName, message);
  }

  const validStages = new Set<ApiStage>([expectedStage, ...allowedStages]);

  if (!validStages.has(stageName)) {
    throw new StageError(
      expectedStage,
      `Unexpected stage "${stageName}" returned from backend.`,
    );
  }

  if (!typed.data || typeof typed.data !== "object") {
    throw new StageError(
      expectedStage,
      "Response payload is missing required data.",
    );
  }

  return {
    status: "success",
    stage: stageName as TExpected | TAllowed,
    message,
    data: typed.data as TData,
  };
}

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

export interface RunIFsParams {
  validatedPath: string;
  endYear: number;
  baseYear?: number | null;
  outputDirectory: string;
  modelId: string;
  ifsId: number;
}

export type IFsProgressEvent = {
  year: number;
  percent?: number;
};

export type ExtractCompareParams = {
  ifsRoot: string;
  modelDb: string;
  inputFilePath: string;
  modelId: string;
  ifsId: number;
};

export async function modelSetup({
  baseYear,
  endYear,
  parameters,
  coefficients,
  paramDim,
  validatedPath,
  inputFilePath,
  outputFolder,
}: {
  baseYear: number | null | undefined;
  endYear: number;
  parameters?: Record<string, unknown>;
  coefficients?: Record<string, unknown>;
  paramDim?: Record<string, unknown>;
  validatedPath: string;
  inputFilePath: string;
  outputFolder?: string | null;
}): Promise<StageSuccess<"model_setup", ModelSetupData>> {
  if (!window.electron?.invoke) {
    throw new StageError("model_setup", "Electron bridge is unavailable.");
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
      outputFolder: outputFolder ?? null,
    };
    const result = await window.electron.invoke("model_setup", payload);
    return normalizeStageResponse(result, "model_setup");
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Unable to complete model setup.";
    throw new StageError("model_setup", message);
  }
}

export async function runML({
  validatedPath,
  endYear,
  baseYear,
  outputDirectory,
  modelId,
  ifsId,
}: RunIFsParams): Promise<StageSuccess<"ml_driver", MLDriverData>> {
  if (!window.electron?.invoke) {
    throw new StageError("ml_driver", "Electron bridge is unavailable.");
  }

  const normalizedModelId = modelId.trim();
  const normalizedIfsId = Number(ifsId);

  if (!normalizedModelId || !Number.isFinite(normalizedIfsId)) {
    throw new Error("Missing modelId or ifsId — runML cannot proceed.");
  }

  try {
    const payload = await window.electron.invoke("run-ml", {
      validatedPath,
      endYear,
      baseYear: baseYear ?? null,
      outputDirectory,
      end_year: endYear,
      base_year: baseYear ?? null,
      output_dir: outputDirectory,
      modelId: normalizedModelId,
      ifsId: normalizedIfsId,
    });
    return normalizeStageResponse(payload, "ml_driver");
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Unable to start the IFs run.";
    throw new StageError("ml_driver", message);
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
