export type RequirementCheck = {
  file: string;
  exists: boolean;
};

export type CheckResponse = {
  valid: boolean;
  base_year?: number | null;
  ifs_static_id?: number | null;
  profileReady?: boolean;
  requirements?: RequirementCheck[];
  missingFiles?: string[];
  infoMessages?: string[];
  dbMigration?: {
    performed?: boolean;
    message?: string | null;
    original_version?: number;
    new_version?: number;
    backup_path?: string | null;
    legacy_tables_dropped?: boolean;
    model_run_rows?: number;
    migrated_input_only_rows?: number;
    migrated_proposal_rows?: number;
    migrated_output_rows?: number;
  };
  pathChecks?: {
    ifsFolder?: ValidationPathCheck;
    outputFolder?: ValidationPathCheck;
    inputProfile?: ValidationProfileCheck;
  };
};

export type ValidationPathCheck = {
  displayPath: string | null;
  exists: boolean;
  readable?: boolean;
  writable?: boolean | null;
  message?: string | null;
};

export type ValidationProfileCheck = ValidationPathCheck & {
  profileId?: number | null;
  valid?: boolean;
  errors?: string[];
  profile?: InputProfileSummary | null;
};

export type InputProfileSummary = {
  profile_id: number;
  ifs_static_id: number;
  name: string;
  description?: string | null;
  created_at_utc?: string | null;
  updated_at_utc?: string | null;
  archived?: boolean;
  source_type?: string | null;
  source_path?: string | null;
  enabled_param_count?: number;
  enabled_coefficient_count?: number;
  enabled_output_count?: number;
};

export type ParameterProfileRow = {
  param_name: string;
  param_type?: string | null;
  param_default?: number | null;
  param_min?: number | null;
  param_max?: number | null;
  enabled: boolean;
  minimum?: number | null;
  maximum?: number | null;
  step?: number | null;
  level_count?: number | null;
  sort_order?: number | null;
};

export type CoefficientProfileRow = {
  function_name: string;
  y_name?: string | null;
  x_name: string;
  reg_seq?: number | null;
  beta_name: string;
  beta_default?: number | null;
  beta_std?: number | null;
  source_sheet?: string | null;
  enabled: boolean;
  minimum?: number | null;
  maximum?: number | null;
  step?: number | null;
  level_count?: number | null;
  sort_order?: number | null;
};

export type OutputProfileRow = {
  variable: string;
  table_name: string;
  enabled: boolean;
  sort_order?: number | null;
};

export type InputProfileMLSettings = {
  ml_method: string;
  fit_metric: string;
  n_sample: number;
  n_max_iteration: number;
  n_convergence: number;
  min_convergence_pct: number;
};

export type InputProfileValidation = {
  valid: boolean;
  errors: string[];
  enabled_param_count: number;
  enabled_coefficient_count: number;
  enabled_output_count: number;
};

export type InputProfileDetail = {
  profile: InputProfileSummary;
  parameter_catalog: ParameterProfileRow[];
  coefficient_catalog: CoefficientProfileRow[];
  output_catalog: OutputProfileRow[];
  ml_settings: InputProfileMLSettings;
  validation: InputProfileValidation;
};

export type InputProfileMutationResponse = {
  profile: InputProfileSummary;
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
  dataset_id: string | null;
  retained_artifact_dir?: string | null;
  dataset_warning?: string | null;
  dataset_diagnostics?: {
    reference_model_id?: string | null;
    reference_dataset_id?: string | null;
    current_param_count?: number;
    reference_param_count?: number;
    parameter_keys_added?: string[];
    parameter_keys_removed?: string[];
    coefficient_keys_added?: string[];
    coefficient_keys_removed?: string[];
    output_keys_added?: string[];
    output_keys_removed?: string[];
  } | null;
};

export type RunIFsData = {
  ifs_id: number;
  model_id: string;
  run_folder?: string | null;
  base_year?: number | null;
  end_year?: number | null;
  w_gdp?: number | null;
  output_file?: string | null;
  metadata_file?: string;
};

export type MLDriverData = {
  code?: number | null;
  best_model_id?: string | null;
  best_fit_pooled?: number | null;
  iterations?: number | null;
  terminationReason?: "completed" | "stopped_gracefully" | null;
  dataset_id?: string | null;
  ifs_id?: number;
  model_id?: string;
  run_folder?: string | null;
  w_gdp?: number | null;
  output_file?: string | null;
  metadata_file?: string;
  base_year?: number | null;
  end_year?: number | null;
};

export type ArtifactRetentionMode = "none" | "best_only" | "all";

export type MLProgressTrial = {
  run_id?: number | null;
  model_id: string | null;
  model_status: string | null;
  fit_pooled: number | null;
  fit_missing?: boolean | null;
  trial_index: number | null;
  batch_index: number | null;
  started_at_utc: string | null;
  completed_at_utc: string | null;
  dataset_id?: string | null;
  sequence_index?: number | null;
  derived_round_index?: number | null;
  progress_rowid?: number | null;
};

export type MLProgressHistoryData = {
  dataset_id: string | null;
  reference_model_id?: string | null;
  reference_fit_pooled?: number | null;
  latest_run_id?: number | null;
  trials: MLProgressTrial[];
};

export type TrendSummaryData = {
  dataset_id: string | null;
  current_round_index: number;
  latest_slice_count: number;
  latest_slice_run_start: number;
  latest_slice_run_end: number;
  latest_slice_round_start: number;
  latest_slice_round_end: number;
  latest_slice_trial_start: number | null;
  latest_slice_trial_end: number | null;
  latest_slice_started_at_utc: string | null;
  latest_slice_last_timestamp_utc: string | null;
  best_fit: number | null;
  best_run_index: number | null;
  best_trial_index: number | null;
  best_round_index: number | null;
  best_model_id: string | null;
  latest_fit: number | null;
  rows_since_last_best_improvement: number | null;
  last_best_improvement_run_index: number | null;
  last_best_improvement_trial_index: number | null;
  last_best_improvement_round_index: number | null;
  last_best_improvement_timestamp_utc: string | null;
  rolling_center_interpretation: string;
  rolling_spread_interpretation: string;
  practical_trend_interpretation: string;
  early_median_average: number | null;
  late_median_average: number | null;
  early_iqr_average: number | null;
  late_iqr_average: number | null;
};

export type TrendAnalysisData = {
  dataset_id: string | null;
  output_dir: string;
  summary_path: string;
  metrics_path: string;
  plot_path: string;
  parameter_plot_paths: string[];
  coefficient_plot_paths: string[];
  parameter_plot_count: number;
  coefficient_plot_count: number;
  parameter_count: number;
  coefficient_count: number;
  output_variable_count: number;
  summary: TrendSummaryData;
};

export type DesktopCapabilities = {
  trendAnalysis: boolean;
  openPath: boolean;
  trendDatasetOptions?: boolean;
  imagePreview?: boolean;
};

export type TrendDatasetOptionsData = {
  latest_dataset_id: string | null;
  dataset_ids: string[];
  dataset_run_counts: Record<string, number>;
  latest_dataset_run_count: number | null;
};

export type ArtifactImagePreviewData = {
  dataUrl: string;
  mimeType: string;
  targetPath: string;
};

const DEFAULT_DESKTOP_CAPABILITIES: DesktopCapabilities = {
  trendAnalysis: false,
  openPath: false,
};

const TREND_ANALYSIS_UNAVAILABLE_MESSAGE =
  "Trend Analysis is unavailable in this desktop session. Restart the app to load the latest desktop handlers.";
const ELECTRON_BRIDGE_UNAVAILABLE_MESSAGE =
  "Trend Analysis is unavailable because the Electron desktop bridge did not load. Restart the app and try again.";

function isMissingIpcHandlerError(error: unknown, channel: string): boolean {
  const message = error instanceof Error ? error.message : String(error ?? "");
  return message.includes(`No handler registered for '${channel}'`);
}

function normalizeTrendAnalysisInvokeError(error: unknown): Error {
  if (isMissingIpcHandlerError(error, "analysis:runTrendAnalysis")) {
    return new Error(TREND_ANALYSIS_UNAVAILABLE_MESSAGE);
  }

  const fallbackMessage =
    error instanceof Error && error.message.trim().length > 0
      ? error.message
      : "Trend analysis failed.";
  return new Error(fallbackMessage);
}

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
  infoMessages: [],
  pathChecks: {},
};

export type ValidateIFsPayload = {
  ifsPath: string;
  outputPath?: string | null;
  inputProfileId?: number | null;
};

export async function validateIFsFolder({
  ifsPath,
  outputPath,
  inputProfileId,
}: ValidateIFsPayload): Promise<CheckResponse> {
  if (!window.electron?.invoke) {
    return { ...FALLBACK_RESPONSE };
  }

  try {
    const payload = {
      ifsPath,
      outputPath: outputPath ?? null,
      inputProfileId: inputProfileId ?? null,
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
  inputProfileId: number;
  artifactRetentionMode?: ArtifactRetentionMode;
}

export type IFsProgressEvent = {
  year: number;
  percent?: number;
};

export type ExtractCompareParams = {
  ifsRoot: string;
  modelDb: string;
  modelId: string;
  ifsId: number;
  inputProfileId?: number | null;
  bigpopaDb?: string | null;
  outputDir?: string | null;
};

export async function modelSetup({
  baseYear,
  endYear,
  parameters,
  coefficients,
  paramDim,
  validatedPath,
  inputProfileId,
  outputFolder,
  artifactRetentionMode,
}: {
  baseYear: number | null | undefined;
  endYear: number;
  parameters?: Record<string, unknown>;
  coefficients?: Record<string, unknown>;
  paramDim?: Record<string, unknown>;
  validatedPath: string;
  inputProfileId: number;
  outputFolder?: string | null;
  artifactRetentionMode?: ArtifactRetentionMode;
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
      inputProfileId,
      outputFolder: outputFolder ?? null,
      artifactRetentionMode: artifactRetentionMode ?? "none",
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
  inputProfileId,
  artifactRetentionMode,
}: RunIFsParams): Promise<StageSuccess<"ml_driver", MLDriverData>> {
  if (!window.electron?.invoke) {
    throw new StageError("ml_driver", "Electron bridge is unavailable.");
  }

  const normalizedModelId = modelId.trim();
  const normalizedIfsId = Number(ifsId);

  if (!normalizedModelId || !Number.isFinite(normalizedIfsId)) {
    throw new Error("Missing modelId or ifsId - runML cannot proceed.");
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
      inputProfileId,
      artifactRetentionMode: artifactRetentionMode ?? "none",
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

export async function getMLProgressHistory(
  outputDir?: string | null,
  datasetId?: string | null,
  modelId?: string | null,
  sinceRunId?: number | null,
): Promise<MLProgressHistoryData> {
  if (!window.electron?.getMLProgressHistory) {
    return {
      dataset_id: datasetId ?? null,
      reference_model_id: modelId ?? null,
      reference_fit_pooled: null,
      latest_run_id: null,
      trials: [],
    };
  }

  try {
    const response = await window.electron.getMLProgressHistory(
      outputDir ?? null,
      datasetId ?? null,
      modelId ?? null,
      sinceRunId ?? null,
    );
    const responseDatasetId =
      typeof response?.data?.dataset_id === "string" || response?.data?.dataset_id === null
        ? response.data.dataset_id
        : datasetId ?? null;
    const referenceModelId =
      typeof response?.data?.reference_model_id === "string" ||
      response?.data?.reference_model_id === null
        ? response.data.reference_model_id
        : modelId ?? null;
    const referenceFitPooled =
      typeof response?.data?.reference_fit_pooled === "number" &&
      Number.isFinite(response.data.reference_fit_pooled)
        ? response.data.reference_fit_pooled
        : response?.data?.reference_fit_pooled === null
          ? null
          : null;
    const trials = response?.data?.trials;
    const latestRunId =
      typeof response?.data?.latest_run_id === "number" &&
      Number.isFinite(response.data.latest_run_id)
        ? response.data.latest_run_id
        : typeof response?.data?.latest_progress_rowid === "number" &&
            Number.isFinite(response.data.latest_progress_rowid)
          ? response.data.latest_progress_rowid
          : response?.data?.latest_run_id === null ||
              response?.data?.latest_progress_rowid === null
          ? null
          : null;
    return {
      dataset_id: responseDatasetId,
      reference_model_id: referenceModelId,
      reference_fit_pooled: referenceFitPooled,
      latest_run_id: latestRunId,
      trials: Array.isArray(trials) ? (trials as MLProgressTrial[]) : [],
    };
  } catch {
    return {
      dataset_id: datasetId ?? null,
      reference_model_id: modelId ?? null,
      reference_fit_pooled: null,
      latest_run_id: null,
      trials: [],
    };
  }
}

export async function listProfiles(
  outputFolder: string,
  ifsStaticId: number,
  includeArchived = false,
): Promise<InputProfileSummary[]> {
  if (!window.electron?.invoke) {
    return [];
  }
  const result = await window.electron.invoke("profiles:list", {
    outputFolder,
    ifsStaticId,
    includeArchived,
  });
  return ((result as { profiles?: InputProfileSummary[] } | null)?.profiles ?? []) as InputProfileSummary[];
}

export async function getProfile(
  outputFolder: string,
  profileId: number,
  ifsRoot?: string | null,
): Promise<InputProfileDetail> {
  if (!window.electron?.invoke) {
    throw new Error("Electron bridge is unavailable.");
  }
  return (await window.electron.invoke("profiles:get", {
    outputFolder,
    profileId,
    ifsRoot: ifsRoot ?? null,
  })) as InputProfileDetail;
}

export async function createProfile(
  outputFolder: string,
  ifsStaticId: number,
  name: string,
  description?: string | null,
): Promise<InputProfileMutationResponse> {
  if (!window.electron?.invoke) {
    throw new Error("Electron bridge is unavailable.");
  }
  return (await window.electron.invoke("profiles:create", {
    outputFolder,
    ifsStaticId,
    name,
    description: description ?? null,
  })) as InputProfileMutationResponse;
}

export async function updateProfileMeta(
  outputFolder: string,
  profileId: number,
  name?: string | null,
  description?: string | null,
): Promise<InputProfileDetail> {
  if (!window.electron?.invoke) {
    throw new Error("Electron bridge is unavailable.");
  }
  return (await window.electron.invoke("profiles:updateMeta", {
    outputFolder,
    profileId,
    name: name ?? null,
    description: description ?? null,
  })) as InputProfileDetail;
}

export async function saveProfileAs(
  outputFolder: string,
  profileId: number,
  name: string,
): Promise<InputProfileMutationResponse> {
  if (!window.electron?.invoke) {
    throw new Error("Electron bridge is unavailable.");
  }
  return (await window.electron.invoke("profiles:duplicate", {
    outputFolder,
    profileId,
    name,
  })) as InputProfileMutationResponse;
}

export async function deleteProfile(
  outputFolder: string,
  profileId: number,
): Promise<{ deleted_profile_id: number }> {
  if (!window.electron?.invoke) {
    throw new Error("Electron bridge is unavailable.");
  }
  return (await window.electron.invoke("profiles:delete", {
    outputFolder,
    profileId,
  })) as { deleted_profile_id: number };
}

export async function saveProfileParameters(
  outputFolder: string,
  profileId: number,
  rows: ParameterProfileRow[],
): Promise<InputProfileDetail> {
  if (!window.electron?.invoke) {
    throw new Error("Electron bridge is unavailable.");
  }
  return (await window.electron.invoke("profiles:saveParameters", {
    outputFolder,
    profileId,
    rows,
  })) as InputProfileDetail;
}

export async function saveProfileCoefficients(
  outputFolder: string,
  profileId: number,
  rows: CoefficientProfileRow[],
): Promise<InputProfileDetail> {
  if (!window.electron?.invoke) {
    throw new Error("Electron bridge is unavailable.");
  }
  return (await window.electron.invoke("profiles:saveCoefficients", {
    outputFolder,
    profileId,
    rows,
  })) as InputProfileDetail;
}

export async function saveProfileOutputs(
  outputFolder: string,
  profileId: number,
  rows: OutputProfileRow[],
): Promise<InputProfileDetail> {
  if (!window.electron?.invoke) {
    throw new Error("Electron bridge is unavailable.");
  }
  return (await window.electron.invoke("profiles:saveOutputs", {
    outputFolder,
    profileId,
    rows,
  })) as InputProfileDetail;
}

export async function saveProfileMLSettings(
  outputFolder: string,
  profileId: number,
  mlSettings: InputProfileMLSettings,
): Promise<InputProfileDetail> {
  if (!window.electron?.invoke) {
    throw new Error("Electron bridge is unavailable.");
  }
  return (await window.electron.invoke("profiles:saveMlSettings", {
    outputFolder,
    profileId,
    mlSettings,
  })) as InputProfileDetail;
}

export async function validateProfileSelection(
  outputFolder: string,
  profileId: number,
  ifsRoot?: string | null,
): Promise<Pick<InputProfileDetail, "profile" | "validation" | "ml_settings">> {
  if (!window.electron?.invoke) {
    throw new Error("Electron bridge is unavailable.");
  }
  return (await window.electron.invoke("profiles:validate", {
    outputFolder,
    profileId,
    ifsRoot: ifsRoot ?? null,
  })) as Pick<InputProfileDetail, "profile" | "validation" | "ml_settings">;
}

export async function getDesktopCapabilities(): Promise<DesktopCapabilities> {
  if (!window.electron?.getDesktopCapabilities) {
    return { ...DEFAULT_DESKTOP_CAPABILITIES };
  }

  try {
    const response = await window.electron.getDesktopCapabilities();
    return {
      trendAnalysis: Boolean(response?.trendAnalysis),
      openPath: Boolean(response?.openPath),
      trendDatasetOptions: Boolean(response?.trendDatasetOptions),
      imagePreview: Boolean(response?.imagePreview),
    };
  } catch (error) {
    if (isMissingIpcHandlerError(error, "desktop:getCapabilities")) {
      return { ...DEFAULT_DESKTOP_CAPABILITIES };
    }
    throw error;
  }
}

export async function getTrendDatasetOptions(
  outputDir: string,
): Promise<TrendDatasetOptionsData> {
  if (!window.electron?.getTrendDatasetOptions) {
    return {
      latest_dataset_id: null,
      dataset_ids: [],
      dataset_run_counts: {},
      latest_dataset_run_count: null,
    };
  }

  const response = await window.electron.getTrendDatasetOptions(outputDir);
  if (response?.status !== "success") {
    throw new Error(
      typeof response?.message === "string" && response.message.trim().length > 0
        ? response.message
        : "Unable to load trend-analysis dataset options.",
    );
  }

    return {
      latest_dataset_id:
        typeof response?.data?.latest_dataset_id === "string" ||
        response?.data?.latest_dataset_id === null
          ? response.data.latest_dataset_id
          : null,
      dataset_ids: Array.isArray(response?.data?.dataset_ids)
        ? response.data.dataset_ids.filter(
            (value): value is string => typeof value === "string" && value.trim().length > 0,
          )
        : [],
      dataset_run_counts:
        response?.data?.dataset_run_counts &&
        typeof response.data.dataset_run_counts === "object" &&
        !Array.isArray(response.data.dataset_run_counts)
          ? Object.entries(response.data.dataset_run_counts).reduce<Record<string, number>>(
              (counts, [key, value]) => {
                if (
                  typeof key === "string" &&
                  key.trim().length > 0 &&
                  typeof value === "number" &&
                  Number.isFinite(value) &&
                  value >= 0
                ) {
                  counts[key] = value;
                }
                return counts;
              },
              {},
            )
          : {},
      latest_dataset_run_count:
        typeof response?.data?.latest_dataset_run_count === "number" &&
        Number.isFinite(response.data.latest_dataset_run_count) &&
        response.data.latest_dataset_run_count >= 0
          ? response.data.latest_dataset_run_count
          : null,
    };
  }

export async function getArtifactImagePreview(
  targetPath: string,
  allowedRoot: string,
): Promise<ArtifactImagePreviewData> {
  if (!window.electron?.getImagePreview) {
    throw new Error("Image previews are unavailable in this desktop session.");
  }

  const response = await window.electron.getImagePreview(targetPath, allowedRoot);
  if (!response?.ok || typeof response.dataUrl !== "string") {
    throw new Error(
      typeof response?.error === "string" && response.error.trim().length > 0
        ? response.error
        : "Unable to load image preview.",
    );
  }

  return {
    dataUrl: response.dataUrl,
    mimeType:
      typeof response.mimeType === "string" && response.mimeType.trim().length > 0
        ? response.mimeType
        : "image/png",
    targetPath:
      typeof response.targetPath === "string" && response.targetPath.trim().length > 0
        ? response.targetPath
        : targetPath,
  };
}

export async function runTrendAnalysis(
  outputDir: string,
  {
    datasetId,
    limit,
    window: rollingWindow,
  }: {
    datasetId?: string | null;
    limit: number;
    window: number;
  },
): Promise<TrendAnalysisData> {
  if (!window.electron) {
    throw new Error(ELECTRON_BRIDGE_UNAVAILABLE_MESSAGE);
  }

  if (!window.electron.runTrendAnalysis) {
    throw new Error(TREND_ANALYSIS_UNAVAILABLE_MESSAGE);
  }

  const capabilities = await getDesktopCapabilities();
  if (!capabilities.trendAnalysis) {
    throw new Error(TREND_ANALYSIS_UNAVAILABLE_MESSAGE);
  }

  let response;
  try {
    response = await window.electron.runTrendAnalysis(
      outputDir,
      datasetId ?? null,
      limit,
      rollingWindow,
    );
  } catch (error) {
    throw normalizeTrendAnalysisInvokeError(error);
  }

  if (response?.status !== "success" || !response.data) {
    throw new Error(
      typeof response?.message === "string" && response.message.trim().length > 0
        ? response.message
        : "Trend analysis failed.",
    );
  }

  return response.data;
}

export async function openArtifactPath(targetPath: string): Promise<void> {
  if (!window.electron?.openPath) {
    throw new Error("Electron bridge is unavailable.");
  }

  const response = await window.electron.openPath(targetPath);
  if (!response?.ok) {
    throw new Error(
      typeof response?.error === "string" && response.error.trim().length > 0
        ? response.error
        : "Unable to open path.",
    );
  }
}
