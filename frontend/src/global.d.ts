export {};

declare global {
  type MLTerminationReason = "completed" | "stopped_gracefully";

  interface MLFinalResult {
    best_model_id?: string | null;
    best_fit_pooled?: number | null;
    iterations?: number | null;
  }

  interface MLProgressTrial {
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
  }

  interface TrendSummaryData {
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
  }

  interface TrendAnalysisData {
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
  }

  interface DesktopCapabilities {
    trendAnalysis: boolean;
    openPath: boolean;
    trendDatasetOptions?: boolean;
    imagePreview?: boolean;
  }

  interface TrendDatasetOptionsData {
    latest_dataset_id: string | null;
    dataset_ids: string[];
    dataset_run_counts: Record<string, number>;
    latest_dataset_run_count: number | null;
  }

  interface ArtifactImagePreviewData {
    dataUrl: string;
    mimeType: string;
    targetPath: string;
  }

  interface Window {
    electron?: {
      selectFolder: (
        type: 'ifs' | 'output',
        defaultPath?: string | null,
      ) => Promise<string | null>;
      selectFile: (defaultPath?: string | null) => Promise<string | null>;
      getDesktopCapabilities: () => Promise<DesktopCapabilities>;
      getDefaultOutputDir: () => Promise<string>;
      getDefaultInputFile: () => Promise<string>;
      getTrendDatasetOptions: (outputDir?: string | null) => Promise<{
        status?: string;
        stage?: string;
        message?: string;
        data?: TrendDatasetOptionsData;
      }>;
      getImagePreview: (
        targetPath?: string | null,
        allowedRoot?: string | null,
      ) => Promise<{
        ok: boolean;
        error?: string | null;
        dataUrl?: string | null;
        mimeType?: string | null;
        targetPath?: string | null;
      }>;
      getMLJobStatus: () => Promise<{
        running: boolean;
        startedAt: number | null;
        pid: number | null;
        progress: { done?: number; total?: number; text?: string } | null;
        lastUpdateAt: number | null;
        exitCode: number | null;
        error: string | null;
        ifsPath: string | null;
        ifsValidated: boolean;
        inputExcelPath: string | null;
        outputDir: string | null;
        stopRequested: boolean;
        stopAcknowledged: boolean;
        finalResult: MLFinalResult | null;
        terminationReason: MLTerminationReason | null;
        runConfig: {
          endYear?: number | string | null;
          baseYear?: number | null;
          initialModelId?: string | number | null;
          datasetId?: string | null;
        } | null;
      }>;
      runTrendAnalysis: (
        outputDir?: string | null,
        datasetId?: string | null,
        limit?: number | null,
        window?: number | null,
      ) => Promise<{
        status?: string;
        stage?: string;
        message?: string;
        data?: TrendAnalysisData;
      }>;
    getMLProgressHistory: (
      outputDir?: string | null,
      datasetId?: string | null,
      modelId?: string | null,
      sinceRunId?: number | null,
    ) => Promise<{
      status?: string;
      stage?: string;
      message?: string;
      data?: {
        dataset_id?: string | null;
        reference_model_id?: string | null;
        reference_fit_pooled?: number | null;
        latest_run_id?: number | null;
        latest_progress_rowid?: number | null;
        latest_output_rowid?: number | null;
        trials?: MLProgressTrial[];
      };
    }>;
      openPath: (targetPath?: string | null) => Promise<{
        ok: boolean;
        error?: string | null;
      }>;
      requestMLStop: () => Promise<{
        accepted: boolean;
        alreadyRequested?: boolean;
        stopRequested: boolean;
        stopAcknowledged: boolean;
      }>;
      invoke: <T = unknown, R = unknown>(channel: string, data?: T) => Promise<R>;
      onMLProgress: (callback: (line: string) => void) => () => void;
      onMLLog: (callback: (line: string) => void) => () => void;
      on: (
        channel: string,
        listener: (...args: unknown[]) => void,
      ) => () => void;
    };
  }
}
