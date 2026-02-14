export {};

declare global {
  interface Window {
    electron?: {
      selectFolder: (
        type: 'ifs' | 'output',
        defaultPath?: string | null,
      ) => Promise<string | null>;
      selectFile: (defaultPath?: string | null) => Promise<string | null>;
      getDefaultOutputDir: () => Promise<string>;
      getDefaultInputFile: () => Promise<string>;
      getMLJobStatus: () => Promise<{
        running: boolean;
        startedAt: number | null;
        pid: number | null;
        progress: { done?: number; total?: number; text?: string } | null;
        lastUpdateAt: number | null;
        exitCode: number | null;
        error: string | null;
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
