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
