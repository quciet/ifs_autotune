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
      invoke: <T = unknown, R = unknown>(channel: string, data?: T) => Promise<R>;
      on: (
        channel: string,
        listener: (...args: unknown[]) => void,
      ) => () => void;
    };
  }
}
