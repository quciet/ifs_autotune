export {};

declare global {
  interface Window {
    electron?: {
      selectFolder: () => Promise<string | null>;
      invoke: <T = unknown, R = unknown>(channel: string, data?: T) => Promise<R>;
      on: (
        channel: string,
        listener: (...args: unknown[]) => void,
      ) => () => void;
      getDefaultOutputDir: () => Promise<string>;
    };
  }
}
