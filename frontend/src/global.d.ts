export {};

declare global {
  interface Window {
    electron?: {
      selectFolder: () => Promise<string | null>;
      invoke: <T = unknown, R = unknown>(channel: string, data?: T) => Promise<R>;
    };
  }
}
