import type { Sandbox, SandboxRequest, SandboxValue } from "./index.js";

/** A test callback receives the admitted bindings and cancellation. */
export type DirectSandboxCallback = (
  bindings: SandboxRequest["bindings"],
  signal: AbortSignal,
) => Promise<SandboxValue>;

/**
 * Dispatch a known-binding callback directly in this process. NOT isolation:
 * submitted code is ignored, and the callback retains all host privileges.
 * Use only trusted tests and examples, never model-authored code execution.
 */
export function directSandbox(callback: DirectSandboxCallback): Sandbox {
  return {
    execute: async (request, signal) => {
      signal.throwIfAborted();
      const result = await callback(request.bindings, signal);
      signal.throwIfAborted();
      return result;
    },
  };
}

/** A test transport that always refuses execution with the supplied message. */
export function rejectingSandbox(message: string): Sandbox {
  return directSandbox(() => Promise.reject(new Error(message)));
}
