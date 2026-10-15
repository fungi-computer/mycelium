import type { input, output, ZodJSONSchema, ZodUnknown } from "zod";

/** JSON value allowed to cross the sandbox and capability boundary. */
export type MyceliumJsonValue = output<ZodJSONSchema>;

/** Untrusted value supplied by or returned from a host sandbox boundary. */
export type SandboxValue = input<ZodUnknown>;

/** One invocation binding visible to the host-provided sandbox. */
export type SandboxBinding = (
  input: SandboxValue,
) => Promise<MyceliumJsonValue>;

/** Request crossing the root Promise-shaped sandbox seam. */
export type SandboxRequest = Readonly<{
  /** Fresh identity shared by the request and every bound operation call. */
  executionId: string;
  /** Model-authored program to evaluate inside the host sandbox. */
  code: string;
  /** Effective Mycelium execution budget; cancellation remains authoritative. */
  timeoutMs: number;
  /** Transport aliases mapped to flat relative operation paths. */
  bindings: Readonly<Record<string, Readonly<Record<string, SandboxBinding>>>>;
  /** Logical namespace path for each transport alias; defaults to `[alias]`. */
  namespacePaths?: Readonly<Record<string, readonly string[]>>;
  /**
   * Every bound execution alias mapped to its stable module id. Always
   * present on a request built by `runExecution`; optional only so a
   * hand-built request from an older call site keeps compiling.
   */
  modules?: Readonly<Record<string, string>>;
  /**
   * Stable Session identity for this run; null for session-free hosts.
   * Always present on a request built by `runExecution`; optional only so a
   * hand-built request from an older call site keeps compiling.
   */
  sessionId?: string | null;
}>;

/** Promise-shaped sandbox interface implemented by a host or stranger. */
export type Sandbox = Readonly<{
  /** Operator identity of this transport. */
  adapter?: string;
  /** Source revision supplied by the host build. */
  buildSha?: string;
  /** Evaluate one bounded program and settle only after cancellation is observed. */
  execute: (
    request: SandboxRequest,
    signal: AbortSignal,
  ) => Promise<SandboxValue>;
}>;
