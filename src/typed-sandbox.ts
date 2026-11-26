import * as Effect from "effect/Effect";

import { sandboxFailure } from "./sandbox-boundary.js";
import type { SandboxFailure } from "./sandbox-failure.js";
import type {
  Sandbox as PromiseSandbox,
  SandboxRequest,
  SandboxValue,
} from "./index.js";

// Kept out of effect.ts, which pulls the sandbox compiler and its script
// parser: a browser client that only adapts a Promise sandbox imports here.

/**
 * Effect-native host seam for sandbox execution.
 *
 * Interrupting `execute` must not complete until the backing work has actually
 * settled. Mycelium bounds that wait by the acknowledgement grace and reports
 * `abort_unacknowledged` when it expires, keeping the work's source revision
 * alive until it exits. A host with Promise-shaped work adapts it with
 * {@link fromTypedSandbox}; `Effect.tryPromise` alone completes interruption
 * without awaiting the Promise and so breaks that settlement law.
 */
export type Sandbox = Readonly<{
  /**
   * Evaluate one bounded program within the caller's Effect scope. `signal` is
   * Mycelium's invocation abort projection for Promise-shaped host work.
   */
  execute: (
    request: SandboxRequest,
    signal: AbortSignal,
  ) => Effect.Effect<SandboxValue, SandboxFailure>;
  /** Operator identity supplied by the host. */
  adapter?: string;
  /** Source revision embedded by the build, absent in an unpacked source run. */
  buildSha?: string | undefined;
}>;

/**
 * Adapt a Promise-shaped sandbox to the Effect seam. This is Mycelium's one
 * Promise sandbox conversion: interruption waits for the host Promise to
 * settle, while cancellation reaches the host through the abort signal.
 */
export function fromTypedSandbox(sandbox: PromiseSandbox): Sandbox {
  return Object.freeze({
    adapter: sandbox.adapter ?? "promise",
    buildSha: sandbox.buildSha,
    execute: (request: SandboxRequest, signal: AbortSignal) =>
      Effect.uninterruptible(
        Effect.tryPromise({
          try: () => sandbox.execute(request, signal),
          catch: sandboxFailure,
        }),
      ),
  });
}
