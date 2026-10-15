import { sandboxFailure } from "./sandbox-boundary.js";
import type { SandboxFailureEvent } from "./sandbox-failure-event.js";
import type { ForageCatalog } from "@fungi.computer/forage";
import type { MuleTool } from "@fungi.computer/mule/effect";
import * as Effect from "effect/Effect";
import * as Scope from "effect/Scope";

import { type SandboxFailure } from "./sandbox-failure.js";
import type { MyceliumError } from "./errors.js";
import type { ExecutionOptions } from "./execute-tool.js";
import type {
  CapabilitySession,
  CapabilitySource,
  ModuleRegistration,
  Sandbox as PromiseSandbox,
  SandboxBinding,
  SandboxRequest,
  SandboxValue,
} from "./index.js";
import { openCoreRuntime } from "./runtime.js";
import type { AccessOptions, LabelledTool } from "./access.js";

export type {
  CapabilitySession,
  CapabilitySource,
  ModuleRegistration,
  SandboxBinding,
  SandboxRequest,
};
export type { ExecutionOptions } from "./execute-tool.js";
export type {
  Access,
  AccessContext,
  AccessLabel,
  AccessOptions,
  AccessRefusal,
  AccessRefusalReason,
  ConfirmRequest,
  Grant,
  Grants,
  LabelledTool,
  ToolPolicy,
} from "./access.js";

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

/** Inputs used to construct the canonical Effect runtime. */
export type MyceliumOptions = Readonly<{
  /** Unordered module registrations compiled into one immutable revision. */
  modules?: readonly ModuleRegistration[];
  /** Versioned host projections reconciled into immutable revisions. */
  sources?: readonly CapabilitySource[];
  /** Session every source acquisition is made for; absent for session-free hosts. */
  session?: CapabilitySession;
  /** Bounded Mycelium-owned deadline and cancellation acknowledgement policy. */
  execution?: ExecutionOptions;
  /** Host-owned Effect boundary for model-authored programs. */
  sandbox: Sandbox;
  /** Operator-only failure sink; never contributes a model message. */
  onSandboxFailure?: (event: SandboxFailureEvent) => Effect.Effect<void>;
  /** Labelled direct tools offered beside execute under the same access check. */
  tools?: readonly LabelledTool[];
  /** Host grants and approval, checked against each call's labels. */
  access?: AccessOptions;
}>;

/** One immutable Effect-facing capability revision. */
export type Lease = Readonly<{
  /** Installed capability revision captured by this immutable lease. */
  revision: string;
  /** Bounded discovery catalog for the captured revision. */
  catalog: ForageCatalog;
  /** Single Effect-native Mule tool for discovery and scoped execution. */
  executeTool: MuleTool;
  /** The runtime's labelled direct tools, each behind the access check. */
  tools: readonly MuleTool[];
  /**
   * Release explicitly with a typed failure. The owning scope also awaits this
   * cleanup; automatic-finalizer failure terminates that scope as the same
   * identifiable defect because Effect finalizers are infallible.
   */
  release: () => Effect.Effect<void, MyceliumError>;
}>;

/** Warm Effect lifecycle for acquiring scope-owned run leases. */
export type Runtime = Readonly<{
  /** Acquire one immutable lease owned by the caller's Effect scope. */
  acquire: () => Effect.Effect<Lease, MyceliumError, Scope.Scope>;
  /**
   * Stop acquisition and await cleanup with a typed failure when called
   * explicitly. Automatic scope cleanup retains a failure as an identifiable
   * defect under Effect's infallible-finalizer contract.
   */
  close: () => Effect.Effect<void, MyceliumError>;
}>;

/** Construct the canonical scoped Effect runtime. */
export function make(
  options: MyceliumOptions,
): Effect.Effect<Runtime, MyceliumError, Scope.Scope> {
  return openCoreRuntime({
    modules: options.modules ?? [],
    sources: options.sources ?? [],
    session: options.session,
    execution: options.execution,
    sandbox: options.sandbox,
    onSandboxFailure: options.onSandboxFailure,
    tools: options.tools ?? [],
    access: options.access ?? {},
  }).pipe(
    Effect.map((runtime) =>
      Object.freeze({
        acquire: () =>
          runtime.acquire().pipe(
            Effect.map((lease) =>
              Object.freeze({
                revision: lease.revision,
                catalog: lease.catalog,
                executeTool: lease.effectExecuteTool,
                tools: lease.tools,
                release: lease.release,
              }),
            ),
          ),
        close: runtime.close,
      }),
    ),
  );
}
