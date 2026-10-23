import type { SandboxFailureEvent } from "./sandbox-failure-event.js";
import type { SandboxFailure } from "./sandbox-failure.js";
import type { MuleTool } from "@fungi.computer/mule/effect";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Semaphore from "effect/Semaphore";
import * as Exit from "effect/Exit";
import * as Scope from "effect/Scope";

import type {
  CapabilitySession,
  CapabilitySource,
} from "./capability-source.js";
import { compileSurface, type CompiledSurface } from "./compiler.js";
import {
  MyceliumClosedError,
  MyceliumDefinitionError,
  type MyceliumError,
} from "./errors.js";
import {
  createBackingWorkTracker,
  normalizeExecutionPolicy,
} from "./internal/execution-lifecycle.js";
import type { ExecutionOptions } from "./execute-tool.js";
import type { ExecuteDoor } from "./execute-tool.js";
import type { ExecutionPolicy } from "./internal/execution-lifecycle.js";
import type { SandboxRequest, SandboxValue } from "./index.js";
import type { ModuleRegistration } from "./registration.js";
import { guardTool, type AccessOptions, type LabelledTool } from "./access.js";
import {
  acquireSourceCandidate,
  type SourceCandidate,
} from "./source-runtime.js";
import { claimThenSettle } from "./semaphore-transition.js";

/** Effect-native host seam for sandbox execution. */
export type EffectSandbox = Readonly<{
  readonly adapter?: string | undefined;
  readonly buildSha?: string | undefined;
  readonly onFailure?:
    ((event: SandboxFailureEvent) => Effect.Effect<void>) | undefined;
  readonly execute: (
    request: SandboxRequest,
    signal: AbortSignal,
  ) => Effect.Effect<SandboxValue, SandboxFailure>;
  /** Effective policy injected by the canonical runtime before compilation. */
  readonly execution?: ExecutionPolicy;
}>;

/** Inputs consumed by the canonical runtime core. */
export type CoreOptions = Readonly<{
  readonly modules: readonly ModuleRegistration[];
  readonly sources: readonly CapabilitySource[];
  readonly session?: CapabilitySession | undefined;
  readonly execution?: ExecutionOptions | undefined;
  readonly sandbox: EffectSandbox;
  readonly onSandboxFailure?:
    ((event: SandboxFailureEvent) => Effect.Effect<void>) | undefined;
  readonly tools: readonly LabelledTool[];
  readonly access: AccessOptions;
}>;

/** Internal lease carrying the stable surface and its Effect execute door. */
export type CoreLease = Readonly<{
  readonly revision: string;
  readonly catalog: CompiledSurface["catalog"];
  readonly executeDoor: ExecuteDoor;
  readonly effectExecuteTool: MuleTool;
  /** Labelled direct tools, each behind the runtime's access check. */
  readonly tools: readonly MuleTool[];
  readonly release: () => Effect.Effect<void, MyceliumError>;
}>;

/** Canonical scoped runtime used by both public lifecycle projections. */
export type CoreRuntime = Readonly<{
  readonly acquire: () => Effect.Effect<CoreLease, MyceliumError, Scope.Scope>;
  readonly close: () => Effect.Effect<void, MyceliumError>;
}>;

type InstalledRevision = {
  readonly candidate: SourceCandidate;
  readonly settled: Deferred.Deferred<undefined, MyceliumError>;
  leases: number;
  retired: boolean;
  closing: boolean;
};

type ClosePlan = Readonly<{
  readonly installed: InstalledRevision;
  readonly runCleanup: boolean;
}>;

function finishCleanup(
  installed: InstalledRevision,
  liveRevisions: Set<InstalledRevision>,
): Effect.Effect<void, MyceliumError> {
  // A failed revision stays live, so a repeated close reports its retained
  // cleanup failure instead of a fresh success.
  return Effect.uninterruptible(
    installed.candidate.close().pipe(
      Effect.matchEffect({
        onFailure: (failure) =>
          Deferred.fail(installed.settled, failure).pipe(
            Effect.andThen(Effect.fail(failure)),
          ),
        onSuccess: () =>
          Deferred.succeed(installed.settled, undefined).pipe(
            Effect.andThen(
              Effect.sync(() => {
                liveRevisions.delete(installed);
              }),
            ),
          ),
      }),
    ),
  );
}

function settleClosePlan(
  plan: ClosePlan,
  liveRevisions: Set<InstalledRevision>,
): Effect.Effect<void, MyceliumError> {
  return plan.runCleanup
    ? finishCleanup(plan.installed, liveRevisions)
    : Deferred.await(plan.installed.settled);
}

function settleClosePlans(
  plans: readonly ClosePlan[],
  liveRevisions: Set<InstalledRevision>,
): Effect.Effect<void, MyceliumError> {
  return Effect.all(
    plans.map((plan) => settleClosePlan(plan, liveRevisions).pipe(Effect.exit)),
    { concurrency: "unbounded" },
  ).pipe(
    Effect.flatMap((exits) => {
      const failure = exits.find(Exit.isFailure);
      return failure === undefined
        ? Effect.void
        : Effect.failCause(failure.cause);
    }),
  );
}

/** Construct the single Effect-owned lifecycle implementation. */
export function openCoreRuntime(
  options: CoreOptions,
): Effect.Effect<CoreRuntime, MyceliumError, Scope.Scope> {
  return Effect.gen(function* () {
    const gate = yield* Semaphore.make(1);
    const execution = yield* Effect.try({
      try: () => normalizeExecutionPolicy(options.execution),
      catch: (cause) =>
        cause instanceof MyceliumDefinitionError
          ? cause
          : new MyceliumDefinitionError("Execution policy is invalid"),
    });
    const backingWork = yield* createBackingWorkTracker(execution.capacity);
    const sandbox: EffectSandbox = Object.freeze({
      execute: options.sandbox.execute,
      adapter: options.sandbox.adapter,
      buildSha: options.sandbox.buildSha,
      onFailure: options.onSandboxFailure,
      execution,
    });
    const staticSurface = yield* Effect.try({
      try: () => compileSurface(options.modules),
      catch: (cause) =>
        cause instanceof MyceliumDefinitionError
          ? cause
          : new MyceliumDefinitionError("Capability compilation failed"),
    });
    const tools = yield* Effect.try({
      try: () =>
        Object.freeze(
          options.tools.map((tool) =>
            guardTool(tool, options.access, {
              sessionId: options.session?.sessionId ?? null,
            }),
          ),
        ),
      catch: (cause) =>
        cause instanceof MyceliumDefinitionError
          ? cause
          : new MyceliumDefinitionError("Direct tool labels are invalid"),
    });
    let installed: InstalledRevision | undefined;
    const liveRevisions = new Set<InstalledRevision>();
    let closed = false;

    if (options.sources.length === 0) {
      const candidate = yield* acquireSourceCandidate({
        ...options,
        sandbox,
        backingWork,
        precompiled: staticSurface,
      });
      const initial: InstalledRevision = {
        candidate,
        settled: yield* Deferred.make<undefined, MyceliumError>(),
        leases: 0,
        retired: false,
        closing: false,
      };
      installed = initial;
      liveRevisions.add(initial);
    }

    const close = (): Effect.Effect<void, MyceliumError> =>
      claimThenSettle(
        gate,
        (): readonly ClosePlan[] => {
          closed = true;
          return [...liveRevisions].map((revision) => {
            revision.retired = true;
            const runCleanup = revision.leases === 0 && !revision.closing;
            if (runCleanup) revision.closing = true;
            return { installed: revision, runCleanup };
          });
        },
        (plans) => settleClosePlans(plans, liveRevisions),
      );
    yield* Effect.addFinalizer(() => close().pipe(Effect.orDie));

    const acquire = (): Effect.Effect<CoreLease, MyceliumError, Scope.Scope> =>
      gate.withPermits(1)(
        Effect.gen(function* () {
          if (closed) {
            return yield* Effect.fail(new MyceliumClosedError());
          }
          if (installed === undefined || installed.candidate.isStale()) {
            const candidate = yield* acquireSourceCandidate({
              ...options,
              sandbox,
              backingWork,
            });
            const previous = installed;
            const replacement: InstalledRevision = {
              candidate,
              settled: yield* Deferred.make<undefined, MyceliumError>(),
              leases: 0,
              retired: false,
              closing: false,
            };
            installed = replacement;
            liveRevisions.add(replacement);
            if (previous !== undefined) {
              previous.retired = true;
              if (previous.leases === 0 && !previous.closing) {
                previous.closing = true;
                yield* finishCleanup(previous, liveRevisions);
              }
            }
          }
          const owned = installed;
          owned.leases += 1;
          // Undefined while held; afterwards, whether this release began cleanup.
          let releaseCleanup: boolean | undefined;
          const release = (): Effect.Effect<void, MyceliumError> =>
            claimThenSettle(
              gate,
              (): ClosePlan | undefined => {
                // A retry awaits the cleanup this lease began, if any.
                if (releaseCleanup !== undefined) {
                  return releaseCleanup
                    ? { installed: owned, runCleanup: false }
                    : undefined;
                }
                owned.leases -= 1;
                releaseCleanup =
                  owned.retired && owned.leases === 0 && !owned.closing;
                if (!releaseCleanup) return undefined;
                owned.closing = true;
                return { installed: owned, runCleanup: true };
              },
              (plan) =>
                plan === undefined
                  ? Effect.void
                  : settleClosePlan(plan, liveRevisions),
            );
          yield* Effect.addFinalizer(() => release().pipe(Effect.orDie));
          return Object.freeze({
            revision: owned.candidate.compiled.revision,
            catalog: owned.candidate.compiled.catalog,
            executeDoor: owned.candidate.executeDoor,
            effectExecuteTool: owned.candidate.executeDoor.effectTool,
            tools,
            release,
          });
        }),
      );

    return Object.freeze({ acquire, close });
  });
}
