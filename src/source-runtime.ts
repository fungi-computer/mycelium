import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Scope from "effect/Scope";
import { z } from "zod";

import type {
  CapabilitySession,
  CapabilitySource,
  CapabilitySourceAcquisition,
  CapabilitySourceSnapshot,
} from "./capability-source.js";
import {
  compileSurface,
  type CompiledSurface,
  type SourceRevisionFrame,
} from "./compiler.js";
import {
  MyceliumDefinitionError,
  type MyceliumError,
  MyceliumSourceError,
} from "./errors.js";
import { createExecuteDoor, type ExecuteDoor } from "./execute-tool.js";
import type { AccessOptions } from "./access.js";
import type { BackingWorkTracker } from "./internal/execution-lifecycle.js";
import type { EffectSandbox } from "./runtime.js";
import type { ModuleRegistration } from "./registration.js";

/** Coherent candidate revision built from static and acquired modules. */
export type SourceCandidate = Readonly<{
  readonly compiled: CompiledSurface;
  readonly executeDoor: ExecuteDoor;
  readonly isStale: () => boolean;
  readonly close: () => Effect.Effect<void, MyceliumError>;
}>;

/** Inputs needed to build one off-to-the-side candidate revision. */
export type SourceCandidateOptions = Readonly<{
  readonly modules: readonly ModuleRegistration[];
  readonly sources: readonly CapabilitySource[];
  readonly session?: CapabilitySession | undefined;
  readonly sandbox: EffectSandbox;
  readonly backingWork: BackingWorkTracker;
  readonly precompiled?: CompiledSurface;
  readonly access: AccessOptions;
}>;

function compareCodeUnits(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}

const maximumCapabilitySources = 128;
const sourceIdSchema = z.string().min(1).max(160);
const sourceSnapshotSchema = z.object({
  version: z.string().min(1).max(160),
  modules: z.array(z.unknown()),
});
const maximumCandidateModules = 1_000;

function validateSources(
  input: readonly CapabilitySource[],
): readonly CapabilitySource[] {
  if (input.length > maximumCapabilitySources) {
    throw new MyceliumDefinitionError("Capability source count exceeds bound");
  }
  const sources = [...input].sort((left, right) =>
    compareCodeUnits(left.id, right.id),
  );
  const ids = new Set<string>();
  for (const source of sources) {
    if (!sourceIdSchema.safeParse(source.id).success) {
      throw new MyceliumDefinitionError(
        "Capability source id must be non-empty and bounded",
      );
    }
    if (ids.has(source.id)) {
      throw new MyceliumDefinitionError(
        `Duplicate capability source id: ${source.id}`,
      );
    }
    ids.add(source.id);
  }
  return sources;
}

function validateCandidateModuleCount(count: number): void {
  if (count > maximumCandidateModules) {
    throw new MyceliumDefinitionError("Candidate module count exceeds bound");
  }
}

function validateSourceSnapshot(
  snapshot: CapabilitySourceSnapshot,
  existingModuleCount: number,
): void {
  const parsed = sourceSnapshotSchema.safeParse(snapshot);
  if (!parsed.success) {
    throw new MyceliumDefinitionError(
      "Capability source snapshot must have a non-empty bounded version",
    );
  }
  validateCandidateModuleCount(
    existingModuleCount + parsed.data.modules.length,
  );
}

function acquireSourceIntoScope(
  source: CapabilitySource,
  candidateScope: Scope.Closeable,
  cleanupFailures: MyceliumSourceError[],
  invalidate: () => void,
  session: CapabilitySession | undefined,
): Effect.Effect<CapabilitySourceAcquisition, MyceliumSourceError> {
  return Effect.uninterruptibleMask((restore) =>
    Effect.gen(function* () {
      const release = (
        acquired: CapabilitySourceAcquisition,
      ): Effect.Effect<void> =>
        Effect.tryPromise({
          try: () => acquired.release(),
          catch: () => new MyceliumSourceError(source.id, "release"),
        }).pipe(
          Effect.catch((failure) =>
            Effect.sync(() => {
              cleanupFailures.push(failure);
            }),
          ),
        );
      const state = yield* Effect.try({
        try: () => {
          const controller = new AbortController();
          return {
            controller,
            outcome: source
              .acquire(
                session === undefined
                  ? { signal: controller.signal, invalidate }
                  : { signal: controller.signal, invalidate, session },
              )
              .then(
                (acquired) => Exit.succeed(acquired),
                () => Exit.fail(new MyceliumSourceError(source.id, "acquire")),
              ),
          };
        },
        catch: () => new MyceliumSourceError(source.id, "acquire"),
      });
      yield* Scope.addFinalizer(
        candidateScope,
        Effect.promise(() => state.outcome).pipe(
          Effect.flatMap((outcome) =>
            Exit.isSuccess(outcome) ? release(outcome.value) : Effect.void,
          ),
        ),
      );
      const outcome = yield* restore(Effect.promise(() => state.outcome)).pipe(
        Effect.onInterrupt(() =>
          Effect.sync(() => {
            state.controller.abort();
          }),
        ),
      );
      if (Exit.isFailure(outcome)) {
        return yield* Effect.failCause(outcome.cause);
      }
      return outcome.value;
    }),
  );
}

/** Acquire one complete candidate attempt without mutating installed state. */
function acquireCandidateAttempt(
  options: SourceCandidateOptions,
): Effect.Effect<SourceCandidate, MyceliumError> {
  return Effect.gen(function* () {
    const sources = yield* Effect.try({
      try: () => validateSources(options.sources),
      catch: (cause) =>
        cause instanceof MyceliumDefinitionError
          ? cause
          : new MyceliumDefinitionError("Capability sources are invalid"),
    });
    yield* Effect.try({
      try: () => {
        validateCandidateModuleCount(options.modules.length);
      },
      catch: (cause) =>
        cause instanceof MyceliumDefinitionError
          ? cause
          : new MyceliumDefinitionError(
              "Static capability modules are invalid",
            ),
    });
    const candidateScope = yield* Scope.make();
    const cleanupFailures: MyceliumSourceError[] = [];
    let stale = false;
    const modules = [...options.modules];
    const sourceFrames: SourceRevisionFrame[] = [];

    const acquireAll = Effect.gen(function* () {
      for (const source of sources) {
        const acquired = yield* acquireSourceIntoScope(
          source,
          candidateScope,
          cleanupFailures,
          () => {
            stale = true;
          },
          options.session,
        );
        yield* Effect.try({
          try: () => {
            validateSourceSnapshot(acquired.snapshot, modules.length);
          },
          catch: (cause) =>
            cause instanceof MyceliumDefinitionError
              ? cause
              : new MyceliumDefinitionError(
                  "Capability source snapshot is invalid",
                ),
        });
        modules.push(...acquired.snapshot.modules);
        sourceFrames.push({
          id: source.id,
          version: acquired.snapshot.version,
        });
      }

      const compiled =
        options.precompiled ??
        (yield* Effect.try({
          try: () => compileSurface(modules, sourceFrames),
          catch: (cause) =>
            cause instanceof MyceliumDefinitionError
              ? cause
              : new MyceliumDefinitionError("Capability compilation failed"),
        }));
      const backingWork = options.backingWork.createGroup();
      const executeDoor = createExecuteDoor(
        compiled.operations,
        compiled.catalog,
        options.sandbox,
        backingWork,
        compiled.moduleAliases,
        compiled.namespacePaths,
        options.session?.sessionId ?? null,
        options.access,
      );
      let closed = false;
      const close = () =>
        Effect.gen(function* () {
          if (!closed) {
            closed = true;
            yield* backingWork.awaitIdle;
            yield* Scope.close(candidateScope, Exit.void);
          }
          const cleanupFailure = cleanupFailures[0];
          if (cleanupFailure !== undefined) yield* Effect.fail(cleanupFailure);
        });

      return Object.freeze({
        compiled,
        executeDoor,
        isStale: () => stale,
        close,
      });
    });

    return yield* acquireAll.pipe(
      Effect.onExit((exit) =>
        Exit.isSuccess(exit) ? Effect.void : Scope.close(candidateScope, exit),
      ),
    );
  });
}

const maximumCandidateAttempts = 2;

/** Acquire one non-stale candidate or fail after one bounded retry. */
export function acquireSourceCandidate(
  options: SourceCandidateOptions,
): Effect.Effect<SourceCandidate, MyceliumError> {
  return Effect.gen(function* () {
    for (let attempt = 0; attempt < maximumCandidateAttempts; attempt += 1) {
      const candidate = yield* acquireCandidateAttempt(options);
      if (!candidate.isStale()) return candidate;
      yield* candidate.close();
    }
    return yield* Effect.fail(new MyceliumSourceError(undefined, "stale"));
  });
}
