import {
  sandboxFailure,
  isSandboxFailure,
  sandboxOperatorValue,
  sandboxValueKind,
} from "../sandbox-boundary.js";
import type { SandboxFailureEvent } from "../sandbox-failure-event.js";
import { sandboxBuildSha } from "#build-identity";
import * as Metric from "effect/Metric";
import {
  referenceSandboxFailure,
  DeadlineExceeded,
  Cancelled,
  ResultTooLarge,
  ResultNotJson,
  CapabilityFailed,
  type SandboxFailure,
} from "../sandbox-failure.js";
import { ForageError } from "@fungi.computer/forage";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import { z } from "zod";

import type { ExecutionOptions } from "../execute-tool.js";
import {
  InvalidOperationInput,
  MyceliumDefinitionError,
  type MyceliumError,
} from "../errors.js";
import type {
  InvocationContext,
  MyceliumJsonValue,
  SandboxRequest,
  SandboxValue,
} from "../index.js";
import type { ExecutionImages } from "../image-collector.js";
import type { EffectSandbox } from "../runtime.js";

const jsonValueSchema = z.json();
const jsonScalarSchema = z.union([
  z.null(),
  z.string(),
  z.boolean(),
  z.number(),
]);
const jsonRecordSchema = z.record(z.string(), z.unknown());
const executionOptionsSchema = z
  .object({
    timeoutMs: z.number().int().min(1).max(900_000).optional(),
    abortGraceMs: z.number().int().min(1).max(30_000).optional(),
  })
  .strict()
  .optional();
const maximumJsonDepth = 64;
const maximumSerializedJsonBytes = 256 * 1024;
const maximumBackingExecutions = 8;
const utf8 = new TextEncoder();

export type ExecutionPolicy = Readonly<{
  readonly timeoutMs: number;
  readonly abortGraceMs: number;
}>;

export type BackingWorkGroup = Readonly<{
  readonly start: <A, E>(
    effect: Effect.Effect<A, E>,
  ) => Effect.Effect<Fiber.Fiber<A, E>, SandboxFailure>;
  readonly awaitIdle: Effect.Effect<void>;
}>;

export type BackingWorkTracker = Readonly<{
  readonly createGroup: () => BackingWorkGroup;
}>;

const resumeIdle = (notify: (effect: Effect.Effect<undefined>) => void) => {
  notify(Effect.succeed(undefined));
};

export function normalizeExecutionPolicy(
  value: ExecutionOptions | undefined,
): ExecutionPolicy {
  const parsed = executionOptionsSchema.safeParse(value);
  if (!parsed.success) {
    throw new MyceliumDefinitionError("Execution policy is invalid");
  }
  return Object.freeze({
    timeoutMs: parsed.data?.timeoutMs ?? 180_000,
    abortGraceMs: parsed.data?.abortGraceMs ?? 5_000,
  });
}

export function createBackingWorkTracker(): BackingWorkTracker {
  let active = 0;
  return Object.freeze({
    createGroup: (): BackingWorkGroup => {
      let groupActive = 0;
      const waiters = new Set<(effect: Effect.Effect<undefined>) => void>();
      const awaitIdle = Effect.callback<undefined>((resume) => {
        if (groupActive === 0) {
          resume(Effect.succeed(undefined));
          return Effect.sync(() => undefined);
        }
        waiters.add(resume);
        return Effect.sync(() => waiters.delete(resume));
      });
      return Object.freeze({
        start: <A, E>(
          effect: Effect.Effect<A, E>,
        ): Effect.Effect<Fiber.Fiber<A, E>, SandboxFailure> =>
          Effect.uninterruptibleMask((restore) =>
            Effect.sync(() => {
              if (active >= maximumBackingExecutions) return false;
              active += 1;
              groupActive += 1;
              return true;
            }).pipe(
              Effect.flatMap((reserved) =>
                reserved
                  ? Effect.forkDetach(
                      restore(effect).pipe(
                        Effect.ensuring(
                          Effect.sync(() => {
                            active -= 1;
                            groupActive -= 1;
                            if (groupActive === 0) {
                              const pending = [...waiters];
                              waiters.clear();
                              pending.forEach(resumeIdle);
                            }
                          }),
                        ),
                      ),
                    )
                  : Effect.fail(
                      new CapabilityFailed({
                        namespace: "execute",
                        operation: "admission",
                        cause: {
                          kind: "Unavailable",
                          message: "Sandbox execution capacity unavailable",
                        },
                      }),
                    ),
              ),
            ),
          ),
        awaitIdle,
      });
    },
  });
}

function normalizeJson(value: SandboxValue, depth = 0): SandboxValue {
  if (depth > maximumJsonDepth) {
    throw new Error("JSON value exceeds the nesting bound");
  }
  const scalar = jsonScalarSchema.safeParse(value);
  if (scalar.success) return scalar.data;
  if (Array.isArray(value))
    return value.map((item) => normalizeJson(item, depth + 1));
  const record = jsonRecordSchema.safeParse(value);
  if (!record.success) throw record.error;
  return Object.fromEntries(
    Object.entries(record.data)
      .filter(([, item]) => item !== undefined)
      .map(([key, item]) => [key, normalizeJson(item, depth + 1)]),
  );
}

function jsonValue(value: SandboxValue): MyceliumJsonValue {
  return jsonValueSchema.parse(normalizeJson(value));
}

export type SerializedSandboxValue = Readonly<{ readonly text: string }>;

function serializeSandboxValue(value: SandboxValue): SerializedSandboxValue {
  const text = JSON.stringify(jsonValue(value));
  const size = utf8.encode(text).byteLength;
  if (size > maximumSerializedJsonBytes) {
    throw new ResultTooLarge({ size });
  }
  return Object.freeze({ text });
}

export type RawBindings = Readonly<
  Record<
    string,
    Readonly<Record<string, (input: SandboxValue) => Promise<SandboxValue>>>
  >
>;

function guardedBindings(
  raw: RawBindings,
  admit: () => void,
): SandboxRequest["bindings"] {
  return Object.freeze(
    Object.fromEntries(
      Object.entries(raw).map(([namespace, operations]) => [
        namespace,
        Object.freeze(
          Object.fromEntries(
            Object.entries(operations).map(([name, operation]) => [
              name,
              async (input: SandboxValue) => {
                admit();
                try {
                  return jsonValue(await operation(input));
                } catch (cause) {
                  if (isSandboxFailure(cause)) throw cause;
                  if (cause instanceof ForageError)
                    throw new CapabilityFailed({
                      namespace,
                      operation: name,
                      cause: { kind: "Forage", message: cause.message },
                      rawCause: cause,
                    });
                  if (cause instanceof InvalidOperationInput)
                    throw new CapabilityFailed({
                      namespace,
                      operation: name,
                      cause: {
                        kind: "InvalidInput",
                        message: `Invalid operation input: ${z.prettifyError({ issues: cause.issues })}; use forage.describe`,
                      },
                      rawCause: cause,
                    });
                  // Capability owners/adapters may admit their own typed diagnostics (Files, etc.).
                  throw cause;
                }
              },
            ]),
          ),
        ),
      ]),
    ),
  );
}

function waitForAbort(signal: AbortSignal): Effect.Effect<void> {
  return Effect.callback((resume) => {
    const onAbort = resume.bind(undefined, Effect.void);
    if (signal.aborted) onAbort();
    else signal.addEventListener("abort", onAbort, { once: true });
    return Effect.sync(
      signal.removeEventListener.bind(signal, "abort", onAbort),
    );
  });
}

function forwardAbort(
  source: AbortSignal | undefined,
  onAbort: () => void,
): () => void {
  if (source === undefined) return () => undefined;
  const forward = onAbort.bind(undefined);
  if (source.aborted) {
    forward();
    return () => undefined;
  }
  source.addEventListener("abort", forward, { once: true });
  return source.removeEventListener.bind(source, "abort", forward);
}

export type ExecutionLifecycleRequest = Readonly<{
  readonly toolCallId?: string | undefined;
  /** This run's identity, minted by the execute door that drains its images. */
  readonly executionId: string;
  /** This run's slot in the door's collector, exposed through the context. */
  readonly images: ExecutionImages;
  readonly code: string;
  readonly callerSignal: AbortSignal | undefined;
  readonly policy: ExecutionPolicy;
  readonly sandbox: EffectSandbox;
  readonly backingWork: BackingWorkGroup;
  readonly createBindings: (context: InvocationContext) => RawBindings;
  /** Every bound execution alias mapped to its stable module id. */
  readonly modules: Readonly<Record<string, string>>;
  readonly namespacePaths: Readonly<Record<string, readonly string[]>>;
  /** Stable Session identity for this run; null for session-free hosts. */
  readonly sessionId: string | null;
}>;

function executionFailure(
  cancellation: "cancelled" | "deadline_exceeded" | undefined,
  timeoutMs: number,
  elapsedMs?: number,
): DeadlineExceeded | Cancelled {
  return cancellation === "deadline_exceeded"
    ? new DeadlineExceeded({ limitMs: timeoutMs, elapsedMs: elapsedMs ?? 0 })
    : new Cancelled({ by: "caller" });
}

/** Wall time since `startedAt`, read through Effect's Clock so tests using TestClock stay deterministic. */
function elapsedSince(startedAt: number): Effect.Effect<number> {
  return Clock.currentTimeMillis.pipe(Effect.map((now) => now - startedAt));
}

export function runExecution(
  request: ExecutionLifecycleRequest,
): Effect.Effect<SerializedSandboxValue, MyceliumError> {
  return Clock.currentTimeMillis.pipe(
    Effect.flatMap((startedAt) =>
      Effect.suspend(() => {
        const executionId = request.executionId;
        const controller = new AbortController();
        let cancellation: "cancelled" | "deadline_exceeded" | undefined;
        const cancel = (phase: "cancelled" | "deadline_exceeded") => {
          if (cancellation === undefined) {
            cancellation = phase;
            controller.abort();
          }
        };
        const admit = () => {
          if (controller.signal.aborted)
            throw executionFailure(cancellation, request.policy.timeoutMs);
        };
        const detachCaller = forwardAbort(request.callerSignal, () => {
          cancel("cancelled");
        });
        const invocation = {
          executionId,
          deadlineAt: startedAt + request.policy.timeoutMs,
          signal: controller.signal,
          images: request.images,
        };
        if (request.toolCallId !== undefined)
          Object.assign(invocation, { toolCallId: request.toolCallId });
        const context: InvocationContext = Object.freeze(invocation);
        const sandboxExecution = Effect.suspend<
          SandboxValue,
          MyceliumError,
          never
        >(() => {
          if (controller.signal.aborted)
            return elapsedSince(startedAt).pipe(
              Effect.flatMap((elapsedMs) =>
                Effect.fail(
                  executionFailure(
                    cancellation,
                    request.policy.timeoutMs,
                    elapsedMs,
                  ),
                ),
              ),
            );
          return request.sandbox
            .execute(
              Object.freeze({
                executionId,
                code: request.code,
                timeoutMs: request.policy.timeoutMs,
                bindings: guardedBindings(
                  request.createBindings(context),
                  admit,
                ),
                // B3c (adr-guest-code-broker addendum): `buildGuestBindings`
                // classifies every compiled alias by its STABLE MODULE ID,
                // never its Team-chosen alias, so the sandbox needs both of
                // these to reach a retained operation at all.
                modules: request.modules,
                namespacePaths: request.namespacePaths,
                sessionId: request.sessionId,
              }),
              controller.signal,
            )
            .pipe(
              Effect.catchCause((cause) =>
                Cause.hasInterruptsOnly(cause)
                  ? Effect.failCause(cause)
                  : Effect.fail(sandboxFailure(Cause.squash(cause))),
              ),
            );
        });
        const interruptBacking = (
          child: Fiber.Fiber<SandboxValue, MyceliumError>,
        ) =>
          Fiber.interrupt(child).pipe(
            Effect.timeoutOrElse({
              duration: request.policy.abortGraceMs,
              orElse: () =>
                Effect.fail(
                  cancellation === "deadline_exceeded"
                    ? new DeadlineExceeded({
                        limitMs: request.policy.timeoutMs,
                        elapsedMs: request.policy.timeoutMs,
                        acknowledged: false,
                      })
                    : new Cancelled({ by: "caller", acknowledged: false }),
                ),
            }),
          );
        let child: Fiber.Fiber<SandboxValue, MyceliumError> | undefined;
        let reported = false;
        const reportFailure = (
          cause: unknown,
        ): Effect.Effect<SandboxFailure> => {
          reported = true;
          return elapsedSince(startedAt).pipe(
            Effect.flatMap((elapsedMs) => {
              const normalized = sandboxFailure(cause);
              const timed =
                normalized instanceof DeadlineExceeded
                  ? new DeadlineExceeded({
                      limitMs: normalized.limitMs,
                      elapsedMs,
                      acknowledged: normalized.acknowledged,
                    })
                  : normalized;
              const failure = referenceSandboxFailure(timed, executionId);
              const fields = z
                .record(z.string(), z.json())
                .parse(sandboxOperatorValue(timed));
              fields.ref = executionId;
              const event: SandboxFailureEvent = {
                kind: "sandbox_failure",
                ref: executionId,
                executionId,
                adapter: request.sandbox.adapter ?? "effect",
                buildSha: request.sandbox.buildSha ?? sandboxBuildSha(),
                tag: failure._tag,
                fields,
                healthAlert: failure._tag === "Unknown",
              };
              const report = Effect.suspend(
                () => request.sandbox.onFailure?.(event) ?? Effect.void,
              );
              return Effect.logError("sandbox_failure", event).pipe(
                Effect.annotateLogs({
                  executionId,
                  ref: executionId,
                  adapter: event.adapter,
                  tag: event.tag,
                  buildSha: event.buildSha,
                }),
                Effect.andThen(
                  failure._tag === "Unknown"
                    ? Metric.update(
                        Metric.withAttributes(
                          Metric.counter("sandbox_unknown"),
                          { adapter: event.adapter },
                        ),
                        1,
                      ).pipe(
                        Effect.andThen(
                          Effect.logError("sandbox_health_alert").pipe(
                            Effect.annotateLogs({
                              ref: executionId,
                              adapter: event.adapter,
                            }),
                          ),
                        ),
                      )
                    : Effect.void,
                ),
                Effect.andThen(
                  report.pipe(
                    Effect.catchCause((reportCause) =>
                      Effect.logError(
                        "sandbox_failure_record_failed",
                        reportCause,
                      ),
                    ),
                  ),
                ),
                Effect.as(failure),
              );
            }),
          );
        };
        const run = Effect.gen(function* () {
          if (controller.signal.aborted) {
            const elapsedMs = yield* elapsedSince(startedAt);
            return yield* Effect.fail(
              executionFailure(
                cancellation,
                request.policy.timeoutMs,
                elapsedMs,
              ),
            );
          }
          child = yield* request.backingWork.start(sandboxExecution);
          const first = yield* Effect.raceFirst(
            Fiber.await(child).pipe(
              Effect.map((exit) =>
                Object.freeze({ kind: "settled" as const, exit }),
              ),
            ),
            Effect.raceFirst(
              Effect.sleep(request.policy.timeoutMs).pipe(
                Effect.as(Object.freeze({ kind: "deadline" as const })),
              ),
              waitForAbort(controller.signal).pipe(
                Effect.as(Object.freeze({ kind: "cancelled" as const })),
              ),
            ),
          );
          if (first.kind === "settled") {
            return yield* Effect.matchCauseEffectEager(first.exit, {
              onFailure: (cause) =>
                // A cancellation or deadline was already decided before this
                // fiber settled: the sandbox's own failure is very likely a
                // side effect of that abort tearing it down (e.g. a rejected
                // in-flight fetch once disposal starts), not independent
                // signal. Report the actionable cancellation cause instead of
                // whatever generic failure teardown produced.
                cancellation === undefined
                  ? Effect.failCause(cause)
                  : elapsedSince(startedAt).pipe(
                      Effect.flatMap((elapsedMs) =>
                        Effect.fail(
                          executionFailure(
                            cancellation,
                            request.policy.timeoutMs,
                            elapsedMs,
                          ),
                        ),
                      ),
                    ),
              onSuccess: (value) =>
                Effect.try({
                  try: () => serializeSandboxValue(value),
                  catch: (cause) =>
                    isSandboxFailure(cause)
                      ? cause
                      : new ResultNotJson({
                          kind: sandboxValueKind(value),
                          rawCause: cause,
                        }),
                }),
            });
          }
          if (first.kind === "deadline") cancel("deadline_exceeded");
          const elapsedMs = yield* elapsedSince(startedAt);
          return yield* interruptBacking(child).pipe(
            Effect.andThen(
              Effect.fail(
                executionFailure(
                  cancellation,
                  request.policy.timeoutMs,
                  elapsedMs,
                ),
              ),
            ),
          );
        }).pipe(
          Effect.onInterrupt(() => {
            cancel("cancelled");
            const cleanup =
              child === undefined
                ? Effect.void
                : Fiber.interrupt(child).pipe(
                    Effect.timeoutOption(request.policy.abortGraceMs),
                    Effect.asVoid,
                  );
            return cleanup.pipe(
              Effect.andThen(
                Effect.suspend(() =>
                  reported
                    ? Effect.void
                    : reportFailure(new Cancelled({ by: "operator" })).pipe(
                        Effect.asVoid,
                      ),
                ),
              ),
            );
          }),
        );
        return run.pipe(
          Effect.catch((cause) =>
            reportFailure(cause).pipe(Effect.flatMap(Effect.fail)),
          ),
          Effect.ensuring(Effect.sync(detachCaller)),
        );
      }),
    ),
  );
}
