import { directSandbox } from "@fungi.computer/mycelium/testing";
import type {
  MulePreparedTool,
  MuleTool,
  MuleToolExecutionPart,
  ToolCall,
} from "@fungi.computer/mule/effect";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { describe, expect, it } from "vitest";
import { z } from "zod";

import { fromTypedSandbox, make as makeEffect } from "../src/effect.js";
import {
  Mycelium,
  MyceliumDefinitionError,
  Unknown,
  sandboxFailure,
  type SandboxFailure,
  type ModuleRegistration,
  type Sandbox,
  type SandboxValue,
  type InvocationContext,
} from "../src/index.js";

const sandbox = {
  execute: (): Promise<never> =>
    Promise.reject(new Error("The scaffold must not execute sandbox code")),
};
const errorSchema = z.instanceof(Error);

function parseError(cause: unknown): Error {
  return errorSchema.parse(cause);
}

function executeCall(callId: string, code: string): ToolCall {
  return {
    type: "toolCall",
    id: callId,
    name: "execute",
    arguments: { code },
  };
}

function prepareTool(
  tool: MuleTool,
  callId: string,
  code: string,
): Effect.Effect<MulePreparedTool, { readonly message: string }> {
  return tool.prepare(executeCall(callId, code));
}

async function runAtPromiseBoundary<A, E extends { readonly message: string }>(
  effect: Effect.Effect<A, E>,
  signal?: AbortSignal,
): Promise<A> {
  const exit = await Effect.runPromiseExit(
    effect,
    signal === undefined ? undefined : { signal },
  );
  if (Exit.isSuccess(exit)) return exit.value;
  const failure = Cause.findErrorOption(exit.cause);
  if (Option.isSome(failure)) {
    return Promise.reject(
      failure.value instanceof Error
        ? failure.value
        : Object.assign(new Error(failure.value.message), failure.value),
    );
  }
  if (Cause.hasInterruptsOnly(exit.cause)) {
    return Promise.reject(
      signal?.reason instanceof Error
        ? signal.reason
        : new DOMException("The operation was aborted", "AbortError"),
    );
  }
  const squashed = Cause.squash(exit.cause);
  return Promise.reject(
    squashed instanceof Error
      ? squashed
      : new Error("Effect failed without an Error cause"),
  );
}

function preparePromiseTool(
  tool: MuleTool,
  callId: string,
  code: string,
  signal?: AbortSignal,
): Promise<MulePreparedTool> {
  return runAtPromiseBoundary(prepareTool(tool, callId, code), signal);
}

function executePromiseTool(
  prepared: MulePreparedTool,
  signal?: AbortSignal,
): Promise<readonly MuleToolExecutionPart[]> {
  return runAtPromiseBoundary(Stream.runCollect(prepared.execute()), signal);
}

describe("Mycelium capability plane", () => {
  it("shares the original execute deadline with later operations in the same invocation", async () => {
    const observed: InvocationContext[] = [];
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const first = yield* Deferred.make<undefined>();
          const proceed = yield* Deferred.make<undefined>();
          const runtime = yield* makeEffect({
            modules: [
              Mycelium.module({
                id: "budget",
                description: "Observe the invocation budget",
                operations: {
                  read: Mycelium.operation({
                    input: z.strictObject({}),
                    output: z.null(),
                    execute: (_input, context) => {
                      observed.push(context);
                      return Promise.resolve(null);
                    },
                  }),
                },
              }),
            ],
            sandbox: {
              execute: (request) =>
                Effect.gen(function* () {
                  const read = request.bindings.budget?.read;
                  if (read === undefined)
                    throw new Error("Expected budget operation");
                  yield* Effect.promise(() => read({}));
                  yield* Deferred.succeed(first, undefined);
                  yield* Deferred.await(proceed);
                  return yield* Effect.promise(() => read({}));
                }),
            },
          });
          const lease = yield* runtime.acquire();
          const prepared = yield* prepareTool(
            lease.executeTool,
            "original-deadline",
            "return budget.read({})",
          );
          const execution = yield* Effect.forkChild(
            Stream.runCollect(prepared.execute()),
          );
          yield* Deferred.await(first);
          yield* TestClock.adjust("30 seconds");
          yield* Deferred.succeed(proceed, undefined);
          yield* Fiber.join(execution);
        }),
      ).pipe(Effect.provide(TestClock.layer())),
    );
    // Default execute budget is 180 seconds from invocation start, including
    // the 30 seconds already spent before the second operation.
    expect(observed).toMatchObject([
      { deadlineAt: 180_000, toolCallId: "original-deadline" },
      { deadlineAt: 180_000, toolCallId: "original-deadline" },
    ]);
  });

  it("provisions opaque Zod registrations as one root execute tool", async () => {
    await expect(
      (async () => {
        const runtime = await Mycelium.make({
          modules: [
            Mycelium.module({
              id: "demo",
              description: "Demo operations",
              operations: {
                ping: Mycelium.operation({
                  input: z.object({ value: z.string() }),
                  output: z.object({ value: z.string() }),
                  execute: ({ value }) => Promise.resolve({ value }),
                }),
              },
            }),
          ],
          sandbox,
        });
        const lease = await runtime.acquire();

        expect(lease.catalog.describe({ path: "demo.ping" })).toMatchObject({
          path: "demo.ping",
        });

        expect(lease.executeTool.descriptor.name).toBe("execute");

        await lease.release();
        await runtime.close();
      })(),
    ).resolves.toBeUndefined();
  });

  it("executes one Zod capability through the root door with Forage bindings", async () => {
    let sandboxExecutionId: string | undefined;
    let operationExecutionId: string | undefined;
    const runtime = await Mycelium.make({
      modules: [
        Mycelium.module({
          id: "demo",
          description: "Demo operations",
          operations: {
            ping: Mycelium.operation({
              input: z.object({ value: z.string() }),
              output: z.object({ value: z.string() }),
              execute: ({ value }, context) => {
                operationExecutionId = context.executionId;
                return Promise.resolve({ value });
              },
            }),
          },
        }),
      ],
      sandbox: {
        execute: async (request) => {
          sandboxExecutionId = request.executionId;
          expect(request.code).toBe('return demo.ping({ value: "pong" })');
          expect(Object.keys(request.bindings)).toEqual(["demo", "forage"]);

          const demo = request.bindings.demo;
          const forage = request.bindings.forage;
          expect(demo).toBeDefined();
          expect(forage).toBeDefined();
          if (demo === undefined || forage === undefined) {
            throw new Error("Expected demo and forage sandbox namespaces");
          }

          expect(Object.keys(demo)).toEqual(["ping"]);
          expect(Object.keys(forage)).toEqual(["list", "describe", "search"]);
          const searchOperation = forage.search;
          const describeOperation = forage.describe;
          const listOperation = forage.list;
          const pingOperation = demo.ping;
          if (
            searchOperation === undefined ||
            describeOperation === undefined ||
            listOperation === undefined ||
            pingOperation === undefined
          ) {
            throw new Error("Expected demo and forage sandbox operations");
          }

          const search = await searchOperation({ query: "ping" });
          const searchResult = z
            .object({ results: z.array(z.object({ path: z.string() })) })
            .parse(search);
          expect(searchResult.results.map((result) => result.path)).toContain(
            "demo.ping",
          );
          const description = await describeOperation({ path: "demo.ping" });
          expect(description).toMatchObject({ path: "demo.ping" });
          const listing = z
            .object({
              results: z.array(z.object({ path: z.string() })),
              total: z.number(),
              nextCursor: z.null(),
            })
            .parse(await listOperation({ limit: 50 }));
          expect(listing.results.map((row) => row.path)).toEqual([
            "demo.ping",
            "forage.describe",
            "forage.list",
            "forage.search",
          ]);
          expect(listing.total).toBe(4);
          return pingOperation({ value: "pong" });
        },
      },
    });
    const lease = await runtime.acquire();

    try {
      await expect(
        (async () => {
          expect(
            lease.catalog.describe({ path: "forage.search" }),
          ).toMatchObject({
            path: "forage.search",
          });
          expect(
            lease.catalog.describe({ path: "forage.describe" }),
          ).toMatchObject({ path: "forage.describe" });

          const prepared = await preparePromiseTool(
            lease.executeTool,
            "execute-1",
            'return demo.ping({ value: "pong" })',
          );
          const parts = await executePromiseTool(prepared);

          expect(sandboxExecutionId).toMatch(/^[a-z0-9]{21}$/);
          expect(sandboxExecutionId).not.toBe("execute-1");
          expect(operationExecutionId).toBe(sandboxExecutionId);
          expect(parts).toEqual([
            {
              type: "result",
              result: {
                content: [{ type: "text", text: '{"value":"pong"}' }],
                isError: false,
              },
            },
          ]);
        })(),
      ).resolves.toBeUndefined();
    } finally {
      await lease.release();
      await runtime.close();
    }
  });

  it("rejects a non-JSON sandbox result instead of coercing it into success", async () => {
    const runtime = await Mycelium.make({
      modules: [],
      sandbox: directSandbox(() => Promise.resolve(Number.POSITIVE_INFINITY)),
    });
    const lease = await runtime.acquire();

    try {
      const prepared = await preparePromiseTool(
        lease.executeTool,
        "non-json-1",
        "return Infinity",
      );
      await expect(executePromiseTool(prepared)).rejects.toHaveProperty(
        "message",
        expect.stringMatching(/^Sandbox returned non-JSON data \(ref /u),
      );
    } finally {
      await lease.release();
      await runtime.close();
    }
  });

  it("enforces the 256 KiB UTF-8 JSON result ceiling", async () => {
    const maximumBytes = 256 * 1024;
    const acceptedValue = "é".repeat((maximumBytes - 2) / 2);
    const rejectedValue = `${acceptedValue}a`;
    let output = acceptedValue;
    const runtime = await Mycelium.make({
      modules: [],
      sandbox: directSandbox(() => Promise.resolve(output)),
    });
    const lease = await runtime.acquire();

    try {
      const run = async (callId: string) => {
        const prepared = await preparePromiseTool(
          lease.executeTool,
          callId,
          "return output",
        );
        return executePromiseTool(prepared);
      };

      const acceptedText = JSON.stringify(acceptedValue);
      expect(new TextEncoder().encode(acceptedText)).toHaveLength(maximumBytes);
      expect(await run("output-exact")).toEqual([
        {
          type: "result",
          result: {
            content: [{ type: "text", text: acceptedText }],
            isError: false,
          },
        },
      ]);

      output = rejectedValue;
      const rejected = await run("output-over").then(
        () => ({ status: "fulfilled" as const }),
        (cause: unknown) => ({
          status: "rejected" as const,
          reason: cause,
        }),
      );
      expect(rejected).toHaveProperty(
        "reason.message",
        expect.stringMatching(/^Sandbox result exceeds 256 KiB \(ref /u),
      );
      expect(rejected).toMatchObject({
        status: "rejected",
        reason: {
          _tag: "MuleError",
          phase: "tool",
        },
      });
    } finally {
      await lease.release();
      await runtime.close();
    }
  });

  it("rejects untrusted operation input before the executor runs", async () => {
    let executorCalls = 0;
    let parserError: unknown;
    const runtime = await Mycelium.make({
      modules: [
        Mycelium.module({
          id: "demo",
          operations: {
            ping: Mycelium.operation({
              input: z.object({ value: z.string() }),
              execute: () => {
                executorCalls += 1;
                return Promise.resolve({ ok: true });
              },
            }),
          },
        }),
      ],
      sandbox: directSandbox(async (bindings) => {
        const ping = bindings.demo?.ping;
        if (ping === undefined) {
          throw new Error("Expected demo.ping sandbox binding");
        }
        try {
          return await ping({ value: 42 });
        } catch (error) {
          parserError = error;
          throw error;
        }
      }),
    });
    const lease = await runtime.acquire();

    try {
      const prepared = await preparePromiseTool(
        lease.executeTool,
        "invalid-input-1",
        "return demo.ping({ value: 42 })",
      );
      await expect(executePromiseTool(prepared)).rejects.toBeDefined();
      expect(parserError).toMatchObject({
        _tag: "CapabilityFailed",
        cause: { kind: "InvalidInput" },
      });
      expect(executorCalls).toBe(0);
    } finally {
      await lease.release();
      await runtime.close();
    }
  });

  it("propagates root invocation cancellation into the sandbox and operation", async () => {
    let sandboxSignal: AbortSignal | undefined;
    let operationSignal: AbortSignal | undefined;
    let markOperationStarted: (() => void) | undefined;
    const operationStarted = new Promise<void>((resolve) => {
      markOperationStarted = resolve;
    });
    const runtime = await Mycelium.make({
      modules: [
        Mycelium.module({
          id: "demo",
          operations: {
            wait: Mycelium.operation({
              input: z.object({}),
              output: z.object({ value: z.string() }),
              execute: (_input, context) => {
                operationSignal = context.signal;
                markOperationStarted?.();
                return new Promise((resolve) => {
                  context.signal.addEventListener(
                    "abort",
                    () => {
                      resolve({ value: "cancelled" });
                    },
                    { once: true },
                  );
                });
              },
            }),
          },
        }),
      ],
      sandbox: directSandbox(async (bindings, signal) => {
        sandboxSignal = signal;
        const wait = bindings.demo?.wait;
        if (wait === undefined) {
          throw new Error("Expected demo.wait sandbox binding");
        }
        return wait({});
      }),
    });
    const lease = await runtime.acquire();
    const controller = new AbortController();

    try {
      const prepared = await preparePromiseTool(
        lease.executeTool,
        "cancel-1",
        "return demo.wait({})",
        controller.signal,
      );
      const consumption = executePromiseTool(prepared, controller.signal);

      await operationStarted;
      controller.abort();

      await expect(consumption).rejects.toMatchObject({ name: "AbortError" });
      expect(operationSignal?.aborted).toBe(true);
      expect(sandboxSignal?.aborted).toBe(true);
    } finally {
      await lease.release();
      await runtime.close();
    }
  });

  it("rejects a sandbox success that arrives after caller cancellation", async () => {
    let resolveSandboxStarted: (() => void) | undefined;
    const sandboxStarted = new Promise<void>((resolve) => {
      resolveSandboxStarted = resolve;
    });
    let delivered: unknown;
    const lateSandbox: Sandbox = {
      execute: (_request, signal) =>
        new Promise((resolve) => {
          resolveSandboxStarted?.();
          signal.addEventListener(
            "abort",
            () => {
              resolve({ late: "success" });
            },
            {
              once: true,
            },
          );
        }),
    };
    const runtime = await Mycelium.make({
      modules: [],
      sandbox: {
        execute: async (request, signal) => {
          const result = await lateSandbox.execute(request, signal);
          delivered = result;
          return result;
        },
      },
    });
    const lease = await runtime.acquire();

    try {
      const controller = new AbortController();
      const prepared = await preparePromiseTool(
        lease.executeTool,
        "cancelled-late-success",
        "return late()",
        controller.signal,
      );
      const parts: unknown[] = [];
      const consumption = executePromiseTool(prepared, controller.signal).then(
        (completedParts) => {
          parts.push(...completedParts);
        },
      );
      const observed = consumption.then(
        () => ({ status: "fulfilled" as const }),
        (cause: unknown) => ({
          status: "rejected" as const,
          reason: parseError(cause),
        }),
      );

      await sandboxStarted;
      controller.abort();
      await expect(observed).resolves.toMatchObject({
        status: "rejected",
        reason: { name: "AbortError" },
      });
      expect(delivered).toEqual({ late: "success" });
      expect(parts).toEqual([]);
    } finally {
      await lease.release();
      await runtime.close();
    }
  });

  it("enforces a deadline without claiming an ignored abort settled", async () => {
    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const sandboxStarted = yield* Deferred.make<undefined>();
          const sandboxGate = yield* Deferred.make<undefined>();
          const runtime = yield* makeEffect({
            execution: { timeoutMs: 100, abortGraceMs: 50 },
            modules: [],
            sandbox: {
              execute: () =>
                Deferred.succeed(sandboxStarted, undefined).pipe(
                  Effect.andThen(
                    Effect.uninterruptible(Deferred.await(sandboxGate)),
                  ),
                ),
            },
          });
          const lease = yield* runtime.acquire();
          const prepared = yield* prepareTool(
            lease.executeTool,
            "deadline-1",
            "return never()",
          );
          const executionFiber = yield* Effect.forkChild(
            Effect.exit(Stream.runCollect(prepared.execute())),
          );

          yield* Deferred.await(sandboxStarted);
          yield* TestClock.adjust("100 millis");
          const beforeGrace = yield* Effect.sync(() =>
            Option.fromUndefinedOr(executionFiber.pollUnsafe()),
          );
          yield* TestClock.adjust("49 millis");
          const beforeGraceExpiry = yield* Effect.sync(() =>
            Option.fromUndefinedOr(executionFiber.pollUnsafe()),
          );
          yield* TestClock.adjust("1 millis");
          const interruptionExit = yield* Fiber.join(executionFiber);
          yield* Deferred.succeed(sandboxGate, undefined);

          return { beforeGrace, beforeGraceExpiry, interruptionExit };
        }),
      ).pipe(Effect.provide(TestClock.layer())),
    );

    expect(Option.isNone(result.beforeGrace)).toBe(true);
    expect(Option.isNone(result.beforeGraceExpiry)).toBe(true);
    expect(Exit.isFailure(result.interruptionExit)).toBe(true);
    const failure = Exit.isFailure(result.interruptionExit)
      ? Cause.findErrorOption(result.interruptionExit.cause)
      : Option.none();
    expect(Option.isSome(failure)).toBe(true);
    if (Option.isSome(failure)) {
      expect(failure.value).toMatchObject({
        _tag: "MuleError",
        phase: "tool",
      });
      expect(failure.value).toHaveProperty(
        "message",
        expect.stringMatching(/sandbox did not acknowledge cancellation/u),
      );
    }
  });
});

it("runtime admits eight backing executions and rejects the ninth before sandbox start", async () => {
  const executionIndexes = Array.from({ length: 10 }, (_, index) => index);
  const hostInvocationIndexes: number[] = [];
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const executionFixtures = yield* Effect.forEach(executionIndexes, () =>
          Effect.gen(function* () {
            return {
              exited: yield* Deferred.make<undefined>(),
              release: yield* Deferred.make<undefined>(),
              started: yield* Deferred.make<undefined>(),
            };
          }),
        );
        const runtime = yield* makeEffect({
          modules: [],
          execution: { timeoutMs: 60_000, abortGraceMs: 30_000 },
          sandbox: {
            execute: (request) => {
              const executionIndex = Number(request.code);
              const fixture = executionFixtures[executionIndex];
              if (!Number.isInteger(executionIndex) || fixture === undefined) {
                return Effect.fail(
                  new Unknown({
                    rawCause: new Error(
                      `Unexpected execution code: ${request.code}`,
                    ),
                  }),
                );
              }
              hostInvocationIndexes.push(executionIndex);
              return Deferred.succeed(fixture.started, undefined).pipe(
                Effect.andThen(
                  Effect.uninterruptible(Deferred.await(fixture.release)),
                ),
                Effect.ensuring(Deferred.succeed(fixture.exited, undefined)),
                Effect.as({ executionIndex }),
              );
            },
          },
        });
        const lease = yield* runtime.acquire();
        const run = (executionIndex: number) =>
          Effect.gen(function* () {
            const prepared = yield* prepareTool(
              lease.executeTool,
              `backing-capacity-${String(executionIndex)}`,
              String(executionIndex),
            );
            return yield* Stream.runCollect(prepared.execute());
          });
        const releaseAllSandboxes = Effect.all(
          executionFixtures.map((fixture) =>
            Deferred.succeed(fixture.release, undefined),
          ),
          { concurrency: "unbounded" },
        ).pipe(Effect.asVoid);
        const observeBounded = (
          first: Effect.Effect<string>,
          second: Effect.Effect<string>,
        ) =>
          Effect.gen(function* () {
            const observationFiber = yield* Effect.forkChild(
              Effect.raceFirst(
                Effect.raceFirst(first, second),
                Effect.sleep("1 millis").pipe(
                  Effect.as("still_pending" as const),
                ),
              ),
            );
            yield* TestClock.adjust("1 millis");
            return yield* Fiber.join(observationFiber);
          });

        return yield* Effect.gen(function* () {
          const originalAttempts = yield* Effect.forEach(
            executionIndexes.slice(0, 8),
            (executionIndex) =>
              Effect.gen(function* () {
                const fixture = executionFixtures[executionIndex];
                if (fixture === undefined) {
                  throw new Error("Expected original execution fixture");
                }
                return {
                  fiber: yield* Effect.forkChild(run(executionIndex)),
                  fixture,
                };
              }),
          );
          const originalStartOutcomes = yield* Effect.forEach(
            originalAttempts,
            (attempt) =>
              observeBounded(
                Deferred.await(attempt.fixture.started).pipe(
                  Effect.as("started" as const),
                ),
                Fiber.await(attempt.fiber).pipe(Effect.as("settled" as const)),
              ),
            { concurrency: "unbounded" },
          );
          const originalStartCount = originalStartOutcomes.filter(
            (outcome) => outcome === "started",
          ).length;
          expect(originalStartCount).toBe(8);
          const originalsAllPending = (yield* Effect.forEach(
            originalAttempts,
            (attempt) =>
              Effect.sync(() =>
                Option.fromUndefinedOr(attempt.fiber.pollUnsafe()),
              ),
            { concurrency: "unbounded" },
          )).every(Option.isNone);
          expect(originalsAllPending).toBe(true);

          const firstOriginal = originalAttempts[0];
          const ninthFixture = executionFixtures[8];
          const laterFixture = executionFixtures[9];
          if (
            firstOriginal === undefined ||
            ninthFixture === undefined ||
            laterFixture === undefined
          ) {
            throw new Error("Expected complete backing-execution fixtures");
          }

          const ninthFiber = yield* Effect.forkChild(run(8));
          const ninthFirst = yield* observeBounded(
            Fiber.await(ninthFiber).pipe(Effect.as("settled" as const)),
            Deferred.await(ninthFixture.started).pipe(
              Effect.as("sandbox_started" as const),
            ),
          );
          const ninthStartedBeforeSlotRelease = Option.isSome(
            yield* Deferred.poll(ninthFixture.started),
          );
          const hostInvocationsBeforeSlotRelease = [
            ...hostInvocationIndexes,
          ].sort((left, right) => left - right);
          expect(ninthFirst).toBe("settled");
          expect(ninthStartedBeforeSlotRelease).toBe(false);
          expect(hostInvocationsBeforeSlotRelease).toEqual([
            0, 1, 2, 3, 4, 5, 6, 7,
          ]);
          const ninthExit = yield* Fiber.await(ninthFiber);
          const ninthFailure = Exit.isFailure(ninthExit)
            ? Cause.findErrorOption(ninthExit.cause)
            : Option.none();
          expect(
            Option.isSome(ninthFailure) ? ninthFailure.value : undefined,
          ).toMatchObject({
            _tag: "MuleError",
            phase: "tool",
          });
          expect(
            Option.isSome(ninthFailure) ? ninthFailure.value : undefined,
          ).toHaveProperty(
            "message",
            expect.stringMatching(
              /^Sandbox execution capacity unavailable \(ref /u,
            ),
          );

          yield* Deferred.succeed(firstOriginal.fixture.release, undefined);
          const firstExitFirst = yield* observeBounded(
            Deferred.await(firstOriginal.fixture.exited).pipe(
              Effect.as("sandbox_exited" as const),
            ),
            Fiber.await(firstOriginal.fiber).pipe(
              Effect.as("execution_settled" as const),
            ),
          );
          expect(firstExitFirst).toBe("sandbox_exited");

          const laterFiber = yield* Effect.forkChild(run(9));
          const laterFirst = yield* observeBounded(
            Deferred.await(laterFixture.started).pipe(
              Effect.as("sandbox_started" as const),
            ),
            Fiber.await(laterFiber).pipe(Effect.as("settled" as const)),
          );
          const laterPendingAfterStart = Option.isNone(
            yield* Effect.sync(() =>
              Option.fromUndefinedOr(laterFiber.pollUnsafe()),
            ),
          );
          const ninthStartedAfterSlotReuse = Option.isSome(
            yield* Deferred.poll(ninthFixture.started),
          );
          const hostInvocationsAfterSlotReuse = [...hostInvocationIndexes].sort(
            (left, right) => left - right,
          );
          expect(laterFirst).toBe("sandbox_started");
          expect(laterPendingAfterStart).toBe(true);
          expect(ninthStartedAfterSlotReuse).toBe(false);
          expect(hostInvocationsAfterSlotReuse).toEqual([
            0, 1, 2, 3, 4, 5, 6, 7, 9,
          ]);

          yield* releaseAllSandboxes;
          const originalExits = yield* Effect.forEach(
            originalAttempts,
            (attempt) => Fiber.await(attempt.fiber),
            { concurrency: "unbounded" },
          );
          yield* Fiber.await(laterFiber);
          const firstOriginalExit = originalExits[0];
          if (firstOriginalExit === undefined) {
            throw new Error("Expected first original execution exit");
          }
          expect(Exit.isSuccess(firstOriginalExit)).toBe(true);
        }).pipe(Effect.ensuring(releaseAllSandboxes));
      }),
    ).pipe(Effect.provide(TestClock.layer())),
  );
});

it("already cancelled invocation starts no sandbox and consumes no backing capacity", async () => {
  const executionIndexes = Array.from({ length: 9 }, (_, index) => index);
  const hostInvocationIndexes: number[] = [];
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const executionFixtures = yield* Effect.forEach(executionIndexes, () =>
          Effect.gen(function* () {
            return {
              exited: yield* Deferred.make<undefined>(),
              release: yield* Deferred.make<undefined>(),
              started: yield* Deferred.make<undefined>(),
            };
          }),
        );
        const runtime = yield* Effect.promise(() =>
          Mycelium.make({
            modules: [],
            execution: { timeoutMs: 60_000, abortGraceMs: 30_000 },
            sandbox: {
              execute: (request) => {
                const executionIndex = Number(request.code);
                const fixture = executionFixtures[executionIndex];
                if (
                  !Number.isInteger(executionIndex) ||
                  fixture === undefined
                ) {
                  return Promise.reject(
                    new Unknown({
                      rawCause: new Error(
                        `Unexpected execution code: ${request.code}`,
                      ),
                    }),
                  );
                }
                hostInvocationIndexes.push(executionIndex);
                return Effect.runPromise(
                  Deferred.succeed(fixture.started, undefined).pipe(
                    Effect.andThen(
                      Effect.uninterruptible(Deferred.await(fixture.release)),
                    ),
                    Effect.ensuring(
                      Deferred.succeed(fixture.exited, undefined),
                    ),
                    Effect.as({ executionIndex }),
                  ),
                );
              },
            },
          }),
        );
        yield* Effect.addFinalizer(() => Effect.promise(() => runtime.close()));
        const lease = yield* Effect.promise(() => runtime.acquire());
        yield* Effect.addFinalizer(() => Effect.promise(() => lease.release()));
        const prepare = (executionIndex: number) =>
          prepareTool(
            lease.executeTool,
            `already-cancelled-${String(executionIndex)}`,
            String(executionIndex),
          );
        const consume = (invocation: MulePreparedTool) =>
          Stream.runCollect(invocation.execute());
        const releaseAllSandboxes = Effect.all(
          executionFixtures.map((fixture) =>
            Deferred.succeed(fixture.release, undefined),
          ),
          { concurrency: "unbounded" },
        ).pipe(Effect.asVoid);
        const observeBounded = (
          first: Effect.Effect<string>,
          second: Effect.Effect<string>,
        ) =>
          Effect.gen(function* () {
            const observationFiber = yield* Effect.forkChild(
              Effect.raceFirst(
                Effect.raceFirst(first, second),
                Effect.sleep("1 millis").pipe(
                  Effect.as("still_pending" as const),
                ),
              ),
            );
            yield* TestClock.adjust("1 millis");
            return yield* Fiber.join(observationFiber);
          });

        return yield* Effect.gen(function* () {
          const originalAttempts = yield* Effect.forEach(
            executionIndexes.slice(0, 7),
            (executionIndex) =>
              Effect.gen(function* () {
                const fixture = executionFixtures[executionIndex];
                if (fixture === undefined) {
                  throw new Error("Expected original execution fixture");
                }
                const invocation = yield* prepare(executionIndex);
                return {
                  fiber: yield* Effect.forkChild(consume(invocation)),
                  fixture,
                };
              }),
          );
          const originalStartOutcomes = yield* Effect.forEach(
            originalAttempts,
            (attempt) =>
              observeBounded(
                Deferred.await(attempt.fixture.started).pipe(
                  Effect.as("started" as const),
                ),
                Fiber.await(attempt.fiber).pipe(Effect.as("settled" as const)),
              ),
            { concurrency: "unbounded" },
          );
          expect(originalStartOutcomes).toEqual(
            originalAttempts.map(() => "started"),
          );
          expect(
            (yield* Effect.forEach(
              originalAttempts,
              (attempt) =>
                Effect.sync(() =>
                  Option.fromUndefinedOr(attempt.fiber.pollUnsafe()),
                ),
              { concurrency: "unbounded" },
            )).every(Option.isNone),
          ).toBe(true);
          expect(
            [...hostInvocationIndexes].sort((left, right) => left - right),
          ).toEqual([0, 1, 2, 3, 4, 5, 6]);

          const cancelledFixture = executionFixtures[7];
          const freshFixture = executionFixtures[8];
          if (cancelledFixture === undefined || freshFixture === undefined) {
            throw new Error("Expected cancellation execution fixtures");
          }
          const cancelledInvocation = yield* prepare(7);
          const cancelledExit = yield* Effect.exit(
            Effect.interrupt.pipe(Effect.andThen(consume(cancelledInvocation))),
          );
          expect(Exit.isFailure(cancelledExit)).toBe(true);
          expect(
            Exit.isFailure(cancelledExit) &&
              Cause.hasInterruptsOnly(cancelledExit.cause),
          ).toBe(true);
          expect(
            Option.isNone(yield* Deferred.poll(cancelledFixture.started)),
          ).toBe(true);
          expect(
            [...hostInvocationIndexes].sort((left, right) => left - right),
          ).toEqual([0, 1, 2, 3, 4, 5, 6]);

          const freshInvocation = yield* prepare(8);
          const freshFiber = yield* Effect.forkChild(consume(freshInvocation));
          const freshObservation = yield* observeBounded(
            Deferred.await(freshFixture.started).pipe(
              Effect.as("sandbox_started" as const),
            ),
            Fiber.await(freshFiber).pipe(Effect.as("settled" as const)),
          );
          expect(freshObservation).toBe("sandbox_started");
          expect(
            Option.isNone(
              yield* Effect.sync(() =>
                Option.fromUndefinedOr(freshFiber.pollUnsafe()),
              ),
            ),
          ).toBe(true);
          expect(
            [...hostInvocationIndexes].sort((left, right) => left - right),
          ).toEqual([0, 1, 2, 3, 4, 5, 6, 8]);

          yield* releaseAllSandboxes;
          const completedExits = yield* Effect.forEach(
            [...originalAttempts.map((attempt) => attempt.fiber), freshFiber],
            Fiber.await,
            { concurrency: "unbounded" },
          );
          expect(completedExits.every(Exit.isSuccess)).toBe(true);
          const exitedIndexes = [...executionIndexes.slice(0, 7), 8];
          expect(
            (yield* Effect.forEach(exitedIndexes, (executionIndex) => {
              const fixture = executionFixtures[executionIndex];
              return fixture === undefined
                ? Effect.succeed(Option.none())
                : Deferred.poll(fixture.exited);
            })).every(Option.isSome),
          ).toBe(true);
        }).pipe(Effect.ensuring(releaseAllSandboxes));
      }),
    ).pipe(Effect.provide(TestClock.layer())),
  );
});

it("cancelled execution rejects every new binding before operation egress", async () => {
  let operationInvocations = 0;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const operationStarted = yield* Deferred.make<AbortSignal>();
        const operationObservedAbort = yield* Deferred.make<AbortSignal>();
        const teardownInFlightOperation = yield* Deferred.make<undefined>();
        const releaseInFlightOperation = yield* Deferred.make<undefined>();
        const postCancelAttempts = yield* Deferred.make<{
          readonly describeRejected: boolean;
          readonly searchRejected: boolean;
          readonly sideEffectRejected: boolean;
        }>();
        const releaseSandbox = yield* Deferred.make<undefined>();
        const sandboxExited = yield* Deferred.make<undefined>();
        const awaitAbort = (signal: AbortSignal) =>
          Effect.callback<AbortSignal>((resume) => {
            const onAbort = () => {
              resume(Effect.succeed(signal));
            };
            if (signal.aborted) {
              onAbort();
            } else {
              signal.addEventListener("abort", onAbort, { once: true });
            }
            return Effect.sync(() => {
              signal.removeEventListener("abort", onAbort);
            });
          });
        const runtime = yield* makeEffect({
          modules: [
            Mycelium.module({
              id: "membrane",
              operations: {
                touch: Mycelium.operation({
                  input: z.object({
                    phase: z.enum(["initial", "post_cancel"]),
                  }),
                  output: z.object({ phase: z.string() }),
                  execute: (input, context) => {
                    operationInvocations += 1;
                    if (input.phase === "post_cancel") {
                      return Promise.resolve({ phase: input.phase });
                    }
                    return Effect.runPromise(
                      Deferred.succeed(operationStarted, context.signal).pipe(
                        Effect.andThen(
                          Effect.raceFirst(
                            awaitAbort(context.signal).pipe(
                              Effect.map(Option.some),
                            ),
                            Deferred.await(teardownInFlightOperation).pipe(
                              Effect.as(Option.none<AbortSignal>()),
                            ),
                          ),
                        ),
                        Effect.tap((signal) =>
                          Option.isSome(signal)
                            ? Deferred.succeed(
                                operationObservedAbort,
                                signal.value,
                              )
                            : Effect.void,
                        ),
                        Effect.andThen(
                          Deferred.await(releaseInFlightOperation),
                        ),
                        Effect.as({ phase: input.phase }),
                      ),
                    );
                  },
                }),
              },
            }),
          ],
          execution: { timeoutMs: 60_000, abortGraceMs: 30_000 },
          sandbox: {
            execute: (request) =>
              Effect.uninterruptible(
                Effect.gen(function* () {
                  const touch = request.bindings.membrane?.touch;
                  const forage = request.bindings.forage;
                  const search = forage?.search;
                  const describe = forage?.describe;
                  if (
                    touch === undefined ||
                    search === undefined ||
                    describe === undefined
                  ) {
                    throw new Error("Expected generated sandbox bindings");
                  }

                  yield* Effect.tryPromise({
                    try: () => touch({ phase: "initial" }),
                    catch: sandboxFailure,
                  });
                  const sideEffectExit = yield* Effect.exit(
                    Effect.tryPromise({
                      try: () => touch({ phase: "post_cancel" }),
                      catch: sandboxFailure,
                    }),
                  );
                  const searchExit = yield* Effect.exit(
                    Effect.tryPromise({
                      try: () => search({ query: "touch" }),
                      catch: sandboxFailure,
                    }),
                  );
                  const describeExit = yield* Effect.exit(
                    Effect.tryPromise({
                      try: () => describe({ path: "membrane.touch" }),
                      catch: sandboxFailure,
                    }),
                  );
                  yield* Deferred.succeed(postCancelAttempts, {
                    describeRejected: Exit.isFailure(describeExit),
                    searchRejected: Exit.isFailure(searchExit),
                    sideEffectRejected: Exit.isFailure(sideEffectExit),
                  });
                  yield* Deferred.await(releaseSandbox);
                  return { status: "settled" };
                }).pipe(
                  Effect.ensuring(Deferred.succeed(sandboxExited, undefined)),
                ),
              ),
          },
        });
        const lease = yield* runtime.acquire();
        const prepared = yield* prepareTool(
          lease.executeTool,
          "post-cancel-binding-membrane",
          "return membrane.touch()",
        );
        const releaseHostGates = Effect.all(
          [
            Deferred.succeed(teardownInFlightOperation, undefined),
            Deferred.succeed(releaseInFlightOperation, undefined),
            Deferred.succeed(releaseSandbox, undefined),
          ],
          { concurrency: "unbounded" },
        ).pipe(Effect.asVoid);
        const awaitBounded = <A>(observation: Effect.Effect<A>) =>
          Effect.gen(function* () {
            const observationFiber = yield* Effect.forkChild(
              Effect.raceFirst(
                observation.pipe(Effect.map(Option.some)),
                Effect.sleep("1 millis").pipe(Effect.as(Option.none<A>())),
              ),
            );
            yield* TestClock.adjust("1 millis");
            const result = yield* Fiber.join(observationFiber);
            expect(Option.isSome(result)).toBe(true);
            if (Option.isNone(result)) {
              throw new Error(
                "Expected observation before the TestClock bound",
              );
            }
            return result.value;
          });

        return yield* Effect.gen(function* () {
          const executionFiber = yield* Effect.forkChild(
            Stream.runCollect(prepared.execute()),
          );
          const signalAtOperationStart = yield* awaitBounded(
            Deferred.await(operationStarted),
          );
          expect(
            Option.isNone(
              yield* Effect.sync(() =>
                Option.fromUndefinedOr(executionFiber.pollUnsafe()),
              ),
            ),
          ).toBe(true);

          const cancellationFiber = yield* Effect.forkChild(
            Fiber.interrupt(executionFiber),
          );
          const signalAtAbortObservation = yield* awaitBounded(
            Deferred.await(operationObservedAbort),
          );
          expect(signalAtAbortObservation).toBe(signalAtOperationStart);
          expect(signalAtOperationStart.aborted).toBe(true);

          yield* Deferred.succeed(releaseInFlightOperation, undefined);
          const attempts = yield* awaitBounded(
            Deferred.await(postCancelAttempts),
          );
          expect(operationInvocations).toBe(1);
          expect(attempts.sideEffectRejected).toBe(true);
          expect(attempts.searchRejected).toBe(true);
          expect(attempts.describeRejected).toBe(true);

          yield* releaseHostGates;
          yield* awaitBounded(Deferred.await(sandboxExited));
          const cancellationExit = yield* awaitBounded(
            Fiber.await(cancellationFiber),
          );
          expect(Exit.isSuccess(cancellationExit)).toBe(true);
          yield* Fiber.join(cancellationFiber);
          const executionExit = yield* Fiber.await(executionFiber);
          expect(Exit.isFailure(executionExit)).toBe(true);
          yield* Fiber.await(executionFiber);
        }).pipe(Effect.ensuring(releaseHostGates));
      }),
    ).pipe(Effect.provide(TestClock.layer())),
  );
});

it("late sandbox success or failure only releases its runtime tracker slot", async () => {
  const executionIndexes = Array.from({ length: 13 }, (_, index) => index);
  const lateSuccessParts: unknown[] = [];
  const lateFailureParts: unknown[] = [];
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const executionFixtures = yield* Effect.forEach(executionIndexes, () =>
          Effect.gen(function* () {
            return {
              exited: yield* Deferred.make<"failure" | "success">(),
              release: yield* Deferred.make<undefined>(),
              started: yield* Deferred.make<undefined>(),
            };
          }),
        );
        const runtime = yield* makeEffect({
          modules: [],
          execution: { timeoutMs: 100, abortGraceMs: 50 },
          sandbox: {
            execute: (request) => {
              const executionIndex = Number(request.code);
              const fixture = executionFixtures[executionIndex];
              if (!Number.isInteger(executionIndex) || fixture === undefined) {
                return Effect.fail(
                  new Unknown({
                    rawCause: new Error(
                      `Unexpected execution code: ${request.code}`,
                    ),
                  }),
                );
              }
              return Effect.uninterruptible(
                Deferred.succeed(fixture.started, undefined).pipe(
                  Effect.andThen(Deferred.await(fixture.release)),
                  Effect.flatMap(() =>
                    executionIndex === 1
                      ? Effect.fail(
                          new Unknown({
                            rawCause: new Error(
                              "Selected late sandbox failure",
                            ),
                          }),
                        )
                      : Effect.succeed({ executionIndex }),
                  ),
                  Effect.onExit((exit) =>
                    Deferred.succeed(
                      fixture.exited,
                      Exit.isSuccess(exit) ? "success" : "failure",
                    ).pipe(Effect.asVoid),
                  ),
                ),
              );
            },
          },
        });
        const lease = yield* runtime.acquire();
        const run = (executionIndex: number, parts: unknown[]) =>
          Effect.gen(function* () {
            const prepared = yield* prepareTool(
              lease.executeTool,
              `late-settlement-${String(executionIndex)}`,
              String(executionIndex),
            );
            return yield* Stream.runForEach(prepared.execute(), (part) =>
              Effect.sync(() => {
                parts.push(part);
              }),
            );
          });
        const releaseAllSandboxes = Effect.all(
          executionFixtures.map((fixture) =>
            Deferred.succeed(fixture.release, undefined),
          ),
          { concurrency: "unbounded" },
        ).pipe(Effect.asVoid);
        const observeBounded = (
          first: Effect.Effect<string>,
          second: Effect.Effect<string>,
        ) =>
          Effect.gen(function* () {
            const observationFiber = yield* Effect.forkChild(
              Effect.raceFirst(
                Effect.raceFirst(first, second),
                Effect.sleep("1 millis").pipe(
                  Effect.as("still_pending" as const),
                ),
              ),
            );
            yield* TestClock.adjust("1 millis");
            return yield* Fiber.join(observationFiber);
          });
        const awaitBounded = <A>(observation: Effect.Effect<A>) =>
          Effect.gen(function* () {
            const observationFiber = yield* Effect.forkChild(
              Effect.raceFirst(
                observation.pipe(Effect.map(Option.some)),
                Effect.sleep("1 millis").pipe(Effect.as(Option.none<A>())),
              ),
            );
            yield* TestClock.adjust("1 millis");
            const observed = yield* Fiber.join(observationFiber);
            expect(Option.isSome(observed)).toBe(true);
            if (Option.isNone(observed)) {
              throw new Error(
                "Expected observation before the TestClock bound",
              );
            }
            return observed.value;
          });
        const expectCapacityRejection = (executionIndex: number) =>
          Effect.gen(function* () {
            const fixture = executionFixtures[executionIndex];
            if (fixture === undefined) {
              throw new Error("Expected capacity-rejection fixture");
            }
            const fiber = yield* Effect.forkChild(run(executionIndex, []));
            const first = yield* observeBounded(
              Fiber.await(fiber).pipe(Effect.as("settled" as const)),
              Deferred.await(fixture.started).pipe(
                Effect.as("sandbox_started" as const),
              ),
            );
            expect(first).toBe("settled");
            const exit = yield* Fiber.await(fiber);
            const failure = Exit.isFailure(exit)
              ? Cause.findErrorOption(exit.cause)
              : Option.none();
            expect(
              Option.isSome(failure) ? failure.value : undefined,
            ).toMatchObject({
              _tag: "MuleError",
              phase: "tool",
            });
            expect(
              Option.isSome(failure) ? failure.value : undefined,
            ).toHaveProperty(
              "message",
              expect.stringMatching(
                /^Sandbox execution capacity unavailable \(ref /u,
              ),
            );
            expect(Option.isNone(yield* Deferred.poll(fixture.started))).toBe(
              true,
            );
          });
        const startReplacement = (executionIndex: number) =>
          Effect.gen(function* () {
            const fixture = executionFixtures[executionIndex];
            if (fixture === undefined) {
              throw new Error("Expected replacement execution fixture");
            }
            const fiber = yield* Effect.forkChild(run(executionIndex, []));
            const first = yield* observeBounded(
              Deferred.await(fixture.started).pipe(
                Effect.as("sandbox_started" as const),
              ),
              Fiber.await(fiber).pipe(Effect.as("settled" as const)),
            );
            expect(first).toBe("sandbox_started");
            expect(
              Option.isNone(
                yield* Effect.sync(() =>
                  Option.fromUndefinedOr(fiber.pollUnsafe()),
                ),
              ),
            ).toBe(true);
            return { fiber, fixture };
          });

        return yield* Effect.gen(function* () {
          const initialAttempts = yield* Effect.forEach(
            executionIndexes.slice(0, 8),
            (executionIndex) => {
              const fixture = executionFixtures[executionIndex];
              if (fixture === undefined) {
                throw new Error("Expected initial execution fixture");
              }
              const parts =
                executionIndex === 0
                  ? lateSuccessParts
                  : executionIndex === 1
                    ? lateFailureParts
                    : [];
              return Effect.gen(function* () {
                return {
                  fiber: yield* Effect.forkChild(run(executionIndex, parts)),
                  fixture,
                };
              });
            },
          );
          const starts = yield* Effect.forEach(
            initialAttempts,
            (attempt) =>
              observeBounded(
                Deferred.await(attempt.fixture.started).pipe(
                  Effect.as("sandbox_started" as const),
                ),
                Fiber.await(attempt.fiber).pipe(Effect.as("settled" as const)),
              ),
            { concurrency: "unbounded" },
          );
          expect(starts).toEqual(initialAttempts.map(() => "sandbox_started"));
          expect(
            (yield* Effect.forEach(
              initialAttempts,
              (attempt) =>
                Effect.sync(() =>
                  Option.fromUndefinedOr(attempt.fiber.pollUnsafe()),
                ),
              { concurrency: "unbounded" },
            )).every(Option.isNone),
          ).toBe(true);

          yield* TestClock.adjust("100 millis");
          yield* TestClock.adjust("50 millis");
          const publishedExits = yield* Effect.forEach(
            initialAttempts,
            (attempt) => awaitBounded(Fiber.await(attempt.fiber)),
            { concurrency: "unbounded" },
          );
          expect(publishedExits.every(Exit.isFailure)).toBe(true);
          const expectedPublishedFailure = {
            _tag: "MuleError",
            phase: "tool",
          } as const;
          [0, 1].forEach((executionIndex) => {
            const exit = publishedExits[executionIndex];
            if (exit === undefined || Exit.isSuccess(exit)) {
              throw new Error("Expected published execution failure");
            }
            const failure = Cause.findErrorOption(exit.cause);
            expect(Option.isSome(failure)).toBe(true);
            if (Option.isNone(failure)) {
              throw new Error("Expected typed published execution failure");
            }
            expect(failure.value).toMatchObject(expectedPublishedFailure);
            expect(failure.value.message).toMatch(
              /sandbox did not acknowledge cancellation/u,
            );
          });
          expect(lateSuccessParts).toEqual([]);
          expect(lateFailureParts).toEqual([]);

          yield* expectCapacityRejection(8);

          const lateSuccess = initialAttempts[0];
          const lateFailure = initialAttempts[1];
          const rejectedNinth = executionFixtures[8];
          const rejectedAfterSuccess = executionFixtures[10];
          if (
            lateSuccess === undefined ||
            lateFailure === undefined ||
            rejectedNinth === undefined ||
            rejectedAfterSuccess === undefined
          ) {
            throw new Error("Expected selected late-settlement attempts");
          }
          yield* Deferred.succeed(lateSuccess.fixture.release, undefined);
          expect(
            yield* awaitBounded(Deferred.await(lateSuccess.fixture.exited)),
          ).toBe("success");
          const successFailureAfterExit = yield* Fiber.await(lateSuccess.fiber);
          const unchangedSuccessFailure = Exit.isFailure(
            successFailureAfterExit,
          )
            ? Cause.findErrorOption(successFailureAfterExit.cause)
            : Option.none();
          expect(
            Option.isSome(unchangedSuccessFailure)
              ? unchangedSuccessFailure.value
              : undefined,
          ).toMatchObject(expectedPublishedFailure);
          expect(lateSuccessParts).toEqual([]);

          const successReplacement = yield* startReplacement(9);
          expect(
            Option.isNone(yield* Deferred.poll(rejectedNinth.started)),
          ).toBe(true);
          yield* expectCapacityRejection(10);

          yield* Deferred.succeed(lateFailure.fixture.release, undefined);
          expect(
            yield* awaitBounded(Deferred.await(lateFailure.fixture.exited)),
          ).toBe("failure");
          const lateFailureAfterExit = yield* Fiber.await(lateFailure.fiber);
          const unchangedLateFailure = Exit.isFailure(lateFailureAfterExit)
            ? Cause.findErrorOption(lateFailureAfterExit.cause)
            : Option.none();
          expect(
            Option.isSome(unchangedLateFailure)
              ? unchangedLateFailure.value
              : undefined,
          ).toMatchObject(expectedPublishedFailure);
          expect(lateFailureParts).toEqual([]);

          const failureReplacement = yield* startReplacement(11);
          expect(
            Option.isNone(yield* Deferred.poll(rejectedAfterSuccess.started)),
          ).toBe(true);
          yield* expectCapacityRejection(12);

          yield* releaseAllSandboxes;
          yield* Effect.forEach(
            [...executionIndexes.slice(0, 8), 9, 11],
            (executionIndex) => {
              const fixture = executionFixtures[executionIndex];
              return fixture === undefined
                ? Effect.void
                : awaitBounded(Deferred.await(fixture.exited)).pipe(
                    Effect.asVoid,
                  );
            },
            { concurrency: "unbounded" },
          );
          const replacementExits = yield* Effect.forEach(
            [successReplacement, failureReplacement],
            (replacement) => awaitBounded(Fiber.await(replacement.fiber)),
            { concurrency: "unbounded" },
          );
          expect(replacementExits.every(Exit.isSuccess)).toBe(true);
        }).pipe(Effect.ensuring(releaseAllSandboxes));
      }),
    ).pipe(Effect.provide(TestClock.layer())),
  );
});

it("retired source waits for its registered backing work before release", async () => {
  let invalidateV1: (() => void) | undefined;
  let sourceAcquisitions = 0;
  let v1ReleaseCalls = 0;
  let v2ReleaseCalls = 0;

  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const backingStarted = yield* Deferred.make<undefined>();
        const releaseBacking = yield* Deferred.make<undefined>();
        const backingExited = yield* Deferred.make<undefined>();
        const v2SourceAcquired = yield* Deferred.make<undefined>();
        const v1ReleaseStarted = yield* Deferred.make<undefined>();
        const allowV1ReleaseToSettle = yield* Deferred.make<undefined>();
        const source = {
          id: "retired-source",
          acquire: (context: { invalidate: () => void }) => {
            sourceAcquisitions += 1;
            const version = `v${String(sourceAcquisitions)}`;
            if (version === "v1") {
              invalidateV1 = context.invalidate;
            }
            const acquisition = {
              snapshot: { modules: [], version },
              release: () => {
                if (version === "v1") {
                  v1ReleaseCalls += 1;
                  return Effect.runPromise(
                    Deferred.succeed(v1ReleaseStarted, undefined).pipe(
                      Effect.andThen(Deferred.await(allowV1ReleaseToSettle)),
                    ),
                  );
                }
                v2ReleaseCalls += 1;
                return Promise.resolve();
              },
            };
            return version === "v2"
              ? Effect.runPromise(
                  Deferred.succeed(v2SourceAcquired, undefined).pipe(
                    Effect.as(acquisition),
                  ),
                )
              : Promise.resolve(acquisition);
          },
        };
        const runtime = yield* makeEffect({
          execution: { timeoutMs: 100, abortGraceMs: 50 },
          modules: [],
          sandbox: {
            execute: () =>
              Effect.uninterruptible(
                Deferred.succeed(backingStarted, undefined).pipe(
                  Effect.andThen(Deferred.await(releaseBacking)),
                  Effect.as({ status: "settled" }),
                  Effect.ensuring(Deferred.succeed(backingExited, undefined)),
                ),
              ),
          },
          sources: [source],
        });
        const v1Lease = yield* runtime.acquire();
        const prepared = yield* prepareTool(
          v1Lease.executeTool,
          "retired-source-backing",
          "hostile",
        );
        const releaseHostGates = Effect.all(
          [
            Deferred.succeed(releaseBacking, undefined),
            Deferred.succeed(allowV1ReleaseToSettle, undefined),
          ],
          { concurrency: "unbounded" },
        ).pipe(Effect.asVoid);
        const observeBounded = (
          first: Effect.Effect<string>,
          second: Effect.Effect<string>,
        ) =>
          Effect.gen(function* () {
            const observationFiber = yield* Effect.forkChild(
              Effect.raceFirst(
                Effect.raceFirst(first, second),
                Effect.sleep("1 millis").pipe(
                  Effect.as("still_pending" as const),
                ),
              ),
            );
            yield* TestClock.adjust("1 millis");
            return yield* Fiber.join(observationFiber);
          });
        const awaitBounded = <A>(observation: Effect.Effect<A>) =>
          Effect.gen(function* () {
            const observationFiber = yield* Effect.forkChild(
              Effect.raceFirst(
                observation.pipe(Effect.map(Option.some)),
                Effect.sleep("1 millis").pipe(Effect.as(Option.none<A>())),
              ),
            );
            yield* TestClock.adjust("1 millis");
            const observed = yield* Fiber.join(observationFiber);
            expect(Option.isSome(observed)).toBe(true);
            if (Option.isNone(observed)) {
              throw new Error(
                "Expected observation before the TestClock bound",
              );
            }
            return observed.value;
          });

        return yield* Effect.gen(function* () {
          const executionFiber = yield* Effect.forkChild(
            Stream.runCollect(prepared.execute()),
          );
          expect(
            yield* observeBounded(
              Deferred.await(backingStarted).pipe(
                Effect.as("backing_started" as const),
              ),
              Fiber.await(executionFiber).pipe(
                Effect.as("execution_settled" as const),
              ),
            ),
          ).toBe("backing_started");

          yield* TestClock.adjust("100 millis");
          yield* TestClock.adjust("50 millis");
          const publishedExit = yield* awaitBounded(
            Fiber.await(executionFiber),
          );
          const publishedFailure = Exit.isFailure(publishedExit)
            ? Cause.findErrorOption(publishedExit.cause)
            : Option.none();
          expect(
            Option.isSome(publishedFailure)
              ? publishedFailure.value
              : undefined,
          ).toMatchObject({
            _tag: "MuleError",
            phase: "tool",
          });
          expect(
            Option.isSome(publishedFailure)
              ? publishedFailure.value
              : undefined,
          ).toHaveProperty(
            "message",
            expect.stringMatching(/sandbox did not acknowledge cancellation/u),
          );
          expect(Option.isNone(yield* Deferred.poll(backingExited))).toBe(true);

          yield* v1Lease.release();
          if (invalidateV1 === undefined) {
            throw new Error("Expected v1 source invalidation authority");
          }
          invalidateV1();
          const replacementFiber = yield* Effect.forkChild(runtime.acquire());
          yield* awaitBounded(Deferred.await(v2SourceAcquired));
          expect(sourceAcquisitions).toBe(2);

          const releaseBeforeBackingExit = yield* observeBounded(
            Deferred.await(v1ReleaseStarted).pipe(
              Effect.as("v1_release_started" as const),
            ),
            Fiber.await(replacementFiber).pipe(
              Effect.as("replacement_settled" as const),
            ),
          );
          const v1ReleaseCallsBeforeBackingExit = v1ReleaseCalls;

          yield* Deferred.succeed(releaseBacking, undefined);
          yield* awaitBounded(Deferred.await(backingExited));
          yield* awaitBounded(Deferred.await(v1ReleaseStarted));
          expect(v1ReleaseCalls).toBe(1);
          yield* Deferred.succeed(allowV1ReleaseToSettle, undefined);

          const replacementExit = yield* awaitBounded(
            Fiber.await(replacementFiber),
          );
          expect(Exit.isSuccess(replacementExit)).toBe(true);
          if (Exit.isFailure(replacementExit)) {
            throw new Error("Expected replacement source lease");
          }
          expect(replacementExit.value.revision).not.toBe(v1Lease.revision);
          yield* replacementExit.value.release();
          yield* runtime.close();

          expect(releaseBeforeBackingExit).toBe("still_pending");
          expect(v1ReleaseCallsBeforeBackingExit).toBe(0);
          expect(v1ReleaseCalls).toBe(1);
          expect(v2ReleaseCalls).toBe(1);
        }).pipe(Effect.ensuring(releaseHostGates));
      }),
    ).pipe(Effect.provide(TestClock.layer())),
  );
});

it("runtime close preserves pre-start running grace and unacknowledged phases before source cleanup", async () => {
  const phases = [
    "pre-start",
    "running",
    "cancellation-grace",
    "unacknowledged",
  ] as const;

  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        for (const phase of phases) {
          const sandboxCodes: string[] = [];
          const backingStarted = yield* Deferred.make<undefined>();
          const backingExited = yield* Deferred.make<undefined>();
          const releaseBacking = yield* Deferred.make<undefined>();
          const sourceReleaseStarted = yield* Deferred.make<undefined>();
          const allowSourceRelease = yield* Deferred.make<undefined>();
          const releaseEvents: string[] = [];
          const source = {
            id: `close-${phase}`,
            acquire: () =>
              Promise.resolve({
                snapshot: { version: "v1", modules: [] },
                release: () => {
                  releaseEvents.push("release");
                  return Effect.runPromise(
                    Deferred.succeed(sourceReleaseStarted, undefined).pipe(
                      Effect.andThen(Deferred.await(allowSourceRelease)),
                    ),
                  );
                },
              }),
          };
          const runtime = yield* makeEffect({
            execution: { timeoutMs: 100, abortGraceMs: 50 },
            modules: [],
            sandbox: {
              execute: (request) => {
                sandboxCodes.push(request.code);
                if (request.code !== "held") {
                  return Effect.succeed({ code: request.code });
                }
                return Effect.uninterruptible(
                  Deferred.succeed(backingStarted, undefined).pipe(
                    Effect.andThen(Deferred.await(releaseBacking)),
                    Effect.ensuring(Deferred.succeed(backingExited, undefined)),
                    Effect.as({ code: request.code }),
                  ),
                );
              },
            },
            sources: [source],
          });

          if (phase === "pre-start") {
            const closeFiber = yield* Effect.forkChild(runtime.close());
            yield* TestClock.adjust("1 millis");
            expect(
              Option.isNone(
                yield* Effect.sync(() =>
                  Option.fromUndefinedOr(closeFiber.pollUnsafe()),
                ),
              ),
            ).toBe(false);
            const acquisition = yield* Effect.exit(runtime.acquire());
            expect(Exit.isFailure(acquisition)).toBe(true);
            const acquisitionFailure = Exit.isFailure(acquisition)
              ? Cause.findErrorOption(acquisition.cause)
              : Option.none();
            expect(
              Option.isSome(acquisitionFailure)
                ? acquisitionFailure.value
                : undefined,
            ).toMatchObject({ code: "runtime_closed" });
            yield* Fiber.join(closeFiber);
            yield* runtime.close();
            expect(sandboxCodes).toEqual([]);
            expect(releaseEvents).toEqual([]);
            continue;
          }

          const lease = yield* runtime.acquire();
          const heldPrepared = yield* prepareTool(
            lease.executeTool,
            `close-${phase}-held`,
            "held",
          );
          const heldFiber = yield* Effect.forkChild(
            Effect.exit(Stream.runCollect(heldPrepared.execute())),
          );
          yield* Deferred.await(backingStarted);

          if (phase === "cancellation-grace" || phase === "unacknowledged") {
            yield* TestClock.adjust("100 millis");
            if (phase === "unacknowledged") {
              yield* TestClock.adjust("50 millis");
              expect(
                Option.isNone(
                  yield* Effect.sync(() =>
                    Option.fromUndefinedOr(heldFiber.pollUnsafe()),
                  ),
                ),
              ).toBe(false);
            } else {
              yield* TestClock.adjust("25 millis");
              expect(
                Option.isNone(
                  yield* Effect.sync(() =>
                    Option.fromUndefinedOr(heldFiber.pollUnsafe()),
                  ),
                ),
              ).toBe(true);
            }
          }

          const firstClose = yield* Effect.forkChild(runtime.close());
          const repeatedClose = yield* Effect.forkChild(runtime.close());
          yield* TestClock.adjust("1 millis");
          expect(
            Option.isNone(
              yield* Effect.sync(() =>
                Option.fromUndefinedOr(firstClose.pollUnsafe()),
              ),
            ),
          ).toBe(true);
          expect(
            Option.isNone(
              yield* Effect.sync(() =>
                Option.fromUndefinedOr(repeatedClose.pollUnsafe()),
              ),
            ),
          ).toBe(true);

          const probePrepared = yield* prepareTool(
            lease.executeTool,
            `close-${phase}-probe`,
            "probe",
          );
          const probeParts = yield* Stream.runCollect(probePrepared.execute());
          expect(probeParts).toHaveLength(1);
          expect(sandboxCodes).toEqual(["held", "probe"]);

          const leaseRelease = yield* Effect.forkChild(lease.release());
          yield* TestClock.adjust("1 millis");
          expect(
            Option.isNone(
              yield* Effect.sync(() =>
                Option.fromUndefinedOr(leaseRelease.pollUnsafe()),
              ),
            ),
          ).toBe(true);
          expect(
            Option.isNone(
              yield* Effect.sync(() =>
                Option.fromUndefinedOr(firstClose.pollUnsafe()),
              ),
            ),
          ).toBe(true);
          expect(
            Option.isNone(
              yield* Effect.sync(() =>
                Option.fromUndefinedOr(repeatedClose.pollUnsafe()),
              ),
            ),
          ).toBe(true);
          expect(
            Option.isNone(yield* Deferred.poll(sourceReleaseStarted)),
          ).toBe(true);

          yield* Deferred.succeed(releaseBacking, undefined);
          yield* Deferred.await(backingExited);
          yield* Deferred.await(sourceReleaseStarted);
          yield* TestClock.adjust("1 millis");
          expect(
            Option.isNone(
              yield* Effect.sync(() =>
                Option.fromUndefinedOr(leaseRelease.pollUnsafe()),
              ),
            ),
          ).toBe(true);
          expect(
            Option.isNone(
              yield* Effect.sync(() =>
                Option.fromUndefinedOr(firstClose.pollUnsafe()),
              ),
            ),
          ).toBe(true);
          expect(
            Option.isNone(
              yield* Effect.sync(() =>
                Option.fromUndefinedOr(repeatedClose.pollUnsafe()),
              ),
            ),
          ).toBe(true);

          yield* Deferred.succeed(allowSourceRelease, undefined);
          yield* Fiber.join(leaseRelease);
          yield* Fiber.join(firstClose);
          yield* Fiber.join(repeatedClose);
          yield* runtime.close();
          expect(releaseEvents).toEqual(["release"]);
          yield* Fiber.await(heldFiber);
        }
      }),
    ).pipe(Effect.provide(TestClock.layer())),
  );
});

it("default facade shares bounded unacknowledged host-work settlement", async () => {
  const result = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const sandboxCodes: string[] = [];
        const backingStarted = yield* Deferred.make<undefined>();
        const backingExited = yield* Deferred.make<undefined>();
        const releaseBacking = yield* Deferred.make<undefined>();
        const sourceReleaseStarted = yield* Deferred.make<undefined>();
        const allowSourceRelease = yield* Deferred.make<undefined>();
        const releaseEvents: string[] = [];
        const runtime = yield* Effect.tryPromise({
          try: () =>
            Mycelium.make({
              modules: [],
              sources: [
                {
                  id: "default-close-source",
                  acquire: () =>
                    Promise.resolve({
                      snapshot: { version: "v1", modules: [] },
                      release: () => {
                        releaseEvents.push("release");
                        return Effect.runPromise(
                          Deferred.succeed(
                            sourceReleaseStarted,
                            undefined,
                          ).pipe(
                            Effect.andThen(Deferred.await(allowSourceRelease)),
                          ),
                        );
                      },
                    }),
                },
              ],
              sandbox: {
                execute: (request) => {
                  sandboxCodes.push(request.code);
                  if (request.code !== "held") {
                    return Promise.resolve({ code: request.code });
                  }
                  return Effect.runPromise(
                    Deferred.succeed(backingStarted, undefined).pipe(
                      Effect.andThen(Deferred.await(releaseBacking)),
                      Effect.ensuring(
                        Deferred.succeed(backingExited, undefined),
                      ),
                      Effect.as({ code: request.code }),
                    ),
                  );
                },
              },
            }),
          catch: parseError,
        });
        const lease = yield* Effect.tryPromise({
          try: () => runtime.acquire(),
          catch: parseError,
        });
        const prepared = yield* Effect.tryPromise({
          try: (signal) =>
            preparePromiseTool(
              lease.executeTool,
              "default-close-held",
              "held",
              signal,
            ),
          catch: parseError,
        });
        const heldFiber = yield* Effect.forkChild(
          Effect.tryPromise({
            try: (signal) => executePromiseTool(prepared, signal),
            catch: parseError,
          }),
        );
        yield* Deferred.await(backingStarted);

        const firstClose = yield* Effect.forkChild(
          Effect.tryPromise({ try: () => runtime.close(), catch: parseError }),
        );
        const repeatedClose = yield* Effect.forkChild(
          Effect.tryPromise({ try: () => runtime.close(), catch: parseError }),
        );
        yield* TestClock.adjust("1 millis");
        const firstCloseBeforeBacking = Option.isNone(
          yield* Effect.sync(() =>
            Option.fromUndefinedOr(firstClose.pollUnsafe()),
          ),
        );
        const repeatedCloseBeforeBacking = Option.isNone(
          yield* Effect.sync(() =>
            Option.fromUndefinedOr(repeatedClose.pollUnsafe()),
          ),
        );

        const probePrepared = yield* Effect.tryPromise({
          try: (signal) =>
            preparePromiseTool(
              lease.executeTool,
              "default-close-probe",
              "probe",
              signal,
            ),
          catch: parseError,
        });
        const probeParts = yield* Effect.tryPromise({
          try: (signal) => executePromiseTool(probePrepared, signal),
          catch: parseError,
        });

        const leaseRelease = yield* Effect.forkChild(
          Effect.tryPromise({ try: () => lease.release(), catch: parseError }),
        );
        yield* TestClock.adjust("1 millis");
        const leaseReleaseBeforeBacking = Option.isNone(
          yield* Effect.sync(() =>
            Option.fromUndefinedOr(leaseRelease.pollUnsafe()),
          ),
        );
        const sourceReleaseBeforeBacking = Option.isNone(
          yield* Deferred.poll(sourceReleaseStarted),
        );

        yield* Deferred.succeed(releaseBacking, undefined);
        yield* Deferred.await(backingExited);
        yield* Deferred.await(sourceReleaseStarted);
        yield* TestClock.adjust("1 millis");
        const leaseReleaseBeforeSource = Option.isNone(
          yield* Effect.sync(() =>
            Option.fromUndefinedOr(leaseRelease.pollUnsafe()),
          ),
        );
        const firstCloseBeforeSource = Option.isNone(
          yield* Effect.sync(() =>
            Option.fromUndefinedOr(firstClose.pollUnsafe()),
          ),
        );
        const repeatedCloseBeforeSource = Option.isNone(
          yield* Effect.sync(() =>
            Option.fromUndefinedOr(repeatedClose.pollUnsafe()),
          ),
        );

        yield* Deferred.succeed(allowSourceRelease, undefined);
        yield* Fiber.join(leaseRelease);
        yield* Fiber.join(firstClose);
        yield* Fiber.join(repeatedClose);
        yield* Fiber.join(heldFiber);

        return {
          firstCloseBeforeBacking,
          repeatedCloseBeforeBacking,
          leaseReleaseBeforeBacking,
          sourceReleaseBeforeBacking,
          leaseReleaseBeforeSource,
          firstCloseBeforeSource,
          repeatedCloseBeforeSource,
          probeParts,
          sandboxCodes,
          releaseEvents,
        };
      }),
    ).pipe(Effect.provide(TestClock.layer())),
  );

  expect(result.firstCloseBeforeBacking).toBe(true);
  expect(result.repeatedCloseBeforeBacking).toBe(true);
  expect(result.leaseReleaseBeforeBacking).toBe(true);
  expect(result.sourceReleaseBeforeBacking).toBe(true);
  expect(result.leaseReleaseBeforeSource).toBe(true);
  expect(result.firstCloseBeforeSource).toBe(true);
  expect(result.repeatedCloseBeforeSource).toBe(true);
  expect(result.probeParts).toHaveLength(1);
  expect(result.sandboxCodes).toEqual(["held", "probe"]);
  expect(result.releaseEvents).toEqual(["release"]);
});

it("Effect hosts adapting a Promise sandbox keep abort_unacknowledged work before source release", async () => {
  const releaseEvents: string[] = [];
  let hostSignal: AbortSignal | undefined;
  let settleHost: ((value: SandboxValue) => void) | undefined;
  const result = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const hostStarted = yield* Deferred.make<undefined>();
        // The exact adapter shape an Effect host writes over Promise work
        // that deliberately ignores abort.
        const hostSandbox: Sandbox = {
          execute: (_request, signal) => {
            hostSignal = signal;
            Effect.runSync(Deferred.succeed(hostStarted, undefined));
            return new Promise<SandboxValue>((resolve) => {
              settleHost = resolve;
            });
          },
        };
        const runtime = yield* makeEffect({
          execution: { timeoutMs: 100, abortGraceMs: 50 },
          sources: [
            {
              id: "promise-host-source",
              acquire: () =>
                Promise.resolve({
                  snapshot: { version: "v1", modules: [] },
                  release: () => {
                    releaseEvents.push("release");
                    return Promise.resolve();
                  },
                }),
            },
          ],
          sandbox: fromTypedSandbox(hostSandbox),
        });
        const lease = yield* runtime.acquire();
        const prepared = yield* prepareTool(
          lease.executeTool,
          "promise-host-deadline",
          "return never()",
        );
        const executionFiber = yield* Effect.forkChild(
          Effect.exit(Stream.runCollect(prepared.execute())),
        );
        yield* Deferred.await(hostStarted);

        yield* TestClock.adjust("100 millis");
        const settledBeforeGrace = Option.isSome(
          yield* Effect.sync(() =>
            Option.fromUndefinedOr(executionFiber.pollUnsafe()),
          ),
        );
        const abortedAtDeadline = hostSignal?.aborted === true;
        yield* TestClock.adjust("50 millis");
        const executionExit = yield* Fiber.join(executionFiber);

        yield* lease.release();
        const closeFiber = yield* Effect.forkChild(runtime.close());
        yield* TestClock.adjust("1 millis");
        const closedBeforeHostSettled = Option.isSome(
          yield* Effect.sync(() =>
            Option.fromUndefinedOr(closeFiber.pollUnsafe()),
          ),
        );
        const releasesBeforeHostSettled = releaseEvents.length;

        settleHost?.({ late: true });
        yield* Fiber.join(closeFiber);

        return {
          abortedAtDeadline,
          closedBeforeHostSettled,
          executionExit,
          releasesBeforeHostSettled,
          settledBeforeGrace,
        };
      }),
    ).pipe(Effect.provide(TestClock.layer())),
  );

  const failure = Exit.isFailure(result.executionExit)
    ? Cause.findErrorOption(result.executionExit.cause)
    : Option.none();
  expect(result.abortedAtDeadline).toBe(true);
  expect(result.settledBeforeGrace).toBe(false);
  expect(Option.isSome(failure) ? failure.value : undefined).toMatchObject({
    _tag: "MuleError",
    phase: "tool",
  });
  expect(Option.isSome(failure) ? failure.value : undefined).toHaveProperty(
    "message",
    expect.stringMatching(/sandbox did not acknowledge cancellation/u),
  );
  expect(result.closedBeforeHostSettled).toBe(false);
  expect(result.releasesBeforeHostSettled).toBe(0);
  expect(releaseEvents).toEqual(["release"]);
});

type FailingOnceSourceState = {
  releaseCalls: number;
  invalidate?: () => void;
};

function failingOnceSource(id: string) {
  const state: FailingOnceSourceState = { releaseCalls: 0 };
  let acquisitions = 0;
  const source = {
    id,
    acquire: ({ invalidate }: { invalidate: () => void }) => {
      acquisitions += 1;
      const version = `v${String(acquisitions)}`;
      state.invalidate ??= invalidate;
      return Promise.resolve({
        snapshot: { version, modules: [] },
        release: () => {
          if (version !== "v1") return Promise.resolve();
          state.releaseCalls += 1;
          return state.releaseCalls === 1
            ? Promise.reject(new Error("Source release failed once"))
            : Promise.resolve();
        },
      });
    },
  };
  return { source, state };
}

it("Promise close retried after a failed close reports the retained failure", async () => {
  const { source, state } = failingOnceSource("close-retry-source");
  const runtime = await Mycelium.make({ sources: [source], sandbox });
  const lease = await runtime.acquire();
  await lease.release();

  const first = await runtime.close().then(
    () => undefined,
    (cause: unknown) => cause,
  );
  const retry = await runtime.close().then(
    () => undefined,
    (cause: unknown) => cause,
  );

  expect(first).toMatchObject({
    code: "source_failure",
    phase: "release",
    sourceId: "close-retry-source",
  });
  expect(retry).toBe(first);
  expect(state.releaseCalls).toBe(1);
  await expect(runtime.acquire()).rejects.toMatchObject({
    code: "runtime_closed",
  });
});

it("Promise lease release retried after a failed release reports the retained failure", async () => {
  const { source, state } = failingOnceSource("release-retry-source");
  const runtime = await Mycelium.make({ sources: [source], sandbox });
  const retiredLease = await runtime.acquire();
  state.invalidate?.();
  const currentLease = await runtime.acquire();
  expect(currentLease.revision).not.toBe(retiredLease.revision);

  const first = await retiredLease.release().then(
    () => undefined,
    (cause: unknown) => cause,
  );
  const retry = await retiredLease.release().then(
    () => undefined,
    (cause: unknown) => cause,
  );

  expect(first).toMatchObject({
    code: "source_failure",
    phase: "release",
    sourceId: "release-retry-source",
  });
  expect(retry).toBe(first);
  expect(state.releaseCalls).toBe(1);
  await currentLease.release();
  await expect(runtime.close()).rejects.toBe(first);
});

it("Effect lease release retried after a failed release reports the retained failure", async () => {
  const { source, state } = failingOnceSource("effect-release-retry-source");
  const outcome = await Effect.runPromise(
    Effect.gen(function* () {
      const runtimeScope = yield* Scope.make();
      const runtime = yield* makeEffect({
        sources: [source],
        sandbox: { execute: () => Effect.succeed(null) },
      }).pipe(Scope.provide(runtimeScope));
      const leaseScope = yield* Scope.make();
      const retiredLease = yield* runtime
        .acquire()
        .pipe(Scope.provide(leaseScope));
      state.invalidate?.();
      const currentLease = yield* runtime
        .acquire()
        .pipe(Scope.provide(leaseScope));
      const first = yield* Effect.flip(retiredLease.release());
      const retry = yield* Effect.flip(retiredLease.release());
      yield* currentLease.release();
      const close = yield* Effect.flip(runtime.close());
      return { close, first, retry };
    }),
  );

  expect(outcome.first).toMatchObject({
    code: "source_failure",
    phase: "release",
    sourceId: "effect-release-retry-source",
  });
  expect(outcome.retry).toBe(outcome.first);
  expect(outcome.close).toBe(outcome.first);
  expect(state.releaseCalls).toBe(1);
});

describe("Mycelium capability plane", () => {
  it("rejects invalid execution budgets before sandbox execution", async () => {
    let sandboxCalls = 0;
    for (const execution of [
      { timeoutMs: 0 },
      { timeoutMs: 900_001 },
      { abortGraceMs: 0 },
      { abortGraceMs: 30_001 },
    ]) {
      await expect(
        Mycelium.make({
          execution,
          modules: [],
          sandbox: directSandbox(() => {
            sandboxCalls += 1;
            return Promise.resolve(null);
          }),
        }),
      ).rejects.toMatchObject({ code: "invalid_definition" });
    }
    expect(sandboxCalls).toBe(0);
  });

  it("derives collision-safe execution aliases from sorted stable module ids", async () => {
    const computer01 = Mycelium.module({
      id: "computer-01",
      name: "GPU box",
      description: "Computer 01 namespace",
      operations: {
        status: Mycelium.operation({
          description: "Read computer 01 status",
          input: z.object({}),
          output: z.object({ computer: z.literal("01") }),
          execute: () => Promise.resolve({ computer: "01" }),
        }),
      },
    });
    const computer02 = Mycelium.module({
      id: "computer-02",
      name: "GPU box",
      description: "Computer 02 namespace",
      operations: {
        status: Mycelium.operation({
          description: "Read computer 02 status",
          input: z.object({}),
          output: z.object({ computer: z.literal("02") }),
          execute: () => Promise.resolve({ computer: "02" }),
        }),
      },
    });
    const observedFirst: unknown[] = [];
    const observedSecond: unknown[] = [];
    const createSandbox = (observed: unknown[]): Sandbox => ({
      execute: async (request) => {
        expect(Object.keys(request.bindings)).toEqual([
          "GPU_box",
          "GPU_box_2",
          "forage",
        ]);
        const alias =
          request.code === "return GPU_box.status({})"
            ? "GPU_box"
            : "GPU_box_2";
        const status = request.bindings[alias]?.status;
        if (status === undefined) {
          throw new Error(`Expected ${alias}.status sandbox binding`);
        }
        const value = await status({});
        observed.push(value);
        return value;
      },
    });
    const invoke = async (
      executeTool: MuleTool,
      code: string,
      callId: string,
    ): Promise<void> => {
      const prepared = await preparePromiseTool(executeTool, callId, code);
      const parts = await executePromiseTool(prepared);
      expect(parts).toHaveLength(1);
    };

    await expect(
      (async () => {
        const firstRuntime = await Mycelium.make({
          modules: [computer02, computer01],
          sandbox: createSandbox(observedFirst),
        });
        const secondRuntime = await Mycelium.make({
          modules: [computer01, computer02],
          sandbox: createSandbox(observedSecond),
        });
        const firstLease = await firstRuntime.acquire();
        const secondLease = await secondRuntime.acquire();

        try {
          expect(firstLease.revision).toBe(secondLease.revision);
          expect(firstLease.catalog.stamp()).toBe(secondLease.catalog.stamp());
          expect(
            firstLease.catalog.describe({ path: "GPU_box" }),
          ).toMatchObject({
            kind: "module",
            description: "Computer 01 namespace",
          });
          expect(
            firstLease.catalog.describe({ path: "GPU_box_2" }),
          ).toMatchObject({
            kind: "module",
            description: "Computer 02 namespace",
          });
          expect(() =>
            firstLease.catalog.describe({ path: "computer-01" }),
          ).toThrow();

          await invoke(
            firstLease.executeTool,
            "return GPU_box.status({})",
            "alias-1",
          );
          await invoke(
            firstLease.executeTool,
            "return GPU_box_2.status({})",
            "alias-2",
          );
          await invoke(
            secondLease.executeTool,
            "return GPU_box.status({})",
            "alias-3",
          );
          await invoke(
            secondLease.executeTool,
            "return GPU_box_2.status({})",
            "alias-4",
          );
          expect(observedFirst).toEqual([
            { computer: "01" },
            { computer: "02" },
          ]);
          expect(observedSecond).toEqual([
            { computer: "01" },
            { computer: "02" },
          ]);
        } finally {
          await firstLease.release();
          await secondLease.release();
          await firstRuntime.close();
          await secondRuntime.close();
        }
      })(),
    ).resolves.toBeUndefined();
  });

  it("keeps installed authority identity separate from catalog content", async () => {
    const moduleWithId = (id: string, name = "GPU box") =>
      Mycelium.module({
        id,
        name,
        description: "GPU namespace",
        operations: {
          status: Mycelium.operation({
            description: "Read GPU status",
            input: z.object({}),
            output: z.object({ ok: z.boolean() }),
            execute: () => Promise.resolve({ ok: true }),
          }),
        },
      });

    await expect(
      (async () => {
        const firstRuntime = await Mycelium.make({
          modules: [moduleWithId("stable-a")],
          sandbox,
        });
        const secondRuntime = await Mycelium.make({
          modules: [moduleWithId("stable-b")],
          sandbox,
        });
        const firstLease = await firstRuntime.acquire();
        const secondLease = await secondRuntime.acquire();

        try {
          expect(firstLease.catalog.stamp()).toBe(secondLease.catalog.stamp());
          expect(firstLease.revision).not.toBe(secondLease.revision);
        } finally {
          await firstLease.release();
          await secondLease.release();
          await firstRuntime.close();
          await secondRuntime.close();
        }

        let sandboxCalls = 0;
        const duplicateRuntime = Mycelium.make({
          modules: [moduleWithId("duplicate"), moduleWithId("duplicate")],
          sandbox: directSandbox(() => {
            sandboxCalls += 1;
            return Promise.reject(
              new Error("Duplicate declarations must not reach sandbox"),
            );
          }),
        });
        await expect(duplicateRuntime).rejects.toBeInstanceOf(
          MyceliumDefinitionError,
        );
        await expect(duplicateRuntime).rejects.toMatchObject({
          code: "invalid_definition",
        });
        expect(sandboxCalls).toBe(0);

        const reservedRuntime = await Mycelium.make({
          modules: [moduleWithId("reserved-module", "forage")],
          sandbox,
        });
        const reservedLease = await reservedRuntime.acquire();
        try {
          expect(
            reservedLease.catalog.describe({ path: "forage_2.status" }),
          ).toMatchObject({ path: "forage_2.status" });
          expect(
            reservedLease.catalog.describe({ path: "forage.search" }),
          ).toMatchObject({ path: "forage.search" });
          expect(
            reservedLease.catalog.describe({ path: "forage.describe" }),
          ).toMatchObject({ path: "forage.describe" });
        } finally {
          await reservedLease.release();
          await reservedRuntime.close();
        }
      })(),
    ).resolves.toBeUndefined();
  });

  it("moves the installed revision when only an operation schema changes", async () => {
    const goals = (goal: z.ZodString | z.ZodNumber) =>
      Mycelium.module({
        id: "session-goal",
        description: "Session goal operations",
        operations: {
          create_goal: Mycelium.operation({
            description: "Create a session goal",
            input: z.object({ goal }),
            execute: () => Promise.resolve(null),
          }),
        },
      });
    const notes = Mycelium.module({
      id: "notes",
      operations: {
        read: Mycelium.operation({
          input: z.object({ id: z.string() }),
          execute: () => Promise.resolve(null),
        }),
      },
    });
    const revisionOf = async (modules: readonly ModuleRegistration[]) => {
      const runtime = await Mycelium.make({ modules, sandbox });
      const lease = await runtime.acquire();
      await lease.release();
      await runtime.close();
      return lease.revision;
    };

    const base = await revisionOf([goals(z.string()), notes]);
    expect(await revisionOf([notes, goals(z.string())])).toBe(base);
    expect(await revisionOf([goals(z.number()), notes])).not.toBe(base);
  });

  it("projects a source module through the single execute door", async () => {
    let sourceAcquires = 0;
    const sourceModule = Mycelium.module({
      id: "source-module",
      name: "source",
      operations: {
        ping: Mycelium.operation({
          input: z.object({ value: z.string() }),
          output: z.object({ value: z.string() }),
          execute: ({ value }) => Promise.resolve({ value }),
        }),
      },
    });
    const runtime = await Mycelium.make({
      sources: [
        {
          id: "source-one",
          acquire: ({ signal, invalidate }) => {
            expect(signal).toBeInstanceOf(AbortSignal);
            expect(invalidate).toBeTypeOf("function");
            sourceAcquires += 1;
            return Promise.resolve({
              snapshot: { version: "v1", modules: [sourceModule] },
              release: () => Promise.resolve(),
            });
          },
        },
      ],
      sandbox: {
        execute: async (request) => {
          expect(request.code).toBe('return source.ping({ value: "pong" })');
          expect(Object.keys(request.bindings)).toEqual(["source", "forage"]);
          const ping = request.bindings.source?.ping;
          if (ping === undefined) {
            throw new Error("Expected source.ping sandbox binding");
          }
          return ping({ value: "pong" });
        },
      },
    });
    const lease = await runtime.acquire();

    try {
      expect(lease.executeTool.descriptor.name).toBe("execute");
      const prepared = await preparePromiseTool(
        lease.executeTool,
        "source-execute-1",
        'return source.ping({ value: "pong" })',
      );
      const parts = await executePromiseTool(prepared);
      expect(parts).toEqual([
        {
          type: "result",
          result: {
            content: [{ type: "text", text: '{"value":"pong"}' }],
            isError: false,
          },
        },
      ]);
      expect(sourceAcquires).toBe(1);
    } finally {
      await lease.release();
      await runtime.close();
    }
  });

  it("awaits source release exactly once across lease release and close", async () => {
    let releaseCalls = 0;
    let settleRelease: (() => void) | undefined;
    let signalReleaseStarted: (() => void) | undefined;
    const releaseStartedLatch = new Promise<void>((resolve) => {
      signalReleaseStarted = resolve;
    });
    let releaseStarted = false;
    const releaseGate = new Promise<void>((resolve) => {
      settleRelease = resolve;
    });
    const runtime = await Mycelium.make({
      sources: [
        {
          id: "release-source",
          acquire: () =>
            Promise.resolve({
              snapshot: { version: "v1", modules: [] },
              release: async () => {
                releaseCalls += 1;
                releaseStarted = true;
                signalReleaseStarted?.();
                await releaseGate;
              },
            }),
        },
      ],
      sandbox,
    });
    const lease = await runtime.acquire();

    let runtimeCloseSettled = false;
    const runtimeClose = runtime.close().then(() => {
      runtimeCloseSettled = true;
    });
    await expect(runtime.acquire()).rejects.toMatchObject({
      code: "runtime_closed",
    });
    let leaseReleaseSettled = false;
    const leaseRelease = lease.release().then(() => {
      leaseReleaseSettled = true;
    });
    await releaseStartedLatch;
    expect(releaseStarted).toBe(true);
    expect(releaseCalls).toBe(1);
    expect(leaseReleaseSettled).toBe(false);
    expect(runtimeCloseSettled).toBe(false);
    expect(settleRelease).toBeTypeOf("function");
    settleRelease?.();
    await leaseRelease;
    await runtimeClose;
    await lease.release();
    await runtime.close();
    expect(releaseCalls).toBe(1);
  });

  it("releases source acquisitions resolved immediately before or after cancellation", async () => {
    for (const timing of ["before", "after"] as const) {
      type Acquisition = {
        snapshot: { version: string; modules: never[] };
        release: () => Promise<void>;
      };
      let resolveAcquisition: ((acquisition: Acquisition) => void) | undefined;
      const acquisitionGate = new Promise<Acquisition>((resolve) => {
        resolveAcquisition = resolve;
      });
      let releaseCalls = 0;
      let signalReleaseStarted: (() => void) | undefined;
      const releaseStarted = new Promise<void>((resolve) => {
        signalReleaseStarted = resolve;
      });
      let settleRelease: (() => void) | undefined;
      const releaseGate = new Promise<void>((resolve) => {
        settleRelease = resolve;
      });
      let signalSourceStarted: (() => void) | undefined;
      const sourceStarted = new Promise<void>((resolve) => {
        signalSourceStarted = resolve;
      });
      const runtime = await Mycelium.make({
        sources: [
          {
            id: `cancelled-${timing}-handoff-source`,
            acquire: () => {
              signalSourceStarted?.();
              return acquisitionGate;
            },
          },
        ],
        sandbox,
      });
      const controller = new AbortController();
      let acquisitionSettled = false;
      const acquisition = runtime
        .acquire({ signal: controller.signal })
        .finally(() => {
          acquisitionSettled = true;
        });
      const acquired: Acquisition = {
        snapshot: { version: "v1", modules: [] },
        release: async () => {
          releaseCalls += 1;
          signalReleaseStarted?.();
          await releaseGate;
        },
      };

      await sourceStarted;
      if (timing === "before") resolveAcquisition?.(acquired);
      controller.abort();
      if (timing === "after") {
        await Promise.resolve();
        expect(acquisitionSettled).toBe(false);
        resolveAcquisition?.(acquired);
      }

      const firstSettlement = await Promise.race([
        acquisition.then(
          () => "acquisition" as const,
          () => "acquisition" as const,
        ),
        releaseStarted.then(() => "release" as const),
      ]);
      expect(firstSettlement).toBe("release");
      expect(releaseCalls).toBe(1);
      expect(acquisitionSettled).toBe(false);
      settleRelease?.();
      await expect(acquisition).rejects.toMatchObject({ name: "AbortError" });
      await runtime.close();
      expect(releaseCalls).toBe(1);
    }
  });

  it("rejects invalid and duplicate source ids before source acquisition", async () => {
    const sandboxCalls = { value: 0 };
    const source = (id: string) => ({
      id,
      acquire: () => {
        sourceAcquires += 1;
        return Promise.resolve({
          snapshot: { version: "v1", modules: [] },
          release: () => Promise.resolve(),
        });
      },
    });
    let sourceAcquires = 0;
    const sandboxWithCount = {
      execute: (): Promise<never> => {
        sandboxCalls.value += 1;
        return Promise.reject(new Error("Sandbox must remain untouched"));
      },
    };

    for (const invalidId of ["", "x".repeat(161)]) {
      const runtime = await Mycelium.make({
        sources: [source(invalidId)],
        sandbox: sandboxWithCount,
      });
      try {
        const acquireWithoutLease = async () => {
          const lease = await runtime.acquire();
          await lease.release();
          throw new Error("Expected invalid source id to reject");
        };
        await expect(acquireWithoutLease()).rejects.toMatchObject({
          code: "invalid_definition",
        });
      } finally {
        await runtime.close();
      }
    }

    const duplicateRuntime = await Mycelium.make({
      sources: [source("duplicate"), source("duplicate")],
      sandbox: sandboxWithCount,
    });
    try {
      const acquireWithoutLease = async () => {
        const lease = await duplicateRuntime.acquire();
        await lease.release();
        throw new Error("Expected duplicate source ids to reject");
      };
      await expect(acquireWithoutLease()).rejects.toMatchObject({
        code: "invalid_definition",
      });
    } finally {
      await duplicateRuntime.close();
    }

    const excessiveRuntime = await Mycelium.make({
      sources: Array.from({ length: 129 }, (_, index) =>
        source(`source-${String(index).padStart(3, "0")}`),
      ),
      sandbox: sandboxWithCount,
    });
    try {
      const acquireWithoutLease = async () => {
        const lease = await excessiveRuntime.acquire();
        await lease.release();
        throw new Error("Expected excessive source count to reject");
      };
      await expect(acquireWithoutLease()).rejects.toMatchObject({
        code: "invalid_definition",
      });
    } finally {
      await excessiveRuntime.close();
    }

    expect(sourceAcquires).toBe(0);
    expect(sandboxCalls.value).toBe(0);
  });

  it.each([
    {
      name: "an empty version",
      snapshot: () => ({ version: "", modules: [] }),
    },
    {
      name: "a version beyond the code-unit bound",
      snapshot: () => ({ version: "x".repeat(161), modules: [] }),
    },
    {
      name: "more than one thousand total modules",
      snapshot: () => ({
        version: "v1",
        modules: Array.from({ length: 1001 }, (_, index) =>
          Mycelium.module({
            id: `source-module-${String(index)}`,
            operations: {},
          }),
        ),
      }),
    },
  ])(
    "rejects a source snapshot with $name before installation and releases it once",
    async ({ snapshot }) => {
      let resolveReleaseStarted: (() => void) | undefined;
      const releaseStarted = new Promise<void>((resolve) => {
        resolveReleaseStarted = resolve;
      });
      let resolveRelease: (() => void) | undefined;
      const releaseGate = new Promise<void>((resolve) => {
        resolveRelease = resolve;
      });
      let releaseCalls = 0;
      let releaseSettled = false;
      let sandboxCalls = 0;
      const runtime = await Mycelium.make({
        sources: [
          {
            id: "invalid-snapshot-source",
            acquire: () =>
              Promise.resolve({
                snapshot: snapshot(),
                release: async () => {
                  releaseCalls += 1;
                  resolveReleaseStarted?.();
                  await releaseGate;
                  releaseSettled = true;
                },
              }),
          },
        ],
        sandbox: directSandbox(() => {
          sandboxCalls += 1;
          return Promise.resolve(null);
        }),
      });
      let acquisitionSettled = false;
      const acquireWithoutLease = async () => {
        const lease = await runtime.acquire();
        await lease.release();
        throw new Error("Expected invalid source snapshot rejection");
      };
      const acquisition = acquireWithoutLease().finally(() => {
        acquisitionSettled = true;
      });

      try {
        const firstEvent = await Promise.race([
          releaseStarted.then(() => "release_started" as const),
          acquisition.then(
            () => "acquisition_settled" as const,
            () => "acquisition_settled" as const,
          ),
        ]);
        expect(firstEvent).toBe("release_started");
        expect(acquisitionSettled).toBe(false);
        resolveRelease?.();
        await expect(acquisition).rejects.toMatchObject({
          code: "invalid_definition",
        });
        expect(releaseCalls).toBe(1);
        expect(releaseSettled).toBe(true);
        expect(sandboxCalls).toBe(0);
      } finally {
        resolveRelease?.();
        try {
          await expect(acquisition).rejects.toMatchObject({
            code: "invalid_definition",
          });
        } finally {
          await runtime.close();
        }
      }
    },
  );

  it("rolls back a partial source acquisition before a later complete install", async () => {
    const events: string[] = [];
    let sourceAAcquires = 0;
    let sourceBAcquires = 0;
    let sourceAReleases = 0;
    let sourceBShouldFail = true;
    const moduleFor = (id: string, name: string) =>
      Mycelium.module({
        id,
        name,
        operations: {
          ping: Mycelium.operation({
            input: z.object({ value: z.string() }),
            output: z.object({ value: z.string() }),
            execute: ({ value }) => Promise.resolve({ value }),
          }),
        },
      });
    const sourceA = {
      id: "source-a",
      acquire: () => {
        sourceAAcquires += 1;
        events.push("A-acquire");
        return Promise.resolve({
          snapshot: {
            version: `a-${String(sourceAAcquires)}`,
            modules: [moduleFor("module-a", "a")],
          },
          release: () => {
            sourceAReleases += 1;
            events.push("A-release");
            return Promise.resolve();
          },
        });
      },
    };
    const sourceB = {
      id: "source-b",
      acquire: () => {
        sourceBAcquires += 1;
        events.push("B-acquire");
        if (sourceBShouldFail) {
          return Promise.reject(new Error("source B unavailable"));
        }
        return Promise.resolve({
          snapshot: {
            version: `b-${String(sourceBAcquires)}`,
            modules: [moduleFor("module-b", "b")],
          },
          release: () => {
            events.push("B-release");
            return Promise.resolve();
          },
        });
      },
    };
    const runtime = await Mycelium.make({
      sources: [sourceB, sourceA],
      sandbox,
    });

    await expect(runtime.acquire()).rejects.toMatchObject({
      code: "source_failure",
    });
    expect(events).toEqual(["A-acquire", "B-acquire", "A-release"]);
    expect(sourceAReleases).toBe(1);

    sourceBShouldFail = false;
    const lease = await runtime.acquire();
    try {
      expect(sourceAAcquires).toBe(2);
      expect(sourceBAcquires).toBe(2);
      expect(lease.catalog.describe({ path: "a.ping" })).toMatchObject({
        path: "a.ping",
      });
      expect(lease.catalog.describe({ path: "b.ping" })).toMatchObject({
        path: "b.ping",
      });
    } finally {
      await lease.release();
      await runtime.close();
    }
    expect(sourceAReleases).toBe(2);
  });

  it("rebuilds after invalidation without changing held leases", async () => {
    let sourceAcquires = 0;
    const releases: string[] = [];
    let invalidate: (() => void) | undefined;
    const source = {
      id: "versioned-source",
      acquire: (context: { invalidate: () => void }) => {
        sourceAcquires += 1;
        invalidate = context.invalidate;
        const version = `v${String(sourceAcquires)}`;
        const module = Mycelium.module({
          id: "versioned-module",
          name: "demo",
          operations: {
            ping: Mycelium.operation({
              input: z.object({ value: z.string() }),
              output: z.object({ value: z.string() }),
              execute: () => Promise.resolve({ value: version }),
            }),
          },
        });
        return Promise.resolve({
          snapshot: { version, modules: [module] },
          release: () => {
            releases.push(version);
            return Promise.resolve();
          },
        });
      },
    };
    const runtime = await Mycelium.make({
      sources: [source],
      sandbox: directSandbox(async (bindings) => {
        const ping = bindings.demo?.ping;
        if (ping === undefined) {
          throw new Error("Expected demo.ping sandbox binding");
        }
        return ping({ value: "ignored" });
      }),
    });
    const lease1 = await runtime.acquire();

    const run = async (lease: typeof lease1, callId: string) => {
      const prepared = await preparePromiseTool(
        lease.executeTool,
        callId,
        'return demo.ping({ value: "ignored" })',
      );
      return executePromiseTool(prepared);
    };

    try {
      expect(await run(lease1, "version-1")).toEqual([
        {
          type: "result",
          result: {
            content: [{ type: "text", text: '{"value":"v1"}' }],
            isError: false,
          },
        },
      ]);
      expect(invalidate).toBeTypeOf("function");
      if (invalidate === undefined)
        throw new Error("Expected invalidation callback");
      invalidate();

      const lease2 = await runtime.acquire();
      try {
        expect(sourceAcquires).toBe(2);
        expect(lease1.revision).not.toBe(lease2.revision);
        expect(lease1.catalog.stamp()).toBe(lease2.catalog.stamp());
        expect(await run(lease2, "version-2")).toEqual([
          {
            type: "result",
            result: {
              content: [{ type: "text", text: '{"value":"v2"}' }],
              isError: false,
            },
          },
        ]);
        expect(await run(lease1, "held-version-1")).toEqual([
          {
            type: "result",
            result: {
              content: [{ type: "text", text: '{"value":"v1"}' }],
              isError: false,
            },
          },
        ]);

        await lease1.release();
        expect(releases).toEqual(["v1"]);
      } finally {
        await lease2.release();
      }
    } finally {
      await lease1.release();
      await runtime.close();
    }
    expect(releases).toEqual(["v1", "v2"]);
  });

  it("rebuilds once after eviction and reuses the warm source revision", async () => {
    let sourceAcquires = 0;
    let sourceReleases = 0;
    const source = {
      id: "cold-rebuild-source",
      acquire: () => {
        sourceAcquires += 1;
        return Promise.resolve({
          snapshot: {
            version: "v1",
            modules: [
              Mycelium.module({
                id: "cold-rebuild-module",
                name: "cold",
                operations: {
                  ping: Mycelium.operation({
                    input: z.object({}),
                    output: z.object({ value: z.literal("pong") }),
                    execute: () => Promise.resolve({ value: "pong" as const }),
                  }),
                },
              }),
            ],
          },
          release: () => {
            sourceReleases += 1;
            return Promise.resolve();
          },
        });
      },
    };
    const openAndCapture = async () => {
      const runtime = await Mycelium.make({ sources: [source], sandbox });
      const first = await runtime.acquire();
      const second = await runtime.acquire();
      expect(sourceAcquires).toBe(sourceReleases + 1);
      expect(second.revision).toBe(first.revision);
      expect(second.catalog.stamp()).toBe(first.catalog.stamp());
      await first.release();
      await second.release();
      await runtime.close();
      return { revision: first.revision, stamp: first.catalog.stamp() };
    };

    const beforeEviction = await openAndCapture();
    expect(sourceAcquires).toBe(1);
    expect(sourceReleases).toBe(1);
    const afterEviction = await openAndCapture();

    expect(sourceAcquires).toBe(2);
    expect(sourceReleases).toBe(2);
    expect(afterEviction).toEqual(beforeEviction);
  });

  it("discards an invalidated candidate and installs only the retried source", async () => {
    let sourceAcquires = 0;
    const releases: string[] = [];
    const source = {
      id: "retry-source",
      acquire: ({ invalidate }: { invalidate: () => void }) => {
        sourceAcquires += 1;
        const version = `v${String(sourceAcquires)}`;
        const module = Mycelium.module({
          id: "retry-module",
          name: "retry",
          operations: {
            ping: Mycelium.operation({
              input: z.object({}),
              output: z.object({ value: z.string() }),
              execute: () => Promise.resolve({ value: version }),
            }),
          },
        });
        if (sourceAcquires === 1) invalidate();
        return Promise.resolve({
          snapshot: { version, modules: [module] },
          release: () => {
            releases.push(version);
            return Promise.resolve();
          },
        });
      },
    };
    const runtime = await Mycelium.make({
      sources: [source],
      sandbox: directSandbox(async (bindings) => {
        const ping = bindings.retry?.ping;
        if (ping === undefined) {
          throw new Error("Expected retry.ping sandbox binding");
        }
        return ping({});
      }),
    });
    const lease = await runtime.acquire();

    try {
      expect(sourceAcquires).toBe(2);
      expect(releases).toEqual(["v1"]);
      const prepared = await preparePromiseTool(
        lease.executeTool,
        "retry-1",
        "return retry.ping({})",
      );
      const parts = await executePromiseTool(prepared);
      expect(parts).toEqual([
        {
          type: "result",
          result: {
            content: [{ type: "text", text: '{"value":"v2"}' }],
            isError: false,
          },
        },
      ]);
    } finally {
      await lease.release();
      await runtime.close();
    }
    expect(releases).toEqual(["v1", "v2"]);
  });

  it("rejects after bounded retries when every source candidate is stale", async () => {
    let sourceAcquires = 0;
    let sandboxCalls = 0;
    const releases: string[] = [];
    const runtime = await Mycelium.make({
      sources: [
        {
          id: "always-stale-source",
          acquire: ({ invalidate }: { invalidate: () => void }) => {
            sourceAcquires += 1;
            const version = `v${String(sourceAcquires)}`;
            invalidate();
            return Promise.resolve({
              snapshot: { version, modules: [] },
              release: () => {
                releases.push(version);
                return Promise.resolve();
              },
            });
          },
        },
      ],
      sandbox: directSandbox(() => {
        sandboxCalls += 1;
        return Promise.resolve(null);
      }),
    });

    try {
      const acquireWithoutLease = async () => {
        const lease = await runtime.acquire();
        await lease.release();
        throw new Error("Expected every stale candidate to reject");
      };
      await expect(acquireWithoutLease()).rejects.toMatchObject({
        code: "source_failure",
      });
    } finally {
      await runtime.close();
    }
    expect(sourceAcquires).toBe(2);
    expect(releases).toEqual(["v1", "v2"]);
    expect(sandboxCalls).toBe(0);
  });

  it("aborts acquisition and awaits prior source cleanup", async () => {
    let resolveBStarted: (() => void) | undefined;
    const bStarted = new Promise<void>((resolve) => {
      resolveBStarted = resolve;
    });
    let resolveARelease: (() => void) | undefined;
    const aReleaseGate = new Promise<void>((resolve) => {
      resolveARelease = resolve;
    });
    let signalAReleaseStarted: (() => void) | undefined;
    const aReleaseStarted = new Promise<void>((resolve) => {
      signalAReleaseStarted = resolve;
    });
    let aReleaseCalls = 0;
    const bReleaseCalls = 0;
    let bAbortObserved = false;
    let sandboxCalls = 0;
    const runtime = await Mycelium.make({
      sources: [
        {
          id: "source-b",
          acquire: ({ signal }) => {
            resolveBStarted?.();
            return new Promise<never>((_resolve, reject) => {
              signal.addEventListener(
                "abort",
                () => {
                  bAbortObserved = true;
                  reject(new Error("source B acquisition aborted"));
                },
                { once: true },
              );
            });
          },
        },
        {
          id: "source-a",
          acquire: () =>
            Promise.resolve({
              snapshot: { version: "v1", modules: [] },
              release: async () => {
                aReleaseCalls += 1;
                signalAReleaseStarted?.();
                await aReleaseGate;
              },
            }),
        },
      ],
      sandbox: directSandbox(() => {
        sandboxCalls += 1;
        return Promise.resolve(null);
      }),
    });
    const controller = new AbortController();
    let acquisitionSettled = false;
    const acquisition = runtime
      .acquire({ signal: controller.signal })
      .finally(() => {
        acquisitionSettled = true;
      });

    try {
      await bStarted;
      controller.abort();
      await Promise.resolve();
      expect(bAbortObserved).toBe(true);
      await aReleaseStarted;
      expect(aReleaseCalls).toBe(1);
      expect(bReleaseCalls).toBe(0);
      expect(acquisitionSettled).toBe(false);
      expect(sandboxCalls).toBe(0);
      resolveARelease?.();
      await expect(acquisition).rejects.toMatchObject({ name: "AbortError" });
    } finally {
      resolveARelease?.();
      try {
        await expect(acquisition).rejects.toMatchObject({ name: "AbortError" });
      } finally {
        await runtime.close();
      }
    }
    expect(aReleaseCalls).toBe(1);
    expect(bReleaseCalls).toBe(0);
  });

  it("waits for every live revision before closing after cleanup failure", async () => {
    let sourceAcquires = 0;
    const invalidators: (() => void)[] = [];
    const releaseCalls: string[] = [];
    let resolveV2: (() => void) | undefined;
    const v2Gate = new Promise<void>((resolve) => {
      resolveV2 = resolve;
    });
    let signalV1ReleaseStarted: (() => void) | undefined;
    const v1ReleaseStarted = new Promise<void>((resolve) => {
      signalV1ReleaseStarted = resolve;
    });
    let signalV2ReleaseStarted: (() => void) | undefined;
    const v2ReleaseStarted = new Promise<void>((resolve) => {
      signalV2ReleaseStarted = resolve;
    });
    const source = {
      id: "close-source",
      acquire: ({ invalidate }: { invalidate: () => void }) => {
        sourceAcquires += 1;
        invalidators.push(invalidate);
        const version = `v${String(sourceAcquires)}`;
        return Promise.resolve({
          snapshot: { version, modules: [] },
          release: async () => {
            releaseCalls.push(version);
            if (version === "v1") {
              signalV1ReleaseStarted?.();
              throw new Error("v1 cleanup failed");
            }
            signalV2ReleaseStarted?.();
            await v2Gate;
          },
        });
      },
    };
    const runtime = await Mycelium.make({ sources: [source], sandbox });
    const lease1 = await runtime.acquire();
    let lease2: typeof lease1 | undefined;
    let closeSettled = false;
    let closePromise: Promise<unknown> | undefined;

    try {
      expect(invalidators).toHaveLength(1);
      invalidators[0]?.();
      const currentLease = await runtime.acquire();
      lease2 = currentLease;
      expect(sourceAcquires).toBe(2);
      expect(lease1.revision).not.toBe(currentLease.revision);
      closePromise = runtime.close().finally(() => {
        closeSettled = true;
      });

      await expect(runtime.acquire()).rejects.toMatchObject({
        code: "runtime_closed",
      });

      const lease2Release = lease2.release();
      await v2ReleaseStarted;
      expect(releaseCalls).toContain("v2");
      expect(closeSettled).toBe(false);

      const lease1Release = lease1.release();
      await v1ReleaseStarted;
      expect(releaseCalls).toContain("v1");
      await expect(lease1Release).rejects.toBeDefined();
      expect(closeSettled).toBe(false);
      resolveV2?.();
      await lease2Release;
      await expect(closePromise).rejects.toBeDefined();
      expect(closeSettled).toBe(true);
      expect(releaseCalls.sort()).toEqual(["v1", "v2"]);
      const cleanupFailure = await closePromise.then(
        () => undefined,
        (cause: unknown) => cause,
      );
      await expect(lease1.release()).rejects.toBe(cleanupFailure);
      await lease2.release();
      await expect(runtime.close()).rejects.toBe(cleanupFailure);
      expect(releaseCalls.sort()).toEqual(["v1", "v2"]);
    } finally {
      resolveV2?.();
      await Promise.all([
        lease2?.release(),
        expect(lease1.release()).rejects.toMatchObject({
          code: "source_failure",
          phase: "release",
        }),
        closePromise === undefined
          ? undefined
          : expect(closePromise).rejects.toMatchObject({
              code: "source_failure",
              phase: "release",
            }),
      ]);
    }
  });

  it("keeps Effect cleanup honest when a release fiber is interrupted", async () => {
    let resolveReleaseStarted: (() => void) | undefined;
    const releaseStarted = new Promise<void>((resolve) => {
      resolveReleaseStarted = resolve;
    });
    let resolveRelease: (() => void) | undefined;
    const releaseGate = new Promise<void>((resolve) => {
      resolveRelease = resolve;
    });
    let resolveReleaseCompleted: (() => void) | undefined;
    const releaseCompleted = new Promise<void>((resolve) => {
      resolveReleaseCompleted = resolve;
    });
    let releaseCalls = 0;
    const source = {
      id: "effect-close-source",
      acquire: () =>
        Promise.resolve({
          snapshot: { version: "v1", modules: [] },
          release: async () => {
            releaseCalls += 1;
            resolveReleaseStarted?.();
            try {
              await releaseGate;
            } finally {
              resolveReleaseCompleted?.();
            }
          },
        }),
    };
    const program = Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* makeEffect({
          sources: [source],
          sandbox: { execute: () => Effect.succeed(null) },
        });
        const lease = yield* runtime.acquire();
        const closeFiber = yield* Effect.forkChild(runtime.close());
        yield* Effect.yieldNow;
        const releaseFiber = yield* Effect.forkChild(lease.release());
        const ordering = yield* Effect.raceFirst(
          Effect.promise(() => releaseStarted).pipe(Effect.as("started")),
          Fiber.await(releaseFiber).pipe(Effect.as("finished")),
        );
        expect(ordering).toBe("started");
        yield* Effect.sync(() => {
          releaseFiber.interruptUnsafe();
        });
        resolveRelease?.();
        yield* Effect.promise(() => releaseCompleted);
        const releaseExit = yield* Fiber.await(releaseFiber);
        const closeObserved = yield* Effect.raceFirst(
          Fiber.await(closeFiber).pipe(Effect.as(true)),
          Effect.gen(function* () {
            for (let turn = 0; turn < 8; turn += 1) {
              yield* Effect.yieldNow;
            }
            return false;
          }),
        );
        if (!closeObserved) {
          yield* Fiber.interrupt(closeFiber);
          expect(closeObserved).toBe(true);
        }
        const closeExit = yield* Fiber.await(closeFiber);
        return { closeExit, closeObserved, releaseExit };
      }),
    );
    const result = await Effect.runPromise(program);

    expect(releaseCalls).toBe(1);
    expect(Exit.isFailure(result.releaseExit)).toBe(true);
    expect(Exit.isSuccess(result.closeExit)).toBe(true);
    expect(result.closeObserved).toBe(true);
  });

  it("retains a source cleanup failure as an identifiable defect during automatic Effect scope finalization", async () => {
    let releaseCalls = 0;
    const exit = await Effect.runPromise(
      Effect.exit(
        Effect.scoped(
          Effect.gen(function* () {
            const runtime = yield* makeEffect({
              sources: [
                {
                  id: "effect-scope-release-failure",
                  acquire: () =>
                    Promise.resolve({
                      snapshot: { version: "v1", modules: [] },
                      release: () => {
                        releaseCalls += 1;
                        return Promise.reject(
                          new Error("Source release failed"),
                        );
                      },
                    }),
                },
              ],
              sandbox: { execute: () => Effect.succeed(null) },
            });
            yield* runtime.acquire();
          }),
        ),
      ),
    );

    const typedFailure = Exit.isFailure(exit)
      ? Cause.findErrorOption(exit.cause)
      : Option.none();
    const defect = Exit.isFailure(exit)
      ? Result.match(Cause.findDefect(exit.cause), {
          onSuccess: Option.some,
          onFailure: () => Option.none(),
        })
      : Option.none();
    expect(releaseCalls).toBe(1);
    expect(Exit.isFailure(exit)).toBe(true);
    expect(Option.isNone(typedFailure)).toBe(true);
    expect(Option.isSome(defect)).toBe(true);
    if (Option.isSome(defect)) {
      expect(defect.value).toMatchObject({
        code: "source_failure",
        phase: "release",
        sourceId: "effect-scope-release-failure",
      });
    }
  });

  it("awaits Effect sandbox cleanup before interrupted execution settles", async () => {
    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const sandboxStarted = yield* Deferred.make<undefined>();
          const cleanupStarted = yield* Deferred.make<undefined>();
          const cleanupGate = yield* Deferred.make<undefined>();
          const runtime = yield* makeEffect({
            modules: [],
            execution: { timeoutMs: 60_000, abortGraceMs: 30_000 },
            sandbox: {
              execute: () =>
                Deferred.succeed(sandboxStarted, undefined).pipe(
                  Effect.andThen(Effect.never),
                  Effect.ensuring(
                    Deferred.succeed(cleanupStarted, undefined).pipe(
                      Effect.andThen(Deferred.await(cleanupGate)),
                    ),
                  ),
                ),
            },
          });
          const lease = yield* runtime.acquire();
          const prepared = yield* prepareTool(
            lease.executeTool,
            "effect-execution-cancel",
            "return null",
          );

          const executionFiber = yield* Effect.forkChild(
            Stream.runCollect(prepared.execute()),
          );
          yield* Deferred.await(sandboxStarted);
          const interruptionFiber = yield* Effect.forkChild(
            Fiber.interrupt(executionFiber),
          );
          const firstSettlement = yield* Effect.raceFirst(
            Deferred.await(cleanupStarted).pipe(
              Effect.as("cleanup_started" as const),
            ),
            Fiber.await(interruptionFiber).pipe(
              Effect.as("interruption_settled" as const),
            ),
          );
          const beforeAcknowledgement = yield* Effect.sync(() =>
            Option.fromUndefinedOr(interruptionFiber.pollUnsafe()),
          );
          yield* Deferred.succeed(cleanupGate, undefined);
          const interruptionExit = yield* Fiber.await(interruptionFiber);

          return {
            firstSettlement,
            interruptionExit,
            settledBeforeAcknowledgement: Option.isSome(beforeAcknowledgement),
          };
        }),
      ),
    );

    expect(result.firstSettlement).toBe("cleanup_started");
    expect(result.settledBeforeAcknowledgement).toBe(false);
    expect(Exit.isSuccess(result.interruptionExit)).toBe(true);
  });

  it("interrupts an Effect sandbox at its deadline and awaits cleanup", async () => {
    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const sandboxStarted = yield* Deferred.make<undefined>();
          const cleanupStarted = yield* Deferred.make<undefined>();
          const cleanupGate = yield* Deferred.make<undefined>();
          const runtime = yield* makeEffect({
            modules: [],
            execution: { timeoutMs: 100, abortGraceMs: 50 },
            sandbox: {
              execute: () =>
                Deferred.succeed(sandboxStarted, undefined).pipe(
                  Effect.andThen(Effect.never),
                  Effect.ensuring(
                    Deferred.succeed(cleanupStarted, undefined).pipe(
                      Effect.andThen(Deferred.await(cleanupGate)),
                    ),
                  ),
                ),
            },
          });
          const lease = yield* runtime.acquire();
          const prepared = yield* prepareTool(
            lease.executeTool,
            "effect-execution-deadline",
            "return never()",
          );
          const executionFiber = yield* Effect.forkChild(
            Effect.exit(Stream.runCollect(prepared.execute())),
          );

          yield* Deferred.await(sandboxStarted);
          yield* TestClock.adjust("100 millis");
          yield* Deferred.await(cleanupStarted);
          const cleanupObservedAtDeadline = Option.isSome(
            yield* Deferred.poll(cleanupStarted),
          );
          const settledBeforeAcknowledgement = Option.isSome(
            yield* Effect.sync(() =>
              Option.fromUndefinedOr(executionFiber.pollUnsafe()),
            ),
          );
          yield* Deferred.succeed(cleanupGate, undefined);
          const interruptionExit = yield* Fiber.join(executionFiber);

          return {
            cleanupObservedAtDeadline,
            interruptionExit,
            settledBeforeAcknowledgement,
          };
        }),
      ).pipe(Effect.provide(TestClock.layer())),
    );

    const failure = Exit.isFailure(result.interruptionExit)
      ? Cause.findErrorOption(result.interruptionExit.cause)
      : Option.none();

    expect(result.cleanupObservedAtDeadline).toBe(true);
    expect(result.settledBeforeAcknowledgement).toBe(false);
    expect(Option.isSome(failure)).toBe(true);
    if (Option.isSome(failure)) {
      expect(failure.value).toMatchObject({
        _tag: "MuleError",
        phase: "tool",
      });
      expect(failure.value).toHaveProperty(
        "message",
        expect.stringMatching(
          /^execute hit its 100 ms deadline after 100 ms; split the work or persist progress \(ref /u,
        ),
      );
    }
  });

  it("reports the deadline cause, not a generic one, when the dying sandbox fails on the same abort", async () => {
    // A real sandbox adapter can reject with a generic, non-typed failure the
    // instant its invocation deadline aborts it (e.g. an in-flight dispatch
    // torn down mid-teardown). That failure racing the deadline's own report
    // must not win: the agent needs "the deadline fired" rather than
    // whatever side effect of the abort reached it first.
    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const runtime = yield* makeEffect({
            modules: [],
            execution: { timeoutMs: 100, abortGraceMs: 50 },
            sandbox: {
              execute: (_request, signal: AbortSignal) =>
                Effect.callback<never, SandboxFailure>((resume) => {
                  const onAbort = () => {
                    resume(
                      Effect.fail(
                        new Unknown({
                          rawCause: new Error("adapter torn down"),
                        }),
                      ),
                    );
                  };
                  if (signal.aborted) onAbort();
                  else
                    signal.addEventListener("abort", onAbort, { once: true });
                  return Effect.sync(() => {
                    signal.removeEventListener("abort", onAbort);
                  });
                }),
            },
          });
          const lease = yield* runtime.acquire();
          const prepared = yield* prepareTool(
            lease.executeTool,
            "race-deadline-vs-generic-failure",
            "return never()",
          );
          const executionFiber = yield* Effect.forkChild(
            Effect.exit(Stream.runCollect(prepared.execute())),
          );
          yield* TestClock.adjust("100 millis");
          return yield* Fiber.join(executionFiber);
        }),
      ).pipe(Effect.provide(TestClock.layer())),
    );

    expect(Exit.isFailure(result)).toBe(true);
    const failure = Exit.isFailure(result)
      ? Cause.findErrorOption(result.cause)
      : Option.none();
    expect(Option.isSome(failure)).toBe(true);
    if (Option.isSome(failure)) {
      expect(failure.value).toMatchObject({
        _tag: "MuleError",
        phase: "tool",
      });
      expect(failure.value).toHaveProperty(
        "message",
        expect.stringMatching(
          /^execute hit its 100 ms deadline after 100 ms; split the work or persist progress \(ref /u,
        ),
      );
    }
  });
});

it("hands the runtime's Session to every source acquisition, and none to session-free hosts", async () => {
  const seen: (string | undefined)[] = [];
  const source = {
    id: "session-probe",
    acquire: (context: { session?: { sessionId: string } }) => {
      seen.push(context.session?.sessionId);
      return Promise.resolve({
        snapshot: { version: "1", modules: [] },
        release: () => Promise.resolve(),
      });
    },
  };
  for (const options of [
    { sources: [source], sandbox, session: { sessionId: "session_one" } },
    { sources: [source], sandbox },
  ]) {
    const runtime = await Mycelium.make(options);
    const lease = await runtime.acquire();
    await lease.release();
    await runtime.close();
  }
  expect(seen).toEqual(["session_one", undefined]);
});

it("maps every bound alias to its module id, including the bare-alias collision winner", async () => {
  const staticModule = Mycelium.module({
    id: "zzz-local",
    name: "Shared",
    operations: {
      ping: Mycelium.operation({
        input: z.object({}),
        execute: () => Promise.resolve(null),
      }),
    },
  });
  const attachedModule = Mycelium.module({
    id: "aaa-attach",
    name: "Shared",
    operations: {
      status: Mycelium.operation({
        input: z.object({}),
        execute: () => Promise.resolve(null),
      }),
    },
  });
  let observedModules: Readonly<Record<string, string>> | undefined;
  const runtime = await Mycelium.make({
    modules: [staticModule],
    sources: [
      {
        id: "attach-one",
        acquire: () =>
          Promise.resolve({
            snapshot: { version: "v1", modules: [attachedModule] },
            release: () => Promise.resolve(),
          }),
      },
    ],
    sandbox: {
      execute: (request) => {
        observedModules = request.modules;
        return Promise.resolve(null);
      },
    },
  });
  const lease = await runtime.acquire();
  try {
    // "aaa-attach" sorts before "zzz-local", so the attachment's module takes
    // the bare "Shared" alias and the static module is pushed to "Shared_2".
    const prepared = await preparePromiseTool(
      lease.executeTool,
      "modules-map-1",
      "return null",
    );
    await executePromiseTool(prepared);
    expect(observedModules).toEqual({
      Shared: "aaa-attach",
      Shared_2: "zzz-local",
    });
  } finally {
    await lease.release();
    await runtime.close();
  }
});

it("sets sessionId for session runs and leaves it null otherwise", async () => {
  const demoModule = Mycelium.module({
    id: "demo",
    operations: {
      ping: Mycelium.operation({
        input: z.object({}),
        execute: () => Promise.resolve(null),
      }),
    },
  });
  const observedSessionIds: (string | null)[] = [];
  const createSandbox = (): Sandbox => ({
    execute: (request) => {
      observedSessionIds.push(request.sessionId ?? null);
      return Promise.resolve(null);
    },
  });
  for (const options of [
    {
      modules: [demoModule],
      sandbox: createSandbox(),
      session: { sessionId: "session_two" },
    },
    { modules: [demoModule], sandbox: createSandbox() },
  ]) {
    const runtime = await Mycelium.make(options);
    const lease = await runtime.acquire();
    try {
      const prepared = await preparePromiseTool(
        lease.executeTool,
        "session-id-1",
        "return null",
      );
      await executePromiseTool(prepared);
    } finally {
      await lease.release();
      await runtime.close();
    }
  }
  expect(observedSessionIds).toEqual(["session_two", null]);
});
