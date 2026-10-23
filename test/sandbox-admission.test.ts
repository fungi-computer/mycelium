import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { expect, it } from "vitest";
import { make, type Lease } from "../src/effect.js";
import { MyceliumClosedError, type SandboxFailureEvent } from "../src/index.js";

it("shares permits across revisions and retains queued source work through close without holding the lifecycle gate", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const started = yield* Deferred.make<undefined>();
        const release = yield* Deferred.make<undefined>();
        const dispatched: string[] = [];
        const released: string[] = [];
        let acquisitions = 0;
        let invalidate: (() => void) | undefined;
        const runtime = yield* make({
          execution: {
            sandboxCapacity: 1,
            admissionWaitMs: 50,
            timeoutMs: 500,
            abortGraceMs: 10,
          },
          sources: [
            {
              id: "admission-revisions",
              acquire: (context: { invalidate: () => void }) => {
                acquisitions += 1;
                const version = `v${String(acquisitions)}`;
                invalidate = context.invalidate;
                return Promise.resolve({
                  snapshot: { modules: [], version },
                  release: () => {
                    released.push(version);
                    return Promise.resolve();
                  },
                });
              },
            },
          ],
          sandbox: {
            execute: (request) =>
              Effect.gen(function* () {
                dispatched.push(request.code);
                if (request.code === "return 'held'") {
                  yield* Deferred.succeed(started, undefined);
                  yield* Deferred.await(release);
                }
                return null;
              }),
          },
        });
        const run = (lease: Lease, code: string) =>
          Effect.gen(function* () {
            const prepared = yield* lease.executeTool.prepare({
              type: "toolCall",
              id: code,
              name: "execute",
              arguments: { code },
            });
            return yield* Stream.runCollect(prepared.execute());
          });
        const first = yield* runtime.acquire();
        const held = yield* Effect.forkChild(run(first, "return 'held'"));
        try {
          yield* Deferred.await(started);
          if (invalidate === undefined) {
            throw new Error("Expected acquired source invalidation authority");
          }
          invalidate();
          const second = yield* runtime.acquire();
          expect(second.revision).not.toBe(first.revision);
          const waiting = yield* Effect.forkChild(
            run(second, "return 'queued'"),
          );
          yield* TestClock.adjust("1 millis");
          expect(dispatched).toEqual(["return 'held'"]);

          const closing = yield* Effect.forkChild(runtime.close());
          yield* TestClock.adjust("1 millis");
          const releasing = yield* Effect.forkChild(second.release());
          yield* TestClock.adjust("1 millis");
          expect(released).toEqual([]);

          const refused = yield* Deferred.make<undefined>();
          const afterClose = yield* Effect.forkChild(
            Effect.flip(runtime.acquire()).pipe(
              Effect.tap(() => Deferred.succeed(refused, undefined)),
            ),
          );
          yield* TestClock.adjust("1 millis");
          expect(Option.isSome(yield* Deferred.poll(refused))).toBe(true);
          const closed = yield* Fiber.join(afterClose);
          expect(closed).toBeInstanceOf(MyceliumClosedError);
          expect(closed).toHaveProperty("code", "runtime_closed");
          expect(dispatched).toEqual(["return 'held'"]);
          expect(released).toEqual([]);

          yield* Deferred.succeed(release, undefined);
          yield* Fiber.join(held);
          yield* Fiber.join(waiting);
          yield* Fiber.join(releasing);
          expect(dispatched).toEqual(["return 'held'", "return 'queued'"]);
          expect(released).toEqual(["v2"]);
          yield* first.release();
          yield* Fiber.join(closing);
          yield* runtime.close();
          expect(released).toEqual(["v2", "v1"]);
        } finally {
          yield* Deferred.succeed(release, undefined);
        }
      }),
    ).pipe(Effect.provide(TestClock.layer())),
  );
});

it("removes a cancelled queued invocation without dispatching it or consuming the next permit", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const started = yield* Deferred.make<undefined>();
        const release = yield* Deferred.make<undefined>();
        const dispatched: string[] = [];
        const failures: SandboxFailureEvent[] = [];
        const runtime = yield* make({
          execution: {
            sandboxCapacity: 1,
            admissionWaitMs: 50,
            timeoutMs: 500,
            abortGraceMs: 10,
          },
          onSandboxFailure: (event) =>
            Effect.sync(() => {
              failures.push(event);
            }),
          sandbox: {
            execute: (request) =>
              Effect.gen(function* () {
                dispatched.push(request.code);
                if (request.code === "return 'held'") {
                  yield* Deferred.succeed(started, undefined);
                  yield* Deferred.await(release);
                }
                return null;
              }),
          },
        });
        const lease = yield* runtime.acquire();
        const run = (code: string) =>
          Effect.gen(function* () {
            const prepared = yield* lease.executeTool.prepare({
              type: "toolCall",
              id: code,
              name: "execute",
              arguments: { code },
            });
            return yield* Stream.runCollect(prepared.execute());
          });
        const held = yield* Effect.forkChild(run("return 'held'"));
        try {
          yield* Deferred.await(started);
          const waiting = yield* Effect.forkChild(run("return 'cancelled'"));
          yield* TestClock.adjust("1 millis");
          expect(dispatched).toEqual(["return 'held'"]);
          yield* Fiber.interrupt(waiting);
          expect(failures).toHaveLength(1);
          expect(failures[0]).toMatchObject({
            tag: "Cancelled",
            healthAlert: false,
          });
        } finally {
          yield* Deferred.succeed(release, undefined);
        }
        yield* Fiber.join(held);
        yield* run("return 'next'");
        expect(dispatched).toEqual(["return 'held'", "return 'next'"]);
        expect(failures).toHaveLength(1);
      }),
    ).pipe(Effect.provide(TestClock.layer())),
  );
});

it("keeps the original deadline while queued and prefers it to simultaneous wait expiry", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const started = yield* Deferred.make<undefined>();
        const release = yield* Deferred.make<undefined>();
        const dispatched: string[] = [];
        const failures: SandboxFailureEvent[] = [];
        const runtime = yield* make({
          execution: {
            sandboxCapacity: 1,
            admissionWaitMs: 50,
            timeoutMs: 50,
            abortGraceMs: 10,
          },
          onSandboxFailure: (event) =>
            Effect.sync(() => {
              failures.push(event);
            }),
          sandbox: {
            execute: (request) =>
              Effect.gen(function* () {
                dispatched.push(request.code);
                yield* Deferred.succeed(started, undefined);
                yield* Deferred.await(release);
                return null;
              }),
          },
        });
        const lease = yield* runtime.acquire();
        const run = (code: string) =>
          Effect.gen(function* () {
            const prepared = yield* lease.executeTool.prepare({
              type: "toolCall",
              id: code,
              name: "execute",
              arguments: { code },
            });
            return yield* Stream.runCollect(prepared.execute());
          });
        const held = yield* Effect.forkChild(Effect.exit(run("return 'held'")));
        try {
          yield* Deferred.await(started);
          const waiting = yield* Effect.forkChild(
            Effect.flip(run("return 'queued'")),
          );
          yield* TestClock.adjust("50 millis");
          const expired = yield* Fiber.join(waiting);
          expect(expired).toMatchObject({
            _tag: "MuleError",
            phase: "tool",
          });
          expect(expired).toHaveProperty(
            "message",
            expect.stringContaining(
              "execute hit its 50 ms deadline after 50 ms;",
            ),
          );
          expect(dispatched).toEqual(["return 'held'"]);
        } finally {
          yield* Deferred.succeed(release, undefined);
        }
        yield* Fiber.join(held);
        expect(dispatched).toEqual(["return 'held'"]);
        expect(failures).toHaveLength(2);
        for (const failure of failures) {
          expect(failure).toMatchObject({
            tag: "DeadlineExceeded",
            fields: { limitMs: 50, elapsedMs: 50 },
            healthAlert: false,
          });
        }
      }),
    ).pipe(Effect.provide(TestClock.layer())),
  );
});

it("admits the configured host capacity before any execution settles", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const release = yield* Deferred.make<undefined>();
        const allStarted = yield* Deferred.make<undefined>();
        let started = 0;
        const runtime = yield* make({
          execution: { sandboxCapacity: 12, admissionWaitMs: 50 },
          sandbox: {
            execute: () =>
              Effect.gen(function* () {
                started += 1;
                if (started === 12) {
                  yield* Deferred.succeed(allStarted, undefined);
                }
                yield* Deferred.await(release);
                return null;
              }),
          },
        });
        const lease = yield* runtime.acquire();
        const executions = yield* Effect.forEach(
          Array.from({ length: 12 }, (_, index) => index),
          (index) =>
            Effect.gen(function* () {
              const prepared = yield* lease.executeTool.prepare({
                type: "toolCall",
                id: `capacity-${String(index)}`,
                name: "execute",
                arguments: { code: "return null" },
              });
              return yield* Effect.forkChild(
                Stream.runCollect(prepared.execute()),
              );
            }),
        );
        try {
          const observation = yield* Effect.forkChild(
            Effect.raceFirst(
              Deferred.await(allStarted).pipe(Effect.as("all_started")),
              Effect.sleep("10 millis").pipe(Effect.as("still_waiting")),
            ),
          );
          yield* TestClock.adjust("10 millis");
          expect(yield* Fiber.join(observation)).toBe("all_started");
          expect(started).toBe(12);
        } finally {
          yield* Deferred.succeed(release, undefined);
        }
        yield* Effect.forEach(executions, Fiber.join);
      }),
    ).pipe(Effect.provide(TestClock.layer())),
  );
});

it("returns typed sandbox-busy at the wait bound without dispatching or replaying the waiter", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const started = yield* Deferred.make<undefined>();
        const release = yield* Deferred.make<undefined>();
        const dispatched: string[] = [];
        const failures: SandboxFailureEvent[] = [];
        const runtime = yield* make({
          execution: {
            sandboxCapacity: 1,
            admissionWaitMs: 50,
            timeoutMs: 500,
            abortGraceMs: 10,
          },
          onSandboxFailure: (event) =>
            Effect.sync(() => {
              failures.push(event);
            }),
          sandbox: {
            execute: (request) =>
              Effect.gen(function* () {
                dispatched.push(request.code);
                if (request.code === "return 'held'") {
                  yield* Deferred.succeed(started, undefined);
                  yield* Deferred.await(release);
                }
                return null;
              }),
          },
        });
        const lease = yield* runtime.acquire();
        const run = (id: string, code: string) =>
          Effect.gen(function* () {
            const prepared = yield* lease.executeTool.prepare({
              type: "toolCall",
              id,
              name: "execute",
              arguments: { code },
            });
            return yield* Stream.runCollect(prepared.execute());
          });
        const held = yield* Effect.forkChild(run("held", "return 'held'"));
        try {
          yield* Deferred.await(started);
          const refused = yield* Deferred.make<undefined>();
          const waiting = yield* Effect.forkChild(
            Effect.flip(run("busy", "return 'busy'")).pipe(
              Effect.tap(() => Deferred.succeed(refused, undefined)),
            ),
          );
          yield* TestClock.adjust("50 millis");
          expect(Option.isSome(yield* Deferred.poll(refused))).toBe(true);
          expect(yield* Fiber.join(waiting)).toMatchObject({
            _tag: "MuleError",
            phase: "tool",
            cause: {
              _tag: "SandboxBusy",
              code: "sandbox-busy",
              capacity: 1,
              waitedMs: 50,
            },
          });
          expect(failures).toHaveLength(1);
          expect(failures[0]).toMatchObject({
            tag: "SandboxBusy",
            fields: { code: "sandbox-busy", capacity: 1, waitedMs: 50 },
            healthAlert: false,
          });
          expect(dispatched).toEqual(["return 'held'"]);
        } finally {
          yield* Deferred.succeed(release, undefined);
        }
        yield* Fiber.join(held);
        yield* run("after-busy", "return 'next'");
        expect(dispatched).toEqual(["return 'held'", "return 'next'"]);
      }),
    ).pipe(Effect.provide(TestClock.layer())),
  );
});
