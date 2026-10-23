import { execFileSync } from "node:child_process";
import { log } from "node:console";
import { availableParallelism } from "node:os";
import process from "node:process";
import { make } from "@fungi.computer/mycelium/effect";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { z } from "zod";

const policy = z
  .strictObject({
    capacity: z.int().positive().optional(),
    attempts: z.int().positive(),
    changedAffinity: z.array(z.int().nonnegative()).nonempty(),
  })
  .parse(JSON.parse(process.argv[2]));

const availableBefore = availableParallelism();
const result = await Effect.runPromise(
  Effect.scoped(
    Effect.gen(function* () {
      const release = yield* Deferred.make();
      let started = 0;
      const runtime = yield* make({
        execution: {
          sandboxCapacity: policy.capacity,
          admissionWaitMs: 50,
        },
        sandbox: {
          execute: () =>
            Effect.gen(function* () {
              started += 1;
              yield* Deferred.await(release);
              return null;
            }),
        },
      });
      // This process is disposable. The parent and every other test retain
      // their affinity; an existing runtime must retain its opening policy.
      execFileSync("taskset", [
        "--pid",
        "--cpu-list",
        policy.changedAffinity.join(","),
        String(process.pid),
      ]);
      const availableAfter = availableParallelism();
      const lease = yield* runtime.acquire();
      const executions = yield* Effect.forEach(
        Array.from({ length: policy.attempts }, (_, index) => index),
        (index) =>
          Effect.gen(function* () {
            const prepared = yield* lease.executeTool.prepare({
              type: "toolCall",
              id: `affinity-${String(index)}`,
              name: "execute",
              arguments: { code: "return null" },
            });
            return yield* Effect.forkChild(
              Stream.runCollect(prepared.execute()),
            );
          }),
      );
      let startedBeforeRelease;
      try {
        yield* TestClock.adjust("10 millis");
        startedBeforeRelease = started;
      } finally {
        yield* Deferred.succeed(release, undefined);
      }
      yield* Effect.forEach(executions, Fiber.join);
      return {
        availableBefore,
        availableAfter,
        startedBeforeRelease,
        startedAfterRelease: started,
      };
    }),
  ).pipe(Effect.provide(TestClock.layer())),
);
log(JSON.stringify(result));
