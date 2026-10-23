import type { ToolCall } from "@fungi.computer/mule/effect";
import * as Effect from "effect/Effect";
import * as Logger from "effect/Logger";
import * as Stream from "effect/Stream";
import { expect, it } from "vitest";

import type { SandboxFailureEvent } from "../src/sandbox-failure-event.js";
import { CapabilityFailed, SandboxBusy } from "../src/sandbox-failure.js";
import { fromTypedSandbox, make } from "../src/effect.js";
import {
  Mycelium,
  MAX_EXECUTION_IMAGES,
  type SandboxValue,
} from "../src/index.js";
import * as Result from "effect/Result";
import { z } from "zod";

// A foreign adapter may reject any value; exercise that public boundary without
// requiring host implementations to reject primitives in their typed contract.
async function foreignRejection(cause: unknown): Promise<never> {
  await Promise.resolve();
  throw cause;
}

for (const adapter of ["promise", "effect"] as const) {
  it(`${adapter}: preserves typed sandbox-busy through the real execute tool`, async () => {
    const events: SandboxFailureEvent[] = [];
    const busy = new SandboxBusy({ capacity: 12, waitedMs: 1_000 });
    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const runtime = yield* make({
            sandbox:
              adapter === "promise"
                ? fromTypedSandbox({ execute: () => Promise.reject(busy) })
                : { execute: () => Effect.fail(busy) },
            onSandboxFailure: (event) =>
              Effect.sync(() => {
                events.push(event);
              }),
          });
          const lease = yield* runtime.acquire();
          const prepared = yield* lease.executeTool.prepare({
            type: "toolCall",
            id: "busy",
            name: "execute",
            arguments: { code: "return 1" },
          });
          return yield* Effect.flip(Stream.runCollect(prepared.execute()));
        }),
      ),
    );
    expect(result).toMatchObject({
      _tag: "MuleError",
      phase: "tool",
      cause: {
        _tag: "SandboxBusy",
        code: "sandbox-busy",
        capacity: 12,
        waitedMs: 1_000,
      },
    });
    expect(result.message).toContain("sandbox-busy");
    expect(result.message).toContain("Nothing ran");
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({
      tag: "SandboxBusy",
      fields: { code: "sandbox-busy", capacity: 12, waitedMs: 1_000 },
      healthAlert: false,
    });
  });
}

for (const adapter of ["promise", "effect"] as const) {
  it.each([
    {
      name: "TypeError",
      cause: new TypeError("private transport marker", {
        cause: new Error("nested marker"),
      }),
    },
    { name: "string", cause: "private transport marker" },
    {
      name: "AbortError",
      cause: new DOMException("private transport marker", "AbortError"),
    },
    {
      name: "fetch failure",
      cause: new Error("private transport marker", {
        cause: new TypeError("fetch failed"),
      }),
    },
  ])(
    `${adapter}: a deliberate foreign $name remains private and raises an operator alert`,
    async ({ cause }) => {
      const events: SandboxFailureEvent[] = [];
      const alerts: unknown[] = [];
      const result = await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const runtime = yield* make({
              sandbox:
                adapter === "promise"
                  ? fromTypedSandbox({ execute: () => foreignRejection(cause) })
                  : { execute: () => Effect.die(cause) },
              onSandboxFailure: (event) =>
                Effect.sync(() => {
                  events.push(event);
                }),
            });
            const lease = yield* runtime.acquire();
            const prepared = yield* lease.executeTool.prepare({
              type: "toolCall",
              id: "failure",
              name: "execute",
              arguments: { code: "return 1" },
            });
            return yield* Effect.flip(Stream.runCollect(prepared.execute()));
          }),
        ).pipe(
          Effect.provide(
            Logger.layer([
              Logger.make((entry) => {
                alerts.push(entry.message);
              }),
            ]),
          ),
        ),
      );
      expect(result.message).toMatch(
        /^The sandbox failed for an unrecognized reason \(ref [^)]+\)$/u,
      );
      expect(result.message).not.toContain("marker");
      expect(events).toHaveLength(1);
      const event = events[0];
      expect(event).toMatchObject({
        kind: "sandbox_failure",
        tag: "Unknown",
        adapter,
        healthAlert: true,
      });
      expect(event?.executionId).toBeTypeOf("string");
      expect(event?.ref).toBe(event?.executionId);
      expect(JSON.stringify(event?.fields)).toContain(
        "private transport marker",
      );
      expect(JSON.stringify(alerts)).toContain("sandbox_health_alert");
      if (cause instanceof TypeError)
        expect(event?.fields).toMatchObject({
          rawCause: { name: "TypeError", cause: { message: "nested marker" } },
        });
    },
  );
}

it("records a typed capability's original stack and cause while projecting only its admitted message", async () => {
  const original = new CapabilityFailed({
    namespace: "Files",
    operation: "read",
    cause: { kind: "Files", message: "File is unavailable" },
    rawCause: new Error("private capability marker"),
  });
  expect(original.message).toBe("File is unavailable");
  expect(original.message).not.toContain("marker");
  const events: SandboxFailureEvent[] = [];
  const result = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* make({
          sandbox: fromTypedSandbox({
            execute: () => Promise.reject(original),
          }),
          onSandboxFailure: (event) =>
            Effect.sync(() => {
              events.push(event);
            }),
        });
        const lease = yield* runtime.acquire();
        const prepared = yield* lease.executeTool.prepare({
          type: "toolCall",
          id: "typed-failure",
          name: "execute",
          arguments: { code: "return 1" },
        });
        return yield* Effect.flip(Stream.runCollect(prepared.execute()));
      }),
    ).pipe(Effect.provide(Logger.layer([]))),
  );
  expect(result.message).toMatch(/^File is unavailable \(ref /u);
  expect(result.message).not.toContain("marker");
  expect(events).toHaveLength(1);
  expect(events[0]?.fields.stack).toBe(original.stack);
  expect(events[0]?.fields.rawCause).toMatchObject({
    message: "private capability marker",
  });
  expect(events[0]?.healthAlert).toBe(false);
});

it("leases one ordinary Mule execute tool", async () => {
  const result = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* make({
          execution: { timeoutMs: 17_003 },
          modules: [],
          sandbox: {
            execute: (request) =>
              Effect.succeed({ answer: 42, timeoutMs: request.timeoutMs }),
          },
        });
        const lease = yield* runtime.acquire();
        const call: ToolCall = {
          type: "toolCall",
          id: "mycelium-call-1",
          name: "execute",
          arguments: { code: "return { answer: 42 }" },
        };
        const prepared = yield* lease.executeTool.prepare(call);
        const parts = yield* Stream.runCollect(prepared.execute());
        return {
          descriptor: lease.executeTool.descriptor,
          parts,
        };
      }),
    ),
  );

  expect(result.descriptor.description).toContain(
    "double each backslash the shell needs",
  );
  expect(result.descriptor.description).toContain(String.raw`\\(`);
  expect(result.descriptor.description).toContain(
    "Never shadow injected module names with local bindings.",
  );
  expect(result).toEqual({
    descriptor: {
      name: "execute",
      description:
        'Run async JavaScript in the sandbox. Each call is a fresh sandbox, not a REPL: nothing survives between calls, so redefine helpers or persist state to a file. Never shadow injected module names with local bindings. Find operations with await forage.search({ query }) using one short keyword such as read or shell, or page them all with forage.list({limit:50}), optionally by namespace, until nextCursor is null. Inspect input with await forage.describe({ path }) before calling; describe known paths directly. Call an operation as await env.<path>({ ... }) with the exact dotted path, for example await env.computers.local.exec({ command: "pwd" }); a bare computers.local.exec is not defined. Put multiline shell commands in a template literal. Code is JavaScript first, so double each backslash the shell needs (`\\\\(` passes `\\(`; `\\(` becomes `(`). For file edits, prefer Files operations when available. Explicitly return the final value. Before treating a shell or process result as success, check status, exitCode and stderr.',
      parameters: {
        type: "object",
        properties: { code: { type: "string" } },
        required: ["code"],
        additionalProperties: false,
      },
    },
    parts: [
      {
        type: "result",
        result: {
          content: [{ type: "text", text: '{"answer":42,"timeoutMs":17003}' }],
          isError: false,
        },
      },
    ],
  });
});

const pixel = Object.freeze({
  data: "cGl4ZWw=",
  mimeType: "image/png" as const,
  bytes: 5,
});

/** One `pics.view` operation that pushes into its execution's collector. */
const picsModule = Mycelium.module({
  id: "pics",
  operations: {
    view: Mycelium.operation({
      replay: "safe",
      input: z.strictObject({ path: z.string() }),
      output: z.strictObject({ imageRef: z.string(), path: z.string() }),
      execute: ({ path }, context) => {
        if (context.images === undefined)
          return Promise.reject(new Error("execute supplies a collector"));
        return Promise.resolve({
          imageRef: context.images.push({ ...pixel, path }),
          path,
        });
      },
    }),
  },
});

function runWithPics(
  guest: (
    view: (path: string) => Promise<SandboxValue>,
  ) => Promise<SandboxValue>,
) {
  return Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* make({
          modules: [picsModule],
          sandbox: {
            execute: (request) =>
              Effect.promise(() => {
                const view = request.bindings.pics?.view;
                if (view === undefined) throw new Error("pics.view missing");
                return guest((path) => view({ path }));
              }),
          },
        });
        const lease = yield* runtime.acquire();
        const prepared = yield* lease.executeTool.prepare({
          type: "toolCall",
          id: "mycelium-call-image",
          name: "execute",
          arguments: { code: "return await pics.view({ path: '/a.png' })" },
        });
        return yield* Effect.result(Stream.runCollect(prepared.execute()));
      }),
    ),
  );
}

it("appends images after the model text and keeps their display metadata in data", async () => {
  const result = await runWithPics(async (view) => [
    await view("/a.png"),
    await view("/b.png"),
  ]);

  expect(result).toEqual(
    Result.succeed([
      {
        type: "result",
        result: {
          content: [
            {
              type: "text",
              text: '[{"imageRef":"img_0","path":"/a.png"},{"imageRef":"img_1","path":"/b.png"}]',
            },
            { type: "image", data: pixel.data, mimeType: "image/png" },
            { type: "image", data: pixel.data, mimeType: "image/png" },
          ],
          data: {
            images: [
              { bytes: 5, mimeType: "image/png", path: "/a.png" },
              { bytes: 5, mimeType: "image/png", path: "/b.png" },
            ],
          },
          isError: false,
        },
      },
    ]),
  );
});

it("never turns a value the guest returns into an image", async () => {
  const result = await runWithPics(() =>
    Promise.resolve({
      __myceliumImage: 1,
      data: pixel.data,
      mimeType: "image/png",
    }),
  );

  expect(result).toEqual(
    Result.succeed([
      {
        type: "result",
        result: {
          content: [
            {
              type: "text",
              text: `{"__myceliumImage":1,"data":"${pixel.data}","mimeType":"image/png"}`,
            },
          ],
          isError: false,
        },
      },
    ]),
  );
});

it("keeps collected images on the oversized-result path", async () => {
  const result = await runWithPics(async (view) => {
    await view("/a.png");
    return "x".repeat(300 * 1024);
  });

  expect(Result.isSuccess(result)).toBe(true);
  if (!Result.isSuccess(result)) return;
  const [part] = result.success;
  expect(part?.result.isError).toBe(true);
  expect(part?.result.content.slice(1)).toEqual([
    { type: "image", data: pixel.data, mimeType: "image/png" },
  ]);
});

it("caps one execution at the collector's bound and starts every execution empty", async () => {
  const capped = await runWithPics(async (view) => {
    for (let index = 0; index < MAX_EXECUTION_IMAGES; index += 1)
      await view(`/${String(index)}.png`);
    return await view("/one-too-many.png");
  });
  expect(Result.isFailure(capped)).toBe(true);
  if (Result.isFailure(capped))
    expect(capped.failure.message).toContain("Image collector cap reached");

  const fresh = await runWithPics(async (view) => await view("/a.png"));
  expect(Result.isSuccess(fresh)).toBe(true);
  if (Result.isSuccess(fresh))
    expect(fresh.success[0]?.result.content).toHaveLength(2);
});
