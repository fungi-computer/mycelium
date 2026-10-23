import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import { expect, it } from "vitest";
import { z } from "zod";
import { Mycelium, type InvocationContext } from "../src/index.js";
import { directSandbox } from "../src/testing.js";

it("numbers operation calls in invocation order across methods and modules, resetting on execute replay", async () => {
  const observed: InvocationContext[] = [];
  const operation = () =>
    Mycelium.operation({
      input: z.strictObject({}),
      output: z.null(),
      execute: async (_input, context) => {
        observed.push(context);
        // Ordinals belong to invocation order, not async completion order.
        await Promise.resolve();
        return null;
      },
    });
  const runtime = await Mycelium.make({
    modules: [
      Mycelium.module({
        id: "notes",
        operations: { read: operation(), saveNote: operation() },
      }),
      Mycelium.module({
        id: "other",
        operations: { saveNote: operation() },
      }),
    ],
    sandbox: directSandbox(async (bindings) => {
      const read = bindings.notes?.read;
      const save = bindings.notes?.saveNote;
      const other = bindings.other?.saveNote;
      if (read === undefined || save === undefined || other === undefined)
        throw new Error("Expected operation bindings");
      await read({});
      await Promise.all([save({}), other({}), save({})]);
      return null;
    }),
  });
  const lease = await runtime.acquire();
  try {
    for (let attempt = 0; attempt < 2; attempt++) {
      const prepared = await Effect.runPromise(
        lease.executeTool.prepare({
          type: "toolCall",
          id: "same-stored-tool-call",
          name: "execute",
          arguments: {
            code: "read; concurrent notes, other, notes writes",
          },
        }),
      );
      await Effect.runPromise(Stream.runCollect(prepared.execute()));
    }
    expect(observed).toMatchObject([
      { toolCallId: "same-stored-tool-call", operationOrdinal: 0 },
      { toolCallId: "same-stored-tool-call", operationOrdinal: 1 },
      { toolCallId: "same-stored-tool-call", operationOrdinal: 2 },
      { toolCallId: "same-stored-tool-call", operationOrdinal: 3 },
      { toolCallId: "same-stored-tool-call", operationOrdinal: 0 },
      { toolCallId: "same-stored-tool-call", operationOrdinal: 1 },
      { toolCallId: "same-stored-tool-call", operationOrdinal: 2 },
      { toolCallId: "same-stored-tool-call", operationOrdinal: 3 },
    ]);
    expect(
      new Set(observed.slice(0, 4).map((item) => item.executionId)).size,
    ).toBe(1);
    expect(observed[0]?.executionId).not.toBe(observed[4]?.executionId);
    expect(observed.every(Object.isFrozen)).toBe(true);
  } finally {
    await lease.release();
    await runtime.close();
  }
});
