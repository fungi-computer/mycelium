import { directSandbox } from "@fungi.computer/mycelium/testing";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import { expect, it } from "vitest";
import { z } from "zod";

import { Mycelium, type GateDecision } from "../src/index.js";

it("gates every operation of a module and returns the typed refusal without running it", async () => {
  const ran: string[] = [];
  const base = Mycelium.module({
    id: "demo",
    name: "demo",
    description: "Demo operations",
    operations: {
      ping: Mycelium.operation({
        input: z.object({ value: z.string() }),
        output: z.object({ value: z.string() }),
        execute: ({ value }) => {
          ran.push(value);
          return Promise.resolve({ value });
        },
      }),
    },
  });
  let decision: GateDecision = { kind: "allow" };
  const asked: string[] = [];
  const gated = Mycelium.gate(base, ({ operation }) => {
    asked.push(operation);
    return decision;
  });
  const results: unknown[] = [];
  const runtime = await Mycelium.make({
    modules: [gated],
    sandbox: directSandbox(async (bindings) => {
      const ping = bindings.demo?.ping;
      if (ping === undefined) throw new Error("demo.ping missing");
      results.push(await ping({ value: "pong" }));
      decision = {
        kind: "refuse",
        value: { refused: true, tool: "demo", reason: "not-granted" },
      };
      // A refused call skips even the input decoder.
      results.push(await ping({ wrong: 1 }));
      return null;
    }),
  });
  const lease = await runtime.acquire();
  try {
    // The gated module keeps its alias and schemas.
    expect(lease.catalog.describe({ path: "demo.ping" })).toMatchObject({
      path: "demo.ping",
    });
    const prepared = await Effect.runPromise(
      lease.executeTool.prepare({
        type: "toolCall",
        id: "gate-call",
        name: "execute",
        arguments: { code: "return demo.ping({ value: 'pong' })" },
      }),
    );
    await Effect.runPromise(Stream.runCollect(prepared.execute()));
    expect(results).toEqual([
      { value: "pong" },
      { refused: true, tool: "demo", reason: "not-granted" },
    ]);
    expect(ran).toEqual(["pong"]);
    expect(asked).toEqual(["ping", "ping"]);
  } finally {
    await lease.release();
    await runtime.close();
  }
});
