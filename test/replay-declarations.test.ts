import { directSandbox } from "@fungi.computer/mycelium/testing";
import { expect, it } from "vitest";
import { z } from "zod";

import { Mycelium } from "../src/index.js";
import { compileSurface } from "../src/compiler.js";

it("retains operation declarations through compilation and gating without making execute replayable", async () => {
  const module = Mycelium.module({
    id: "replay-proof",
    operations: {
      read: Mycelium.operation({
        replay: "safe",
        input: z.strictObject({}),
        output: z.string(),
        execute: () => Promise.resolve("read"),
      }),
      write: Mycelium.operation({
        replay: "unsafe",
        input: z.strictObject({}),
        output: z.string(),
        execute: () => Promise.resolve("written"),
      }),
      strangerDefault: Mycelium.operation({
        input: z.strictObject({}),
        output: z.string(),
        execute: () => Promise.resolve("undeclared"),
      }),
    },
  });
  const gated = Mycelium.gate(module, () => ({ kind: "allow" }));
  expect(
    compileSurface([gated]).operations.map(({ path, operation }) => [
      path,
      operation.replay,
    ]),
  ).toEqual([
    ["replay_proof.read", "safe"],
    ["replay_proof.strangerDefault", "unsafe"],
    ["replay_proof.write", "unsafe"],
  ]);
  const runtime = await Mycelium.make({
    modules: [gated],
    sandbox: directSandbox(() => Promise.resolve(null)),
  });
  const lease = await runtime.acquire();
  try {
    expect(lease.executeTool.replay).toBe("unsafe");
  } finally {
    await lease.release();
    await runtime.close();
  }
});
