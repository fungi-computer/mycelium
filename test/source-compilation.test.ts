import { expect, it, vi } from "vitest";
import { z } from "zod";
import * as compiler from "../src/compiler.js";
import { Mycelium } from "../src/index.js";
import { directSandbox } from "../src/testing.js";

const notes = Mycelium.module({
  id: "notes",
  name: "NOTES",
  operations: {
    read: Mycelium.operation({
      replay: "safe",
      input: z.object({}),
      output: z.string(),
      execute: () => Promise.resolve("note"),
    }),
  },
});
const source = {
  id: "notes-source",
  acquire: () =>
    Promise.resolve({
      snapshot: { version: "v1", modules: [notes] },
      release: () => Promise.resolve(),
    }),
};

it("compiles only the acquired catalog for a source-only runtime", async () => {
  const compilation = vi.spyOn(compiler, "compileSurface");
  try {
    const runtime = await Mycelium.make({
      sources: [source],
      sandbox: directSandbox(() => Promise.resolve(null)),
    });
    try {
      expect(compilation).not.toHaveBeenCalled();
      const lease = await runtime.acquire();
      try {
        expect(compilation).toHaveBeenCalledTimes(1);
        expect(lease.catalog.describe({ path: "NOTES.read" }).types).toContain(
          "string",
        );
      } finally {
        await lease.release();
      }
    } finally {
      await runtime.close();
    }
  } finally {
    compilation.mockRestore();
  }
});

it("still validates supplied static modules before acquiring sources", async () => {
  await expect(
    Mycelium.make({
      modules: [notes, notes],
      sources: [source],
      sandbox: directSandbox(() => Promise.resolve(null)),
    }),
  ).rejects.toMatchObject({ code: "invalid_definition" });
});
