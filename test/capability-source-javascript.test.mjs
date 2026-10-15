import { directSandbox } from "@fungi.computer/mycelium/testing";
import { expect, it } from "vitest";
import { Mycelium } from "../src/index.js";

// A JavaScript host can supply malformed projections; the TypeScript contract
// intentionally forbids this input. Exercise the real runtime admission here.
it("rejects a JavaScript host's non-array modules, awaiting release before rejection", async () => {
  const releaseStarted = Promise.withResolvers();
  const releaseGate = Promise.withResolvers();
  let releases = 0;
  let released = false;
  let sandboxExecutions = 0;
  let settled = false;
  const runtime = await Mycelium.make({
    sources: [
      {
        id: "invalid-javascript-source",
        acquire: () =>
          Promise.resolve({
            snapshot: { version: "v1", modules: "not-an-array" },
            release: async () => {
              releases += 1;
              releaseStarted.resolve();
              await releaseGate.promise;
              released = true;
            },
          }),
      },
    ],
    sandbox: directSandbox(() => {
      sandboxExecutions += 1;
      return Promise.resolve(null);
    }),
  });
  const acquisition = runtime
    .acquire()
    .then(async (lease) => {
      await lease.release();
      throw new Error("Malformed source unexpectedly acquired a lease");
    })
    .finally(() => {
      settled = true;
    });
  try {
    const first = await Promise.race([
      releaseStarted.promise.then(() => "release-started"),
      acquisition.then(
        () => "settled",
        () => "settled",
      ),
    ]);
    expect(first).toBe("release-started");
    expect(settled).toBe(false);
    releaseGate.resolve();
    await expect(acquisition).rejects.toMatchObject({
      code: "invalid_definition",
    });
    expect(releases).toBe(1);
    expect(released).toBe(true);
    expect(sandboxExecutions).toBe(0);
  } finally {
    releaseGate.resolve();
    try {
      await expect(acquisition).rejects.toMatchObject({
        code: "invalid_definition",
      });
    } finally {
      await runtime.close();
    }
  }
});

it("rejects a JavaScript host's string namespace before capturing its characters", () => {
  expect(() =>
    Mycelium.module({ id: "app", namespace: "apps", operations: {} }),
  ).toThrow("Invalid module namespace");
});
