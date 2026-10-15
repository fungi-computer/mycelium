/* global AbortController, console */
import { expect, it, vi } from "vitest";
import { createCodemodeSandbox } from "../dist/workers/index.js";

it("forwards the caller's existing execution budget as a native CPU limit", async () => {
  const definitions = [];
  const sandbox = createCodemodeSandbox({
    load(definition) {
      definitions.push(definition);
      return {
        getEntrypoint: () => ({
          evaluate: async () => ({ result: { ok: true, value: ["Garden"] } }),
        }),
      };
    },
  });
  await expect(
    sandbox.execute(
      {
        executionId: "code-budget",
        code: 'return ["Garden"];',
        timeoutMs: 17_003,
        bindings: {},
      },
      new AbortController().signal,
    ),
  ).resolves.toEqual(["Garden"]);
  expect(definitions).toHaveLength(1);
  expect(definitions[0].limits).toEqual({ cpuMs: 17_003 });
  expect(definitions[0].globalOutbound).toBeNull();
});

it("reports unenforced local-dev limits once without refusing execution", async () => {
  const warning = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  try {
    const loader = {
      load: () => ({
        getEntrypoint: () => ({
          evaluate: async () => ({ result: { ok: true, value: "completed" } }),
        }),
      }),
    };
    for (let attempt = 0; attempt < 2; attempt++) {
      const sandbox = createCodemodeSandbox(loader, "local-dev", {
        localDevUnenforcedLimits: ["cpu", "memory", "output", "cancel"],
      });
      await expect(
        sandbox.execute(
          {
            executionId: `dev-${attempt}`,
            code: 'return "completed";',
            timeoutMs: 1_000,
            bindings: {},
          },
          new AbortController().signal,
        ),
      ).resolves.toBe("completed");
    }
    expect(warning.mock.calls).toEqual([
      ["CPU limits are not enforced in local dev"],
      ["Memory limits are not enforced in local dev"],
      ["Output limits are not enforced in local dev"],
      ["Cancellation limits are not enforced in local dev"],
    ]);
  } finally {
    warning.mockRestore();
  }
});
