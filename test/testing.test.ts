import { expect, it } from "vitest";
import { directSandbox, rejectingSandbox } from "../src/testing.js";
import type { SandboxRequest } from "../src/index.js";

const request: SandboxRequest = {
  executionId: "testing",
  code: "this code is deliberately not evaluated",
  timeoutMs: 1000,
  bindings: { demo: { ping: () => Promise.resolve({ value: "pong" }) } },
};
it("dispatches only the supplied callback to the admitted bindings", async () => {
  const sandbox = directSandbox(async (bindings) => {
    const ping = bindings.demo?.ping;
    if (!ping) throw new Error("Missing demo.ping");
    return ping({});
  });
  expect(await sandbox.execute(request, new AbortController().signal)).toEqual({
    value: "pong",
  });
});
it("refuses cancellation before invoking the callback", async () => {
  let called = false;
  const controller = new AbortController();
  controller.abort(new Error("cancelled"));
  const sandbox = directSandbox(() => {
    called = true;
    return Promise.resolve(null);
  });
  await expect(sandbox.execute(request, controller.signal)).rejects.toThrow(
    "cancelled",
  );
  expect(called).toBe(false);
});
it("provides a deterministic rejecting transport", async () => {
  await expect(
    rejectingSandbox("no tools").execute(request, new AbortController().signal),
  ).rejects.toThrow("no tools");
});

it("does not return a result after the callback observes cancellation", async () => {
  const controller = new AbortController();
  const sandbox = directSandbox(() => {
    controller.abort(new Error("cancelled during dispatch"));
    return Promise.resolve({ value: "must not escape" });
  });
  await expect(sandbox.execute(request, controller.signal)).rejects.toThrow(
    "cancelled during dispatch",
  );
});

it("preserves the callback's rejection", async () => {
  const failure = new Error("binding unavailable");
  const sandbox = directSandbox(() => Promise.reject(failure));
  await expect(
    sandbox.execute(request, new AbortController().signal),
  ).rejects.toBe(failure);
});
