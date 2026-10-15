/* global AbortController, Response, URL, structuredClone */
import {
  isSandboxFailure,
  sandboxFailure,
} from "@fungi.computer/mycelium/sandbox-boundary";
import { createCodemodeSandbox } from "@fungi.computer/mycelium/workers";
import { z } from "zod";

const programs = {
  input: `const input = await env.INPUT.read({});
    input.names[0] = "changed";
    return { input: await env.INPUT.read({}), authority: Object.keys(env) };`,
  network: `try { await fetch("https://unreachable.invalid/"); return false; }
    catch { return true; }`,
  source: "return );",
  guest: 'throw new Error("private guest diagnostic");',
  cancel: `await env.INPUT.read({}); return "late success";`,
  deadline: "return new Promise(() => {});",
  memory: `const values = Array.from({ length: 524288 }, (_, i) => i + 0.5);
    return values.length;`,
  output: 'return "o".repeat(70000);',
  cpu: `let value = 0;
    for (let i = 0; i < 50_000_000; i++) value = (value + i) % 1000003;
    return value;`,
};

export default {
  async fetch(request, env) {
    const route = new URL(request.url).pathname.slice(1);
    if (route === "ready") return new Response("ready");
    const code = programs[route];
    if (code === undefined) return new Response(null, { status: 404 });
    const sandbox = createCodemodeSandbox(env.LOADER, "native-proof", {
      localDevUnenforcedLimits:
        env.LOCAL_WORKERD === "true"
          ? ["cpu", "memory", "output", "cancel"]
          : ["memory", "output", "cancel"],
    });
    const controller = new AbortController();
    const execute = (executionId) =>
      sandbox.execute(
        {
          executionId,
          code,
          timeoutMs: route === "cpu" ? 10 : route === "deadline" ? 250 : 5_000,
          bindings: {
            INPUT: {
              read: (input) => {
                z.strictObject({}).parse(input);
                if (route === "cancel") controller.abort();
                return structuredClone({ names: ["Garden"] });
              },
            },
          },
        },
        controller.signal,
      );
    // More than four calls in one request also exercise Codemode's native
    // stub disposal after timeout; Loader's concurrent-worker ceiling is four.
    if (route === "deadline") {
      const failures = [];
      for (let attempt = 0; attempt < 6; attempt++) {
        try {
          await execute(`native-deadline-${attempt}`);
          failures.push("completed");
        } catch (cause) {
          if (!isSandboxFailure(cause)) throw cause;
          failures.push(cause._tag);
        }
      }
      return Response.json({ failures });
    }
    try {
      const value = await execute(`native-${route}`);
      if (route === "output")
        return Response.json({
          kind: "completed",
          length: z.string().parse(value).length,
        });
      return Response.json({ kind: "completed", value });
    } catch (cause) {
      return Response.json({
        kind: "failed",
        failure: sandboxFailure(cause)._tag,
      });
    }
  },
};
