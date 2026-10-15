import {
  createMcp,
  createMcpCatalog,
  sameMcpPrincipal,
} from "@fungi.computer/mycelium/mcp";
import { directSandbox } from "@fungi.computer/mycelium/testing";
import { Mycelium } from "@fungi.computer/mycelium";
import { make as makeEffectRuntime } from "@fungi.computer/mycelium/effect";
import * as Effect from "effect/Effect";
import * as Predicate from "effect/Predicate";
import * as Stream from "effect/Stream";
import { z } from "zod";

if (!Predicate.isFunction(makeEffectRuntime)) {
  throw new Error("Mycelium stranger import lost its Effect entrypoint");
}

const runtime = await Mycelium.make({
  modules: [
    Mycelium.module({
      id: "stranger-module",
      name: "demo",
      operations: {
        ping: Mycelium.operation({
          input: z.object({}),
          output: z.object({ value: z.string() }),
          execute: () => Promise.resolve({ value: "pong" }),
        }),
      },
    }),
  ],
  sandbox: directSandbox((bindings) => {
    const ping = bindings.demo?.ping;
    if (ping === undefined) throw new Error("Missing demo.ping binding");
    return ping({});
  }),
});
const lease = await runtime.acquire();

try {
  if (lease.executeTool.descriptor.name !== "execute") {
    throw new Error("Mycelium stranger import lost its execute descriptor");
  }
  const signal = new globalThis.AbortController().signal;
  const prepared = await Effect.runPromise(
    lease.executeTool.prepare({
      type: "toolCall",
      id: "stranger-execute",
      name: "execute",
      arguments: { code: "return demo.ping({})" },
    }),
    { signal },
  );
  const parts = await Effect.runPromise(Stream.runCollect(prepared.execute()), {
    signal,
  });
  if (parts[0]?.result?.content?.[0]?.text !== '{"value":"pong"}') {
    throw new Error(
      "Mycelium stranger execute route returned the wrong result",
    );
  }
} finally {
  await lease.release();
  await runtime.close();
}

const mcp = createMcp({
  catalog: createMcpCatalog({
    load: () => Promise.resolve({ instructions: "", sources: [], grants: {} }),
    sandbox: directSandbox(() => Promise.resolve(null)),
  }),
  authenticate: () => Promise.resolve({ kind: "refused", status: 401 }),
});
try {
  const response = await mcp.handleRequest(
    new globalThis.Request("https://mcp.invalid/mcp", { method: "POST" }),
  );
  if (response.status !== 401)
    throw new Error("MCP package did not enforce authentication");
} finally {
  await mcp.close();
}

const owner = {
  actor: { kind: "member", memberId: "packed-member" },
  role: "owner",
};
if (
  !sameMcpPrincipal(owner, { ...owner }) ||
  sameMcpPrincipal(owner, { ...owner, role: "member" })
)
  throw new Error("MCP package lost canonical actor/role session identity");

const refusedMcp = createMcp({
  catalog: createMcpCatalog({
    load: async () => ({ instructions: "", sources: [], grants: {} }),
    sandbox: directSandbox(() => Promise.resolve(null)),
  }),
  authenticate: async () => ({ kind: "refused", status: 429, retryAfter: 9 }),
});
try {
  const response = await refusedMcp.handleRequest(
    new globalThis.Request("https://mcp.invalid/mcp", { method: "POST" }),
  );
  if (response.status !== 429 || response.headers.get("Retry-After") !== "9")
    throw new Error("Packed MCP lost the auth refusal status/header");
} finally {
  await refusedMcp.close();
}
