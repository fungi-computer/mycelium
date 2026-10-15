import { createMcp, createMcpCatalog } from "@fungi.computer/mycelium/mcp";
import {
  Mycelium,
  MyceliumClosedError,
  MyceliumDefinitionError,
  Cancelled,
  Unknown,
  MyceliumSourceError,
  type Lease,
  type ModuleRegistration,
  type Runtime,
  type Sandbox,
} from "@fungi.computer/mycelium";
import {
  make as makeEffectRuntime,
  type Runtime as EffectRuntime,
  type Sandbox as EffectSandbox,
} from "@fungi.computer/mycelium/effect";
import * as Effect from "effect/Effect";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { z } from "zod";
import {
  directSandbox,
  rejectingSandbox,
} from "@fungi.computer/mycelium/testing";

const module: ModuleRegistration = Mycelium.module({
  id: "typed-stranger",
  operations: {
    ping: Mycelium.operation({
      input: z.object({ value: z.string() }),
      execute: ({ value }) => Promise.resolve({ value }),
    }),
  },
});
const sandbox: Sandbox = directSandbox(() => Promise.resolve(null));
const runtimePromise: Promise<Runtime> = Mycelium.make({
  modules: [module],
  sandbox,
});
const effectSandbox: EffectSandbox = {
  execute: () => Effect.succeed(null),
};
const effectRuntime: Effect.Effect<EffectRuntime, Error, Scope.Scope> =
  makeEffectRuntime({
    modules: [module],
    sandbox: effectSandbox,
  });

const errorCodes: readonly string[] = [
  new MyceliumClosedError().code,
  new MyceliumDefinitionError("invalid").code,
  new Cancelled({ by: "caller" })._tag,
  new Unknown({ rawCause: "foreign rejection" })._tag,
  new MyceliumSourceError(undefined, "stale").code,
];

void errorCodes;
void effectRuntime;
void runtimePromise.then(async (runtime) => {
  const lease: Lease = await runtime.acquire();
  try {
    const prepared = await Effect.runPromise(
      lease.executeTool.prepare({
        type: "toolCall",
        id: "typed-stranger-execute",
        name: "execute",
        arguments: { code: "return null" },
      }),
    );
    await Effect.runPromise(Stream.runCollect(prepared.execute()));
  } finally {
    await lease.release();
    await runtime.close();
  }
});

void directSandbox(async (bindings, signal) => {
  signal.throwIfAborted();
  const ping = bindings.demo?.ping;
  if (!ping) throw new Error("Missing demo.ping");
  return ping({});
});
void rejectingSandbox("no tools");

const mcp = createMcp({
  catalog: createMcpCatalog({
    load: () => Promise.resolve({ instructions: "", sources: [], grants: {} }),
    sandbox: directSandbox(() => Promise.resolve(null)),
  }),
  authenticate: () => Promise.resolve({ kind: "refused", status: 401 }),
});
try {
  const response = await mcp.handleRequest(
    new Request("https://mcp.invalid/mcp", { method: "POST" }),
  );
  if (response.status !== 401)
    throw new Error("MCP package did not enforce authentication");
} finally {
  await mcp.close();
}

import type { Sandbox as NeutralSandbox } from "@fungi.computer/mycelium/sandbox";
export type { NeutralSandbox };

import {
  sameMcpPrincipal,
  type McpPrincipal,
} from "@fungi.computer/mycelium/mcp";
const canonicalPrincipal: McpPrincipal = {
  actor: { kind: "member", memberId: "packed-member" },
  role: "member",
};
const canonicalAgent: McpPrincipal = {
  actor: { kind: "agent", agentId: "packed-agent" },
};
void sameMcpPrincipal(canonicalPrincipal, canonicalAgent);

import type { McpAuthentication } from "@fungi.computer/mycelium/mcp";
const policyRefusal: McpAuthentication = {
  kind: "refused",
  status: 429,
  retryAfter: 9,
};
void policyRefusal;
