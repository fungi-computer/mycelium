import { sameMcpPrincipal } from "./internal/mcp-principal.js";
import type { MuleToolResult } from "@fungi.computer/mule/effect";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import {
  Mycelium,
  type CapabilitySource,
  type Grants,
  type Lease,
  type Sandbox,
} from "./index.js";
import type { AccessOptions } from "./access.js";
import type {
  McpCatalogProvider,
  McpCatalogSession,
  McpPrincipal,
} from "./mcp.js";

/** Current installed capabilities and explicit grants for one admitted principal. */
export type McpCatalogSnapshot = Readonly<{
  instructions: string;
  sources: readonly CapabilitySource[];
  /** Every permitted module is named; an omitted module is refused. */
  grants: Grants;
  /**
   * Granted modules whose writes a person already approved for this principal,
   * such as an owner's stored approval of a key's App tools. Their writes run
   * without a per-call elicitation; every other write still asks.
   */
  approvedWrites?: readonly string[];
}>;

/**
 * Adapt principal-scoped capability sources and a sandbox to the shared MCP server.
 * Connections are process-local. Durable hosts supply their own catalog provider.
 */
export function createMcpCatalog(
  options: Readonly<{
    load: (principal: McpPrincipal) => Promise<McpCatalogSnapshot>;
    sandbox: Sandbox;
  }>,
): McpCatalogProvider {
  const sessions = new Map<string, McpCatalogSession>();
  return {
    open(principal, input) {
      let principalSessions = 0;
      for (const session of sessions.values()) {
        if (sameMcpPrincipal(session.principal, principal)) principalSessions++;
      }
      if (principalSessions >= input.maxOpenSessions)
        return Promise.resolve(null);
      let snapshot: McpCatalogSnapshot | undefined;
      let requestedRefresh = 0;
      let appliedRefresh = 0;
      const refresh = async () => {
        const revision = ++requestedRefresh;
        const current = await options.load(principal);
        // A slower, older storage read cannot restore authority after revocation.
        if (revision > appliedRefresh) {
          snapshot = current;
          appliedRefresh = revision;
        }
      };
      async function withLease<A>(
        access: AccessOptions,
        use: (lease: Lease, acquired: McpCatalogSnapshot) => Promise<A>,
      ) {
        if (snapshot === undefined)
          throw new Error("MCP catalog was not acquired");
        const acquired = snapshot;
        const runtime = await Mycelium.make({
          sources: acquired.sources,
          session: { sessionId: input.id },
          sandbox: options.sandbox,
          access: {
            ...access,
            defaultGrant: "none",
            sessionGrants: () => snapshot?.grants ?? {},
            approvedWrites: () => snapshot?.approvedWrites ?? [],
          },
        });
        try {
          const lease = await runtime.acquire();
          try {
            return await use(lease, acquired);
          } finally {
            await lease.release();
          }
        } finally {
          await runtime.close();
        }
      }
      const session: McpCatalogSession = {
        id: input.id,
        principal,
        refresh,
        describe: () =>
          withLease({}, (lease, acquired) =>
            Promise.resolve({
              instructions: acquired.instructions,
              tools: [
                lease.executeTool.descriptor,
                ...lease.tools.map(({ descriptor }) => descriptor),
              ],
            }),
          ),
        call: (call, context) =>
          withLease(context.access, async (lease) => {
            const tool = [lease.executeTool, ...lease.tools].find(
              ({ descriptor }) => descriptor.name === call.name,
            );
            if (tool === undefined) throw new Error("Unknown MCP tool");
            let result: MuleToolResult | undefined;
            await Effect.runPromise(
              Effect.gen(function* () {
                const prepared = yield* tool.prepare({
                  type: "toolCall",
                  ...call,
                });
                yield* prepared.execute().pipe(
                  Stream.runForEach((part) => {
                    if (part.type === "result") {
                      result = part.result;
                      return Effect.void;
                    }
                    return Effect.promise(async () => {
                      await context.onPart?.(part);
                    });
                  }),
                );
              }),
              { signal: context.signal },
            );
            if (result === undefined)
              throw new Error("MCP tool returned no result");
            return result;
          }),
        disconnect: () => {
          sessions.delete(input.id);
          return Promise.resolve();
        },
      };
      sessions.set(input.id, session);
      return Promise.resolve(session);
    },
    resume(principal, id) {
      const session = sessions.get(id);
      return Promise.resolve(
        session !== undefined && sameMcpPrincipal(session.principal, principal)
          ? session
          : null,
      );
    },
  };
}
