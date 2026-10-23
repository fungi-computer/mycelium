import {
  sameMcpPrincipal,
  type McpPrincipal,
} from "./internal/mcp-principal.js";
export { sameMcpPrincipal, type McpPrincipal };
export { createMcpCatalog, type McpCatalogSnapshot } from "./mcp-catalog.js";
import { nanoid } from "@fungi.computer/cairn";
import type {
  MuleTool,
  MuleToolExecutionPart,
  MuleToolResult,
  ToolCall,
} from "@fungi.computer/mule/effect";
import {
  McpServer as ProtocolServer,
  WebStandardStreamableHTTPServerTransport,
  specTypeSchemas,
  type EventStore,
  type WebStandardStreamableHTTPServerTransportOptions,
} from "@modelcontextprotocol/server";
import * as Schema from "effect/Schema";
import type { AccessOptions, ConfirmRequest } from "./access.js";

/** Admission is required on every HTTP request, including notifications. */
export type McpAuthentication =
  | Readonly<{ kind: "admitted"; principal: McpPrincipal }>
  | Readonly<{
      kind: "refused";
      status: 401 | 403 | 404 | 429;
      retryAfter?: number;
    }>;

/** A JSON tool call submitted to the host's execution owner. */
export type McpToolCall = Readonly<{
  id: string;
  name: string;
  arguments: ToolCall["arguments"];
}>;

/** Request-scoped hooks carried into the same Mycelium runtime as hosted tools. */
export type McpExecutionContext = Readonly<{
  signal: AbortSignal;
  /** Forward non-terminal tool output through this request's progress channel. */
  onPart?: (
    part: Exclude<MuleToolExecutionPart, { type: "result" }>,
  ) => Promise<void>;
  /** Apply to Mycelium access options for this call only; never persist callbacks. */
  access: Pick<AccessOptions, "confirm"> &
    Readonly<{
      /** Recheck the open connection and refresh authority before each operation. */
      beforeCall: (signal: AbortSignal) => Promise<void>;
    }>;
}>;

/** A principal-bound catalog and its existing execution owner. */
export type McpCatalogSession = Readonly<{
  id: string;
  principal: McpPrincipal;
  /** Refresh current grants before listing, execution and after approval. */
  refresh: () => Promise<void>;
  /** Return the existing execution owner's current instructions and tool descriptors. */
  describe: () => Promise<{
    instructions: string;
    tools: readonly MuleTool["descriptor"][];
  }>;
  /** Execute through Mycelium, applying context.access to operations and direct tools. */
  call: (
    call: McpToolCall,
    context: McpExecutionContext,
  ) => Promise<MuleToolResult>;
  /** Release the open connection; retained work belongs to the host. */
  disconnect: () => Promise<void>;
}>;

/** Host catalog adapter. All lookups and acquisitions are principal-bound. */
export type McpCatalogProvider = Readonly<{
  /** Atomically reserve a connection, or return null at the supplied bound. */
  open: (
    principal: McpPrincipal,
    input: Readonly<{
      id: string;
      title: string;
      /** Maximum concurrent sessions for this admitted principal. */
      maxOpenSessions: number;
    }>,
  ) => Promise<McpCatalogSession | null>;
  /** Return an open connection only; the MCP server also checks its principal. */
  resume: (
    principal: McpPrincipal,
    id: string,
  ) => Promise<McpCatalogSession | null>;
}>;

/** HTTP MCP lifecycle. Closing transport streams preserves host-owned Sessions. */
export type Mcp = Readonly<{
  /** Authenticate and serve one MCP HTTP request. */
  handleRequest: (request: Request) => Promise<Response>;
  /** End transport streams and refuse new requests; retained Sessions belong to the catalog. */
  close: () => Promise<void>;
}>;

/** Durable connection identity, without credentials or live execution callbacks. */
export type McpSessionMetadata = Readonly<{
  /** The host-owned catalog Session identity. */
  id: string;
  /** The complete admitted principal, including a member's role. */
  principal: McpPrincipal;
}>;

/** SDK-supported persistence; live protocol callbacks are never reconstructed. */
export type McpPersistence = Readonly<{
  /** Supply the SDK's event storage for one host-owned catalog Session. */
  eventStore: (sessionId: string) => EventStore;
  /** Persist canonical metadata after the SDK allocates its protocol session ID. */
  initialized: (id: string, session: McpSessionMetadata) => Promise<void>;
  /** Expire SDK metadata and events when the SDK closes that session. */
  closed: (id: string, session: McpSessionMetadata) => Promise<void>;
}>;

/** Serve the host's principal-bound Mycelium catalog through one MCP implementation. */
export function createMcp(
  options: Readonly<{
    catalog: McpCatalogProvider;
    authenticate: (request: Request) => Promise<McpAuthentication>;
    persistence?: McpPersistence;
  }>,
): Mcp {
  type Connection = {
    server: ProtocolServer;
    session: McpCatalogSession;
    transport: WebStandardStreamableHTTPServerTransport;
  };
  const live = new Map<string, Promise<Connection>>();
  const opening = new Set<Promise<Connection>>();
  let closed = false;
  const isClosed = () => closed;

  async function current(principal: McpPrincipal, id: string) {
    const session = await options.catalog.resume(principal, id);
    if (session?.id !== id || !sameMcpPrincipal(session.principal, principal))
      throw new Error("MCP connection is unavailable");
    await session.refresh();
    return session;
  }

  async function connect(session: McpCatalogSession, canElicit: boolean) {
    const { instructions } = await session.describe();
    const server = new ProtocolServer(
      { name: "mycelium", version: "1" },
      {
        capabilities: { tools: {} },
        instructions,
        // The durable execution remains suspended during a form elicitation.
        // Do not negotiate a protocol that requires replaying the program.
        supportedProtocolVersions: ["2025-11-25"],
      },
    );
    server.server.setRequestHandler("tools/list", async () => {
      try {
        const active = await current(session.principal, session.id);
        const { tools } = await active.describe();
        return {
          tools: tools.map((tool) => {
            const parsed = specTypeSchemas.Tool["~standard"].validate({
              name: tool.name,
              description: tool.description,
              inputSchema: tool.parameters,
            });
            if (parsed.issues !== undefined)
              throw new Error("Invalid MCP tool descriptor");
            return parsed.value;
          }),
        };
      } catch {
        throw new Error("MCP catalog is unavailable");
      }
    });
    server.server.setRequestHandler("tools/call", async (request, context) => {
      try {
        const active = await current(session.principal, session.id);
        const confirm = canElicit
          ? async (approval: ConfirmRequest, signal: AbortSignal) => {
              const answer = await context.mcpReq.send(
                {
                  method: "elicitation/create",
                  params: {
                    mode: "form",
                    message: `Allow ${approval.module}.${approval.operation} (${approval.access})?`,
                    requestedSchema: {
                      type: "object",
                      properties: {
                        approve: {
                          type: "boolean",
                          title: "Approve this operation",
                        },
                      },
                      required: ["approve"],
                    },
                  },
                },
                { signal },
              );
              return (
                answer.action === "accept" && answer.content?.approve === true
              );
            }
          : undefined;
        const access = {
          confirm,
          beforeCall: async (signal: AbortSignal) => {
            signal.throwIfAborted();
            const request = context.http?.req;
            if (request === undefined)
              throw new Error("MCP request is unavailable");
            const admission = await options.authenticate(request);
            if (
              admission.kind !== "admitted" ||
              !sameMcpPrincipal(admission.principal, session.principal)
            )
              throw new Error("MCP authority is unavailable");
            await current(session.principal, session.id);
            signal.throwIfAborted();
          },
        } satisfies McpExecutionContext["access"];
        let progress = 0;
        const progressToken = request.params._meta?.progressToken;
        const callOptions: McpExecutionContext = {
          signal: context.mcpReq.signal,
          access,
          onPart: async (part) => {
            if (progressToken === undefined) return;
            await context.mcpReq.notify({
              method: "notifications/progress",
              params: {
                progressToken,
                progress: ++progress,
                message: JSON.stringify(part),
              },
            });
          },
        };
        const result = await active.call(
          {
            id: nanoid(),
            name: request.params.name,
            arguments: Schema.decodeUnknownSync(
              Schema.Record(Schema.String, Schema.MutableJson),
            )(request.params.arguments ?? {}),
          },
          callOptions,
        );
        return { content: result.content, isError: result.isError };
      } catch {
        throw new Error("MCP tool call failed");
      }
    });
    const metadata: McpSessionMetadata = {
      id: session.id,
      principal: session.principal,
    };
    const transportOptions: WebStandardStreamableHTTPServerTransportOptions = {
      sessionIdGenerator: () => nanoid(),
      onsessioninitialized: async (id) => {
        await options.persistence?.initialized(id, metadata);
        live.set(id, Promise.resolve({ server, transport, session }));
      },
      onsessionclosed: async (id) => {
        live.delete(id);
        await session.disconnect();
        await options.persistence?.closed(id, metadata);
      },
    };
    if (options.persistence !== undefined)
      transportOptions.eventStore = options.persistence.eventStore(session.id);
    const transport = new WebStandardStreamableHTTPServerTransport(
      transportOptions,
    );
    await server.connect(transport);
    return { server, transport, session };
  }

  async function handleRequest(request: Request): Promise<Response> {
    const admission = await options.authenticate(request);
    if (admission.kind === "refused")
      return new Response("Access refused", {
        status: admission.status,
        headers:
          admission.retryAfter === undefined
            ? {}
            : { "Retry-After": String(admission.retryAfter) },
      });
    if (isClosed()) return new Response("MCP is closed", { status: 503 });
    const principal = admission.principal;
    const id = request.headers.get("Mcp-Session-Id");
    let session: McpCatalogSession | null;
    const created = id === null;
    let canElicit = false;
    if (created) {
      if (request.method !== "POST")
        return new Response("Not found", { status: 404 });
      let body: unknown;
      try {
        body = await request.clone().json();
      } catch {
        return new Response("Invalid initialization", { status: 400 });
      }
      const parsed =
        specTypeSchemas.InitializeRequest["~standard"].validate(body);
      if (parsed.issues !== undefined)
        return new Response("Invalid initialization", { status: 400 });
      canElicit =
        parsed.value.params.capabilities.elicitation?.form !== undefined;
      session = await options.catalog.open(principal, {
        id: nanoid(),
        title: `${parsed.value.params.clientInfo.name} · MCP`,
        maxOpenSessions: 8,
      });
      if (session === null)
        return new Response("Too many open MCP Sessions", { status: 429 });
    } else {
      const active = live.get(id);
      if (active === undefined)
        return new Response("Not found", { status: 404 });
      session = (await active).session;
    }
    if (!sameMcpPrincipal(session.principal, principal))
      return new Response("Not found", { status: 404 });
    if (isClosed()) {
      if (created) await session.disconnect();
      return new Response("MCP is closed", { status: 503 });
    }
    let active: Promise<Connection> | undefined;
    try {
      active =
        id === null
          ? (async () => {
              await session.refresh();
              return connect(session, canElicit);
            })()
          : live.get(id);
      if (active === undefined)
        return new Response("Not found", { status: 404 });
      if (created) opening.add(active);
      const connection = await active;
      if (isClosed()) return new Response("MCP is closed", { status: 503 });
      const response = await connection.transport.handleRequest(request);
      if (created && !response.ok) {
        await session.disconnect();
        await connection.server.close();
      }
      return response;
    } catch {
      if (id !== null && live.get(id) === active) live.delete(id);
      if (active !== undefined) {
        const [result] = await Promise.allSettled([active]);
        if (result.status === "fulfilled") await result.value.server.close();
      }
      if (created) await session.disconnect();
      return new Response("MCP is unavailable", { status: 503 });
    } finally {
      if (active !== undefined) opening.delete(active);
    }
  }

  return {
    async handleRequest(request) {
      try {
        return await handleRequest(request);
      } catch {
        return new Response("MCP is unavailable", { status: 503 });
      }
    },
    async close() {
      closed = true;
      const connections = [...new Set([...live.values(), ...opening])];
      live.clear();
      await Promise.all(
        connections.map(async (connection) =>
          (await connection).server.close(),
        ),
      );
    },
  };
}
