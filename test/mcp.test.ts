import {
  Client,
  StreamableHTTPClientTransport,
} from "@modelcontextprotocol/client";
import { z } from "zod";
import { Mycelium } from "../src/index.js";
import { directSandbox } from "../src/testing.js";
import { expect, it, vi } from "vitest";
import { createMcp, createMcpCatalog, type McpPrincipal } from "../src/mcp.js";

it("one principal cannot exhaust another principal's MCP session allowance", async () => {
  const mcp = createMcp({
    authenticate: (request) =>
      Promise.resolve({
        kind: "admitted",
        principal:
          request.headers.get("Authorization") === "owner"
            ? {
                actor: { kind: "member", memberId: "tenant-owner" },
                role: "owner",
              }
            : { actor: { kind: "team-key", keyId: "tenant-key" } },
      }),
    catalog: createMcpCatalog({
      load: () =>
        Promise.resolve({ instructions: "", sources: [], grants: {} }),
      sandbox: directSandbox(() => Promise.resolve(null)),
    }),
  });
  const clients: Client[] = [];
  const connect = async (authorization: string) => {
    const client = new Client({ name: "Session allowance", version: "1" });
    clients.push(client);
    const transport = new StreamableHTTPClientTransport(
      new URL("https://mcp.test/mcp"),
      {
        requestInit: { headers: { Authorization: authorization } },
        fetch: (input, init) => mcp.handleRequest(new Request(input, init)),
      },
    );
    await client.connect(transport);
    return { client, transport };
  };
  try {
    const first = await connect("key");
    for (let at = 1; at < 8; at++) await connect("key");
    const owner = await connect("owner");
    expect(
      (await owner.client.listTools()).tools.map(({ name }) => name),
    ).toEqual(["execute"]);
    await expect(connect("key")).rejects.toMatchObject({ status: 429 });
    await first.transport.terminateSession();
    const replacement = await connect("key");
    expect((await replacement.client.listTools()).tools).toHaveLength(1);
  } finally {
    await Promise.all(clients.map((client) => client.close()));
    await mcp.close();
  }
});

it.each(["demoted", "suspended"])(
  "refuses a member %s while the MCP approval prompt is open",
  async (change) => {
    let role: "owner" | "member" = "owner";
    let admitted = true;
    let calls = 0;
    let asked = 0;
    let changeAfterReplyAdmission = false;
    const module = Mycelium.module({
      id: "app",
      name: "app",
      operations: {
        write: Mycelium.operation({
          input: z.object({}),
          execute: () => {
            calls++;
            return Promise.resolve(true);
          },
        }),
      },
    });
    const mcp = createMcp({
      authenticate: (request) => {
        const principal: McpPrincipal = {
          actor: { kind: "member", memberId: "approving-member" },
          role,
        };
        const wasAdmitted = admitted;
        // Authority changes after the elicitation reply is admitted, before the
        // suspended operation resumes and checks authority again.
        if (changeAfterReplyAdmission && request.method === "POST") {
          changeAfterReplyAdmission = false;
          if (change === "demoted") role = "member";
          else admitted = false;
        }
        return Promise.resolve(
          wasAdmitted
            ? { kind: "admitted", principal }
            : { kind: "refused", status: 403 },
        );
      },
      catalog: createMcpCatalog({
        load: () =>
          Promise.resolve({
            instructions: "",
            grants: { app: "write" },
            sources: [
              {
                id: "apps",
                acquire: () =>
                  Promise.resolve({
                    snapshot: { version: "unchanged", modules: [module] },
                    release: () => Promise.resolve(),
                  }),
              },
            ],
          }),
        sandbox: directSandbox(async (bindings) => {
          const write = bindings.app?.write;
          if (write === undefined) throw new Error("Missing App write");
          return write({});
        }),
      }),
    });
    const client = new Client(
      { name: "Changing member", version: "1" },
      { capabilities: { elicitation: { form: {} } } },
    );
    client.setRequestHandler("elicitation/create", () => {
      asked++;
      changeAfterReplyAdmission = true;
      return Promise.resolve({ action: "accept", content: { approve: true } });
    });
    const transport = new StreamableHTTPClientTransport(
      new URL("https://mcp.test/mcp"),
      {
        fetch: (input, init) => mcp.handleRequest(new Request(input, init)),
      },
    );
    try {
      await client.connect(transport);
      await expect(
        client.callTool({ name: "execute", arguments: { code: "write" } }),
      ).rejects.toThrow("MCP tool call failed");
      expect(asked).toBe(1);
      expect(calls).toBe(0);
    } finally {
      await client.close();
      await mcp.close();
    }
  },
);

it("refuses unauthenticated requests before acquiring any catalog", async () => {
  const open = vi.fn();
  const resume = vi.fn();
  const mcp = createMcp({
    catalog: { open, resume },
    authenticate: () => Promise.resolve({ kind: "refused", status: 401 }),
  });
  try {
    for (const method of ["POST", "GET", "DELETE"]) {
      const response = await mcp.handleRequest(
        new Request("https://mcp.test/mcp", { method }),
      );
      expect(response.status).toBe(401);
    }
    expect(open).not.toHaveBeenCalled();
    expect(resume).not.toHaveBeenCalled();
  } finally {
    await mcp.close();
  }
});

it("serves a principal-scoped source catalog and refuses an unapproved write", async () => {
  const written: string[] = [];
  const principals: string[] = [];
  let admitted = true;
  let subject = "key:one";
  const notes = Mycelium.module({
    id: "notes",
    name: "notes",
    operations: {
      write: Mycelium.operation({
        input: z.object({ text: z.string() }),
        execute: ({ text }) => {
          written.push(text);
          return Promise.resolve(true);
        },
      }),
    },
  });
  const catalog = createMcpCatalog({
    load: (principal) => {
      if (principal.actor.kind === "team-key")
        principals.push(principal.actor.keyId);
      return Promise.resolve({
        instructions: "Use the installed Apps.",
        grants: { notes: "write" },
        sources: [
          {
            id: "installed-apps",
            acquire: () =>
              Promise.resolve({
                snapshot: { version: "v1", modules: [notes] },
                release: () => Promise.resolve(),
              }),
          },
        ],
      });
    },
    sandbox: directSandbox(async (bindings) => {
      const write = bindings.notes?.write;
      if (write === undefined) throw new Error("Missing write");
      return await write({ text: "requires approval" });
    }),
  });
  const mcp = createMcp({
    catalog,
    authenticate: () =>
      Promise.resolve(
        admitted
          ? {
              kind: "admitted",
              principal: { actor: { kind: "team-key", keyId: subject } },
            }
          : { kind: "refused", status: 401 },
      ),
  });
  const client = new Client({ name: "Oyster consumer", version: "1" });
  const transport = new StreamableHTTPClientTransport(
    new URL("https://mcp.test/mcp"),
    {
      fetch: (input, init) => mcp.handleRequest(new Request(input, init)),
    },
  );
  try {
    await client.connect(transport);
    expect(client.getInstructions()).toBe("Use the installed Apps.");
    expect((await client.listTools()).tools.map(({ name }) => name)).toEqual([
      "execute",
    ]);
    const result = await client.callTool({
      name: "execute",
      arguments: { code: "write" },
    });
    expect(written).toEqual([]);
    expect(JSON.stringify(result.content)).toContain(
      "confirmation-unavailable",
    );
    expect(principals.length).toBeGreaterThan(0);
    expect(new Set(principals)).toEqual(new Set(["key:one"]));
    subject = "key:two";
    const sessionId = transport.sessionId;
    if (sessionId === undefined) throw new Error("Missing MCP Session");
    expect(
      (
        await mcp.handleRequest(
          new Request("https://mcp.test/mcp", {
            method: "DELETE",
            headers: { "Mcp-Session-Id": sessionId },
          }),
        )
      ).status,
    ).toBe(404);
    subject = "key:one";
    expect((await client.listTools()).tools).toHaveLength(1);
    admitted = false;
    await expect(client.listTools()).rejects.toBeDefined();
  } finally {
    await client.close();
    await mcp.close();
  }
});

it.each(["accepted", "declined", "revoked", "ungranted"] as const)(
  "checks %s approval and current grants before a write",
  async (decision) => {
    const written: string[] = [];
    let grant: "write" | "none" = "write";
    const module = Mycelium.module({
      id: "app",
      name: "app",
      operations: {
        write: Mycelium.operation({
          input: z.object({}),
          execute: () => {
            written.push("changed");
            return Promise.resolve("done");
          },
        }),
      },
    });
    const catalog = createMcpCatalog({
      load: () =>
        Promise.resolve({
          instructions: "Installed App",
          grants: decision === "ungranted" ? {} : { app: grant },
          sources: [
            {
              id: "apps",
              acquire: () =>
                Promise.resolve({
                  snapshot: { version: "1", modules: [module] },
                  release: () => Promise.resolve(),
                }),
            },
          ],
        }),
      sandbox: directSandbox(async (bindings) => {
        const write = bindings.app?.write;
        if (write === undefined) throw new Error("Missing App");
        return [await write({}), await write({})];
      }),
    });
    const mcp = createMcp({
      catalog,
      authenticate: () =>
        Promise.resolve({
          kind: "admitted",
          principal: {
            actor: { kind: "member", memberId: "owner:one" },
            role: "owner",
          },
        }),
    });
    const client = new Client(
      { name: "Approving client", version: "1" },
      {
        capabilities: { elicitation: { form: {} } },
      },
    );
    let asked = 0;
    client.setRequestHandler("elicitation/create", () => {
      asked++;
      expect(written).toEqual(
        decision === "accepted" && asked === 2 ? ["changed"] : [],
      );
      if (decision === "revoked") grant = "none";
      return Promise.resolve(
        decision === "declined"
          ? { action: "decline" }
          : { action: "accept", content: { approve: true } },
      );
    });
    const transport = new StreamableHTTPClientTransport(
      new URL("https://mcp.test/mcp"),
      {
        fetch: (input, init) => mcp.handleRequest(new Request(input, init)),
      },
    );
    try {
      await client.connect(transport);
      const result = await client.callTool({
        name: "execute",
        arguments: { code: "write" },
      });
      expect(asked).toBe(
        decision === "ungranted" ? 0 : decision === "revoked" ? 1 : 2,
      );
      expect(written).toEqual(
        decision === "accepted" ? ["changed", "changed"] : [],
      );
      expect(JSON.stringify(result.content)).toContain(
        decision === "accepted"
          ? "done"
          : decision === "declined"
            ? "declined"
            : "not-granted",
      );
    } finally {
      await client.close();
      await mcp.close();
    }
  },
);

it.each(["approved", "unapproved", "author-confirm", "withdrawn"] as const)(
  "a headless client that cancels every elicitation: %s write",
  async (scenario) => {
    // `claude -p` advertises a bare `elicitation: {}` and answers every
    // elicitation with `cancel` when no SDK host or hook can ask a person.
    const written: string[] = [];
    let approvedWrites: readonly string[] =
      scenario === "unapproved" ? [] : ["app"];
    const module = Mycelium.module({
      id: "app",
      name: "app",
      operations: {
        write: Mycelium.operation({
          confirm: scenario === "author-confirm",
          input: z.object({}),
          execute: () => {
            written.push("changed");
            if (scenario === "withdrawn") approvedWrites = [];
            return Promise.resolve("done");
          },
        }),
      },
    });
    const catalog = createMcpCatalog({
      load: () =>
        Promise.resolve({
          instructions: "Installed App",
          grants: { app: "write" },
          approvedWrites,
          sources: [
            {
              id: "apps",
              acquire: () =>
                Promise.resolve({
                  snapshot: { version: "1", modules: [module] },
                  release: () => Promise.resolve(),
                }),
            },
          ],
        }),
      sandbox: directSandbox(async (bindings) => {
        const write = bindings.app?.write;
        if (write === undefined) throw new Error("Missing App");
        return [await write({}), await write({})];
      }),
    });
    const mcp = createMcp({
      catalog,
      authenticate: () =>
        Promise.resolve({
          kind: "admitted",
          principal: { actor: { kind: "team-key", keyId: "agent-key" } },
        }),
    });
    const client = new Client(
      { name: "claude-code", version: "2.1.299" },
      { capabilities: { elicitation: {} } },
    );
    let asked = 0;
    client.setRequestHandler("elicitation/create", () => {
      asked++;
      return Promise.resolve({ action: "cancel" });
    });
    const transport = new StreamableHTTPClientTransport(
      new URL("https://mcp.test/mcp"),
      {
        fetch: (input, init) => mcp.handleRequest(new Request(input, init)),
      },
    );
    try {
      await client.connect(transport);
      const result = await client.callTool({
        name: "execute",
        arguments: { code: "write" },
      });
      const content = JSON.stringify(result.content);
      if (scenario === "approved") {
        expect(asked).toBe(0);
        expect(written).toEqual(["changed", "changed"]);
        expect(content).toContain("done");
      } else if (scenario === "withdrawn") {
        // The next operation reloads authority and no longer finds approval.
        expect(asked).toBe(1);
        expect(written).toEqual(["changed"]);
        expect(content).toContain("declined");
      } else {
        expect(asked).toBe(2);
        expect(written).toEqual([]);
        expect(content).toContain("declined");
      }
    } finally {
      await client.close();
      await mcp.close();
    }
  },
);

it("refuses a cold protocol session rather than fabricating a transport from a catalog Session", async () => {
  const principal = { actor: { kind: "team-key", keyId: "key" } } as const;
  let release: (() => void) | undefined;
  const ready = new Promise<void>((resolve) => {
    release = resolve;
  });
  const refresh = vi.fn(() => ready);
  const describe = vi.fn(() =>
    Promise.resolve({ instructions: "Resume", tools: [] }),
  );
  const session = {
    id: "existing",
    principal,
    refresh,
    describe,
    call: () => Promise.reject(new Error("No tools")),
    disconnect: () => Promise.resolve(),
  };
  const mcp = createMcp({
    authenticate: () => Promise.resolve({ kind: "admitted", principal }),
    catalog: {
      open: () => Promise.resolve(null),
      resume: () => Promise.resolve(session),
    },
  });
  const request = (id: number) =>
    new Request("https://mcp.test/mcp", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json, text/event-stream",
        "Mcp-Session-Id": session.id,
        "MCP-Protocol-Version": "2025-11-25",
      },
      body: JSON.stringify({ jsonrpc: "2.0", id, method: "tools/list" }),
    });
  try {
    const pending = Promise.all([
      mcp.handleRequest(request(1)),
      mcp.handleRequest(request(2)),
    ]);
    release?.();
    const responses = await pending;
    expect(responses.map(({ status }) => status)).toEqual([404, 404]);
    expect(refresh).not.toHaveBeenCalled();
    expect(describe).not.toHaveBeenCalled();
  } finally {
    release?.();
    await mcp.close();
  }
});

it("does not restore stale grants when concurrent catalog refreshes finish out of order", async () => {
  const read = vi.fn(() => Promise.resolve("private content"));
  const module = Mycelium.module({
    id: "app",
    name: "app",
    operations: {
      read: Mycelium.operation({
        access: "read",
        input: z.object({}),
        execute: read,
      }),
    },
  });
  const sources = [
    {
      id: "apps",
      acquire: () =>
        Promise.resolve({
          snapshot: { version: "1", modules: [module] },
          release: () => Promise.resolve(),
        }),
    },
  ];
  const allowed = {
    instructions: "App",
    sources,
    grants: { app: "write" as const },
  };
  const refused = {
    instructions: "App",
    sources,
    grants: { app: "none" as const },
  };
  let releaseOld: ((snapshot: typeof allowed) => void) | undefined;
  const old = new Promise<typeof allowed>((resolve) => {
    releaseOld = resolve;
  });
  let loads = 0;
  const catalog = createMcpCatalog({
    load: () => {
      loads++;
      if (loads === 1) return Promise.resolve(allowed);
      if (loads === 2) return old;
      if (loads === 4)
        return Promise.resolve(refused).then((snapshot) => {
          // Complete the older storage read while the current authority refresh unwinds.
          queueMicrotask(() => {
            releaseOld?.(allowed);
          });
          return snapshot;
        });
      return Promise.resolve(refused);
    },
    sandbox: directSandbox(async (bindings) => {
      const operation = bindings.app?.read;
      if (operation === undefined) throw new Error("Missing App");
      return operation({});
    }),
  });
  const mcp = createMcp({
    catalog,
    authenticate: () =>
      Promise.resolve({
        kind: "admitted",
        principal: {
          actor: { kind: "member", memberId: "owner" },
          role: "owner",
        },
      }),
  });
  const client = new Client({
    name: "Concurrent authority witness",
    version: "1",
  });
  const transport = new StreamableHTTPClientTransport(
    new URL("https://mcp.test/mcp"),
    {
      fetch: (input, init) => mcp.handleRequest(new Request(input, init)),
    },
  );
  try {
    await client.connect(transport);
    const listing = client.listTools();
    await vi.waitFor(() => {
      expect(loads).toBe(2);
    });
    const result = await client.callTool({
      name: "execute",
      arguments: { code: "read" },
    });
    await listing;
    expect(read).not.toHaveBeenCalled();
    expect(JSON.stringify(result.content)).toContain("not-granted");
  } finally {
    releaseOld?.(allowed);
    await client.close();
    await mcp.close();
  }
});

it("waits for a pending initialization to close its transport", async () => {
  const principal = { actor: { kind: "team-key", keyId: "key" } } as const;
  let release: (() => void) | undefined;
  const ready = new Promise<void>((resolve) => {
    release = resolve;
  });
  const refresh = vi.fn(() => ready);
  const mcp = createMcp({
    authenticate: () => Promise.resolve({ kind: "admitted", principal }),
    catalog: {
      open: (_principal, { id }) =>
        Promise.resolve({
          id,
          principal,
          refresh,
          describe: () =>
            Promise.resolve({ instructions: "Initialize", tools: [] }),
          call: () => Promise.reject(new Error("No tools")),
          disconnect: () => Promise.resolve(),
        }),
      resume: () => Promise.resolve(null),
    },
  });
  const pending = mcp.handleRequest(
    new Request("https://mcp.test/mcp", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json, text/event-stream",
      },
      body: JSON.stringify({
        jsonrpc: "2.0",
        id: 1,
        method: "initialize",
        params: {
          protocolVersion: "2025-11-25",
          capabilities: {},
          clientInfo: { name: "closing", version: "1" },
        },
      }),
    }),
  );
  await vi.waitFor(() => {
    expect(refresh).toHaveBeenCalled();
  });
  let closed = false;
  const closing = mcp.close().then(() => {
    closed = true;
  });
  try {
    await new Promise<void>((resolve) => {
      setTimeout(resolve, 0);
    });
    expect(closed).toBe(false);
  } finally {
    release?.();
    await pending;
    await closing;
    await mcp.close();
  }
});

it("rechecks the calling credential between operations in one program", async () => {
  let admitted = true;
  let calls = 0;
  const module = Mycelium.module({
    id: "app",
    name: "app",
    operations: {
      read: Mycelium.operation({
        access: "read",
        input: z.object({}),
        execute: () => {
          calls++;
          admitted = false;
          return Promise.resolve("first");
        },
      }),
    },
  });
  const mcp = createMcp({
    authenticate: () =>
      Promise.resolve(
        admitted
          ? {
              kind: "admitted",
              principal: { actor: { kind: "team-key", keyId: "revocable" } },
            }
          : { kind: "refused", status: 401 },
      ),
    catalog: createMcpCatalog({
      load: () =>
        Promise.resolve({
          instructions: "Read App",
          grants: { app: "read" },
          sources: [
            {
              id: "apps",
              acquire: () =>
                Promise.resolve({
                  snapshot: { version: "1", modules: [module] },
                  release: () => Promise.resolve(),
                }),
            },
          ],
        }),
      sandbox: directSandbox(async (bindings) => {
        const read = bindings.app?.read;
        if (read === undefined) throw new Error("Missing App");
        return [await read({}), await read({})];
      }),
    }),
  });
  const client = new Client({ name: "Revocable key", version: "1" });
  const transport = new StreamableHTTPClientTransport(
    new URL("https://mcp.test/mcp"),
    {
      fetch: (input, init) => mcp.handleRequest(new Request(input, init)),
    },
  );
  try {
    await client.connect(transport);
    await expect(
      client.callTool({
        name: "execute",
        arguments: { code: "read twice" },
      }),
    ).rejects.toThrow("MCP tool call failed");
    expect(calls).toBe(1);
  } finally {
    await client.close();
    await mcp.close();
  }
});

it("binds a canonical MCP session to its actor and member role", async () => {
  const principal = {
    actor: { kind: "member", memberId: "member-one" },
    role: "owner",
  } as const;
  const catalog = createMcpCatalog({
    load: () => Promise.resolve({ instructions: "", sources: [], grants: {} }),
    sandbox: directSandbox(() => Promise.resolve(null)),
  });
  const session = await catalog.open(principal, {
    id: "canonical-session",
    title: "Tenant member",
    maxOpenSessions: 1,
  });
  expect(session).not.toBeNull();
  expect(await catalog.resume({ ...principal }, "canonical-session")).toBe(
    session,
  );
  const changed = [
    { actor: { kind: "member", memberId: "member-one" }, role: "member" },
    { actor: { kind: "member", memberId: "member-two" }, role: "owner" },
    { actor: { kind: "agent", agentId: "member-one" } },
    { actor: { kind: "team-key", keyId: "member-one" } },
  ] as const;
  let current: McpPrincipal = principal;
  const mcp = createMcp({
    authenticate: () =>
      Promise.resolve({ kind: "admitted", principal: current }),
    catalog,
  });
  const client = new Client({ name: "Canonical member", version: "1" });
  const transport = new StreamableHTTPClientTransport(
    new URL("https://mcp.test/mcp"),
    {
      fetch: (input, init) => mcp.handleRequest(new Request(input, init)),
    },
  );
  try {
    await client.connect(transport);
    expect(transport.sessionId).toBeTruthy();
    expect((await client.listTools()).tools).toHaveLength(1);
    for (const next of changed) {
      expect(await catalog.resume(next, "canonical-session")).toBeNull();
      current = next;
      await expect(client.listTools()).rejects.toMatchObject({ status: 404 });
      current = principal;
      expect((await client.listTools()).tools).toHaveLength(1);
    }
    await transport.terminateSession();
  } finally {
    await client.close();
    await mcp.close();
    await session?.disconnect();
  }
});

it.each([404, 429] as const)(
  "preserves MCP auth refusal %s and its Retry-After hint before catalog acquisition",
  async (status) => {
    const open = vi.fn();
    const mcp = createMcp({
      catalog: { open, resume: vi.fn() },
      authenticate: () =>
        Promise.resolve({ kind: "refused", status, retryAfter: 9 }),
    });
    try {
      const response = await mcp.handleRequest(
        new Request("https://mcp.test/mcp", { method: "POST" }),
      );
      expect(response.status).toBe(status);
      expect(response.headers.get("Retry-After")).toBe("9");
      expect(await response.text()).toBe("Access refused");
      expect(open).not.toHaveBeenCalled();
    } finally {
      await mcp.close();
    }
  },
);
