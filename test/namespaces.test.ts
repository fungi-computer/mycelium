import { directSandbox } from "@fungi.computer/mycelium/testing";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import { expect, it } from "vitest";
import { z } from "zod";
import {
  Mycelium,
  MyceliumDefinitionError,
  type GateDecision,
} from "../src/index.js";
import {
  sandboxGuestProgram,
  sandboxNamespacePathsSchema,
} from "../src/sandbox-guest.js";

it("compiles deep child modules under one authority and gates their relative operation paths", async () => {
  const asked: string[] = [];
  const ran: string[] = [];
  let decision: GateDecision = { kind: "allow" };
  const namespace = ["apps", "personal"];
  const leaf = Mycelium.operation({
    input: z.object({ value: z.string() }),
    output: z.object({ value: z.string() }),
    execute: ({ value }) => {
      ran.push(value);
      return Promise.resolve({ value });
    },
  });
  const module = Mycelium.gate(
    Mycelium.module({
      id: "authority:one",
      name: "bazaar",
      namespace,
      operations: {
        ping: leaf,
        windows: Mycelium.module({
          id: "presentation-only",
          namespace: ["views"],
          operations: {
            panels: Mycelium.module({
              id: "panel-group",
              operations: { open: leaf },
            }),
          },
        }),
      },
    }),
    ({ operation }) => {
      asked.push(operation);
      return decision;
    },
  );
  namespace.push("mustNotRetarget");
  const runtime = await Mycelium.make({
    modules: [module],
    sandbox: {
      execute: async (request) => {
        expect(request.modules).toEqual({ bazaar: "authority:one" });
        expect(request.namespacePaths).toEqual({
          bazaar: ["apps", "personal", "bazaar"],
        });
        const open = request.bindings.bazaar?.["windows.views.panels.open"];
        const ping = request.bindings.bazaar?.ping;
        if (open === undefined || ping === undefined)
          throw new Error("Missing compiled operations");
        expect(await open({ value: "deep" })).toEqual({ value: "deep" });
        expect(await ping({ value: "flat" })).toEqual({ value: "flat" });
        decision = { kind: "refuse", value: { refused: true } };
        expect(await open({ malformed: true })).toEqual({ refused: true });
        return null;
      },
    },
  });
  const lease = await runtime.acquire();
  try {
    expect(
      lease.catalog
        .list({ namespace: "apps.personal.bazaar.windows" })
        .results.map((item) => item.path),
    ).toEqual(["apps.personal.bazaar.windows.views.panels.open"]);
    const described = lease.catalog.describe({
      path: "apps.personal.bazaar.windows.views.panels",
      methods: ["open"],
    });
    expect(described.types).toContain('["apps"]');
    expect(described.types).toContain('["panels"]');
    expect(described.types).toContain(
      "Promise<apps$personal$bazaar$windows$views$panels$open_Output>",
    );
    expect(described.types).not.toContain("bazaar$ping_Input");
    expect(
      lease.catalog.describe({
        path: "apps.personal.bazaar.windows.views.panels.open",
      }).usage,
    ).toBe("await env.apps.personal.bazaar.windows.views.panels.open({ ... })");
    const prepared = await Effect.runPromise(
      lease.executeTool.prepare({
        type: "toolCall",
        id: "nested",
        name: "execute",
        arguments: { code: "return null" },
      }),
    );
    const parts = await Effect.runPromise(
      Stream.runCollect(prepared.execute()),
    );
    expect(parts).toEqual([
      {
        type: "result",
        result: { content: [{ type: "text", text: "null" }], isError: false },
      },
    ]);
    expect(asked).toEqual([
      "windows.views.panels.open",
      "ping",
      "windows.views.panels.open",
    ]);
    expect(ran).toEqual(["deep", "flat"]);
  } finally {
    await lease.release();
    await runtime.close();
  }
});

it("rejects malformed namespace metadata before interpolating guest source", () => {
  expect(
    sandboxNamespacePathsSchema.safeParse({ app: ["apps", "bad.name"] })
      .success,
  ).toBe(false);
  expect(() =>
    sandboxGuestProgram("async () => null", { default: ["read"] }),
  ).toThrow("Invalid sandbox transport alias");
  expect(() =>
    sandboxGuestProgram("async () => null", {
      "x); throw new Error()": ["read"],
    }),
  ).toThrow();
  expect(() =>
    sandboxGuestProgram("async () => null", { app: ["files", "files.read"] }),
  ).toThrow("Operation conflicts with namespace");
  expect(() =>
    Mycelium.module({ id: "bad", namespace: ["bad.name"], operations: {} }),
  ).toThrow(MyceliumDefinitionError);
});

it("rejects collisions with intrinsic namespace leaves at installation", async () => {
  await expect(
    Mycelium.make({
      modules: [
        Mycelium.module({
          id: "collision",
          name: "list",
          namespace: ["forage"],
          operations: {
            read: Mycelium.operation({
              input: z.object({}),
              execute: () => Promise.resolve(null),
            }),
          },
        }),
      ],
      sandbox: directSandbox(() => Promise.resolve(null)),
    }),
  ).rejects.toBeInstanceOf(MyceliumDefinitionError);
});

it("keeps logical names independent across namespaces while transport aliases stay unique", async () => {
  const operation = Mycelium.operation({
    input: z.object({}),
    execute: () => Promise.resolve(null),
  });
  const runtime = await Mycelium.make({
    modules: ["apps", "computers"].map((namespace) =>
      Mycelium.module({
        id: namespace,
        name: "personal",
        namespace: [namespace],
        operations: { read: operation },
      }),
    ),
    sandbox: directSandbox(() => Promise.resolve(null)),
  });
  const lease = await runtime.acquire();
  try {
    expect(
      lease.catalog.list({ namespace: "apps" }).results.map(({ path }) => path),
    ).toEqual(["apps.personal.read"]);
    expect(
      lease.catalog
        .list({ namespace: "computers" })
        .results.map(({ path }) => path),
    ).toEqual(["computers.personal.read"]);
  } finally {
    await lease.release();
    await runtime.close();
  }
});

it("compiles dotted operation keys as nested guest paths and refuses a key that is also a namespace", async () => {
  const leaf = (value: string) =>
    Mycelium.operation({
      input: z.object({}),
      output: z.string(),
      execute: () => Promise.resolve(value),
    });
  const runtime = await Mycelium.make({
    modules: [
      Mycelium.module({
        id: "app:bonsai",
        name: "BONSAI",
        operations: {
          "repos.list": leaf("listed"),
          "files.history.read": leaf("history"),
        },
      }),
    ],
    sandbox: {
      execute: async (request) => {
        expect(request.namespacePaths).toEqual({ BONSAI: ["BONSAI"] });
        expect(Object.keys(request.bindings.BONSAI ?? {}).sort()).toEqual([
          "files.history.read",
          "repos.list",
        ]);
        const list = request.bindings.BONSAI?.["repos.list"];
        if (list === undefined) throw new Error("Missing dotted operation");
        expect(await list({})).toBe("listed");
        return null;
      },
    },
  });
  const lease = await runtime.acquire();
  try {
    expect(lease.catalog.describe({ path: "BONSAI.repos.list" }).usage).toBe(
      "await env.BONSAI.repos.list({ ... })",
    );
    const { types } = lease.catalog.describe({ path: "BONSAI" });
    expect(types).toContain('["repos"]: { ["list"]:');
    expect(types).not.toContain('"repos.list"');
    const prepared = await Effect.runPromise(
      lease.executeTool.prepare({
        type: "toolCall",
        id: "dotted",
        name: "execute",
        arguments: { code: "return null" },
      }),
    );
    await Effect.runPromise(Stream.runCollect(prepared.execute()));
  } finally {
    await lease.release();
    await runtime.close();
  }
  for (const operations of [
    { files: leaf("a"), "files.read": leaf("b") },
    { "files.read": leaf("b"), files: leaf("a") },
  ])
    await expect(
      Mycelium.make({
        modules: [
          Mycelium.module({ id: "app:collide", name: "COLLIDE", operations }),
        ],
        sandbox: directSandbox(() => Promise.resolve(null)),
      }),
    ).rejects.toThrow("Operation conflicts with namespace: COLLIDE.files.read");
  for (const key of [
    "repos..list",
    "repos.",
    ".list",
    "repos.1list",
    "repos-list",
  ])
    await expect(
      Mycelium.make({
        modules: [
          Mycelium.module({
            id: "app:bad",
            name: "BAD",
            operations: { [key]: leaf("x") },
          }),
        ],
        sandbox: directSandbox(() => Promise.resolve(null)),
      }),
    ).rejects.toBeInstanceOf(MyceliumDefinitionError);
});
