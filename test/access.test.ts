import { directSandbox } from "@fungi.computer/mycelium/testing";
import { Type, type MuleTool } from "@fungi.computer/mule/effect";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import { describe, expect, it } from "vitest";
import { z } from "zod";

import {
  Mycelium,
  MyceliumDefinitionError,
  type AccessOptions,
  type AccessRefusal,
  type ConfirmRequest,
  type Grants,
  type LabelledTool,
  type SandboxValue,
} from "../src/index.js";

const refusalSchema = z.object({
  refused: z.literal(true),
  module: z.string(),
  operation: z.string(),
  reason: z.enum([
    "not-granted",
    "read-only",
    "confirmation-unavailable",
    "declined",
  ]),
  message: z.string(),
});

it("requires approval for each MCP write, including unlabelled operations", async () => {
  const ran: string[] = [];
  const results = await callAll(
    { confirmWrites: true },
    [
      { operation: "read", input: { text: "visible" } },
      { operation: "write", input: { text: "must not run" } },
    ],
    ran,
  );
  expect(ran).toEqual(["read:visible"]);
  expect(reasonOf(results[1])).toBe("confirmation-unavailable");
});

it("refreshes authority after approval before executing the write", async () => {
  const ran: string[] = [];
  let approved = false;
  let grant: "write" | "none" = "write";
  const results = await callAll(
    {
      confirmWrites: true,
      sessionGrants: () => ({ notes: grant }),
      beforeCall: () => {
        if (approved) grant = "none";
        return Promise.resolve();
      },
      confirm: () => {
        approved = true;
        return Promise.resolve(true);
      },
    },
    [{ operation: "write", input: { text: "revoked while deciding" } }],
    ran,
  );
  expect(ran).toEqual([]);
  expect(reasonOf(results[0])).toBe("not-granted");
});

/** The refusal reason a refused call returned. */
function reasonOf(result: SandboxValue): AccessRefusal["reason"] {
  return refusalSchema.parse(result).reason;
}

/** A module with one read, one write (unlabelled) and one confirm operation. */
function notes(ran: string[]) {
  const record = (name: string) => (input: { text: string }) => {
    ran.push(`${name}:${input.text}`);
    return Promise.resolve({ ok: name });
  };
  return Mycelium.module({
    id: "notes",
    name: "notes",
    operations: {
      read: Mycelium.operation({
        access: "read",
        input: z.object({ text: z.string() }),
        execute: record("read"),
      }),
      write: Mycelium.operation({
        input: z.object({ text: z.string() }),
        execute: record("write"),
      }),
      send: Mycelium.operation({
        access: "write",
        confirm: true,
        input: z.object({ text: z.string() }),
        execute: record("send"),
      }),
    },
  });
}

/** Call each named `notes` operation once through execute; collect the results. */
async function callAll(
  access: AccessOptions,
  calls: readonly Readonly<{ operation: string; input: SandboxValue }>[],
  ran: string[],
): Promise<SandboxValue[]> {
  const results: SandboxValue[] = [];
  const runtime = await Mycelium.make({
    modules: [notes(ran)],
    access,
    sandbox: directSandbox(async (bindings) => {
      for (const { operation, input } of calls) {
        const bound = bindings.notes?.[operation];
        if (bound === undefined) throw new Error(`${operation} missing`);
        results.push(await bound(input));
      }
      return null;
    }),
  });
  const lease = await runtime.acquire();
  try {
    const prepared = await Effect.runPromise(
      lease.executeTool.prepare({
        type: "toolCall",
        id: "call",
        name: "execute",
        arguments: { code: "ignored by the direct sandbox" },
      }),
    );
    await Effect.runPromise(Stream.runCollect(prepared.execute()));
    return results;
  } finally {
    await lease.release();
    await runtime.close();
  }
}

const each = (input: SandboxValue) =>
  ["read", "write", "send"].map((operation) => ({ operation, input }));

describe("operation access labels and host grants", () => {
  it("runs read and write operations with no grants, and refuses confirm with no handler", async () => {
    const ran: string[] = [];
    const results = await callAll({}, each({ text: "a" }), ran);
    expect(ran).toEqual(["read:a", "write:a"]);
    expect(results[2]).toEqual({
      refused: true,
      module: "notes",
      operation: "send",
      reason: "confirmation-unavailable",
      message:
        "notes.send needs a person's approval, and no one can be asked here.",
    });
  });

  it("refuses a write operation under a read grant without running or decoding it", async () => {
    const ran: string[] = [];
    const refusals: AccessRefusal[] = [];
    const results = await callAll(
      {
        grants: { notes: "read" },
        onRefused: (refusal) => refusals.push(refusal),
      },
      [
        { operation: "read", input: { text: "a" } },
        // Malformed input proves the refusal happens before the decoder.
        { operation: "write", input: { wrong: 1 } },
      ],
      ran,
    );
    expect(ran).toEqual(["read:a"]);
    expect(results[1]).toEqual({
      refused: true,
      module: "notes",
      operation: "write",
      reason: "read-only",
      message:
        "notes.write changes something, and this Agent has read-only access to notes.",
    });
    expect(refusals).toEqual([results[1]]);
  });

  it("refuses every operation under a none grant, and treats unknown grant values as none", async () => {
    const malformed: Grants = {};
    // A host value outside the Grant set must fail closed, not open.
    Object.assign(malformed, { notes: "admin" });
    for (const grants of [{ notes: "none" } as const, malformed]) {
      const ran: string[] = [];
      const results = await callAll({ grants }, each({ text: "a" }), ran);
      expect(ran).toEqual([]);
      expect(results.map(reasonOf)).toEqual([
        "not-granted",
        "not-granted",
        "not-granted",
      ]);
    }
  });

  it("gives a module the host does not name full access, ignoring inherited keys", async () => {
    const ran: string[] = [];
    const prototype: Grants = { notes: "none" };
    const inherited: Grants = { other: "none" };
    Object.setPrototypeOf(inherited, prototype);
    await callAll({ grants: inherited }, each({ text: "a" }).slice(0, 2), ran);
    expect(ran).toEqual(["read:a", "write:a"]);
  });

  it("refuses a required module that no grant names, and keeps unrequired ones open", async () => {
    const ran: string[] = [];
    const results = await callAll(
      { required: ["notes"], grants: { other: "write" } },
      each({ text: "a" }).slice(0, 2),
      ran,
    );
    expect(ran).toEqual([]);
    expect(results.map(reasonOf)).toEqual(["not-granted", "not-granted"]);
    const delivered: string[] = [];
    await callAll(
      { required: ["notes"], sessionGrants: () => ({ notes: "read" }) },
      each({ text: "b" }).slice(0, 2),
      delivered,
    );
    expect(delivered).toEqual(["read:b"]);
    const open: string[] = [];
    await callAll(
      { required: ["other"] },
      each({ text: "c" }).slice(0, 2),
      open,
    );
    expect(open).toEqual(["read:c", "write:c"]);
  });

  it("lets Session grants override the default grants key by key", async () => {
    const ran: string[] = [];
    const contexts: (string | null)[] = [];
    await callAll(
      {
        grants: { notes: "none", other: "none" },
        sessionGrants: ({ sessionId }) => {
          contexts.push(sessionId);
          return { notes: "read" };
        },
      },
      each({ text: "a" }).slice(0, 2),
      ran,
    );
    expect(ran).toEqual(["read:a"]);
    expect(contexts).toEqual([null, null]);
  });

  it("reads Session grants at every call, so a revoked grant applies to the next call", async () => {
    const ran: string[] = [];
    let grant: "write" | "none" = "write";
    const results: SandboxValue[] = [];
    const runtime = await Mycelium.make({
      modules: [notes(ran)],
      access: { sessionGrants: () => ({ notes: grant }) },
      sandbox: directSandbox(async (bindings) => {
        results.push(await bindings.notes?.write?.({ text: "first" }));
        grant = "none";
        results.push(await bindings.notes?.write?.({ text: "second" }));
        return null;
      }),
    });
    const lease = await runtime.acquire();
    try {
      const prepared = await Effect.runPromise(
        lease.executeTool.prepare({
          type: "toolCall",
          id: "call",
          name: "execute",
          arguments: { code: "" },
        }),
      );
      await Effect.runPromise(Stream.runCollect(prepared.execute()));
    } finally {
      await lease.release();
      await runtime.close();
    }
    expect(ran).toEqual(["write:first"]);
    expect(reasonOf(results[1])).toBe("not-granted");
  });

  it("asks the confirm handler with the requested input, and runs only on approval", async () => {
    const asked: ConfirmRequest[] = [];
    let answer = true;
    const ran: string[] = [];
    const results = await callAll(
      {
        confirm: (request) => {
          asked.push(request);
          const approved = answer;
          answer = false;
          return Promise.resolve(approved);
        },
      },
      [
        { operation: "send", input: { text: "yes" } },
        { operation: "send", input: { text: "no" } },
      ],
      ran,
    );
    expect(asked).toEqual([
      {
        module: "notes",
        operation: "send",
        access: "write",
        sessionId: null,
        input: { text: "yes" },
      },
      {
        module: "notes",
        operation: "send",
        access: "write",
        sessionId: null,
        input: { text: "no" },
      },
    ]);
    expect(ran).toEqual(["send:yes"]);
    expect(reasonOf(results[1])).toBe("declined");
  });

  it("checks the grant before asking a person", async () => {
    const ran: string[] = [];
    let asked = 0;
    const results = await callAll(
      {
        grants: { notes: "read" },
        confirm: () => {
          asked += 1;
          return Promise.resolve(true);
        },
      },
      [{ operation: "send", input: { text: "a" } }],
      ran,
    );
    expect(asked).toBe(0);
    expect(reasonOf(results[0])).toBe("read-only");
  });

  it("looks a reassigned module's grant up by its grant key, through Mycelium.gate too", async () => {
    const ran: string[] = [];
    const runtime = await Mycelium.make({
      modules: [
        Mycelium.gate(Mycelium.grantAs(notes(ran), "owner"), () => ({
          kind: "allow",
        })),
      ],
      access: { grants: { owner: "read", notes: "none" } },
      sandbox: directSandbox(async (bindings) => [
        await bindings.notes?.read?.({ text: "a" }),
        await bindings.notes?.write?.({ text: "b" }),
      ]),
    });
    const lease = await runtime.acquire();
    let result: SandboxValue;
    try {
      const prepared = await Effect.runPromise(
        lease.executeTool.prepare({
          type: "toolCall",
          id: "call",
          name: "execute",
          arguments: { code: "" },
        }),
      );
      result = await Effect.runPromise(Stream.runCollect(prepared.execute()));
    } finally {
      await lease.release();
      await runtime.close();
    }
    expect(ran).toEqual(["read:a"]);
    expect(JSON.stringify(result)).toContain("read-only access to owner");
    expect(() => Mycelium.grantAs(notes(ran), "")).toThrow(
      "Grant key must be non-empty and bounded",
    );
  });

  it("keeps labels through Mycelium.gate", async () => {
    const ran: string[] = [];
    const runtime = await Mycelium.make({
      modules: [Mycelium.gate(notes(ran), () => ({ kind: "allow" }))],
      access: { grants: { notes: "read" } },
      sandbox: directSandbox(async (bindings) => [
        await bindings.notes?.read?.({ text: "a" }),
        await bindings.notes?.write?.({ text: "b" }),
      ]),
    });
    const lease = await runtime.acquire();
    try {
      const prepared = await Effect.runPromise(
        lease.executeTool.prepare({
          type: "toolCall",
          id: "call",
          name: "execute",
          arguments: { code: "" },
        }),
      );
      await Effect.runPromise(Stream.runCollect(prepared.execute()));
    } finally {
      await lease.release();
      await runtime.close();
    }
    expect(ran).toEqual(["read:a"]);
  });
});

describe("label parsing", () => {
  const operationWith =
    (labels: Readonly<{ access?: SandboxValue; confirm?: SandboxValue }>) =>
    () => {
      const definition = {
        input: z.object({}),
        execute: () => Promise.resolve({}),
      };
      // Untyped JS authors can pass anything; parsing must catch it.
      Object.assign(definition, labels);
      return Mycelium.operation(definition);
    };

  it.each([
    { access: "Write" },
    { access: "WRITE" },
    { access: "rw" },
    { access: "" },
    { access: null },
    { confirm: "yes" },
    { confirm: 1 },
    { confirm: null },
  ])("refuses the malformed label %j at definition", (labels) => {
    expect(operationWith(labels)).toThrow(MyceliumDefinitionError);
    expect(operationWith(labels)).toThrow(
      'Access labels must be access: "read" | "write" and confirm: boolean',
    );
  });

  it("accepts exactly read, write and boolean confirm", () => {
    for (const labels of [
      {},
      { access: "read" },
      { access: "write" },
      { confirm: true },
      { confirm: false },
    ])
      expect(operationWith(labels)).not.toThrow();
  });

  it("refuses a direct tool with a malformed label when the runtime is built", async () => {
    const tool: LabelledTool = {
      descriptor: {
        name: "patch",
        description: "patch",
        parameters: Type.Object({}),
      },
      prepare: () => Effect.die("never prepared"),
      module: "files",
    };
    Object.assign(tool, { access: "Write" });
    await expect(
      Mycelium.make({
        tools: [tool],
        sandbox: directSandbox(() => Promise.resolve(null)),
      }),
    ).rejects.toBeInstanceOf(MyceliumDefinitionError);
  });
});

describe("approval and the grant", () => {
  it("re-checks the grant after a person approves", async () => {
    const ran: string[] = [];
    let grant: "write" | "none" = "write";
    const results = await callAll(
      {
        sessionGrants: () => ({ notes: grant }),
        confirm: () => {
          // Revoked while the person was deciding.
          grant = "none";
          return Promise.resolve(true);
        },
      },
      [{ operation: "send", input: { text: "late" } }],
      ran,
    );
    expect(ran).toEqual([]);
    expect(reasonOf(results[0])).toBe("not-granted");
  });
});

describe("labelled direct tools", () => {
  function direct(
    name: string,
    labels: Pick<LabelledTool, "access" | "confirm">,
    prepared: string[],
  ): LabelledTool {
    const tool: MuleTool = {
      descriptor: { name, description: name, parameters: Type.Object({}) },
      prepare: () => {
        prepared.push(name);
        return Effect.succeed({
          execute: () =>
            Stream.succeed({
              type: "result" as const,
              result: {
                content: [{ type: "text" as const, text: "ran" }],
                isError: false,
              },
            }),
        });
      },
    };
    return { ...tool, module: "files", ...labels };
  }

  async function invoke(tool: MuleTool): Promise<string> {
    const prepared = await Effect.runPromise(
      tool.prepare({
        type: "toolCall",
        id: "direct",
        name: tool.descriptor.name,
        arguments: { path: "a" },
      }),
    );
    const [part] = await Effect.runPromise(
      Stream.runCollect(prepared.execute()),
    );
    if (part?.type !== "result") throw new Error("missing result");
    const [content] = part.result.content;
    if (content?.type !== "text") throw new Error("missing text");
    return content.text;
  }

  it("puts every direct tool behind the same grants, before its decoder", async () => {
    const prepared: string[] = [];
    const runtime = await Mycelium.make({
      tools: [
        direct("read", { access: "read" }, prepared),
        direct("apply_patch", {}, prepared),
      ],
      access: { grants: { files: "read" } },
      sandbox: directSandbox(() => Promise.resolve(null)),
    });
    const lease = await runtime.acquire();
    try {
      const [read, patch] = lease.tools;
      if (read === undefined || patch === undefined)
        throw new Error("missing tools");
      expect(read.descriptor.name).toBe("read");
      expect(await invoke(read)).toBe("ran");
      expect(JSON.parse(await invoke(patch))).toEqual({
        refused: true,
        module: "files",
        operation: "apply_patch",
        reason: "read-only",
        message:
          "files.apply_patch changes something, and this Agent has read-only access to files.",
      });
      expect(prepared).toEqual(["read"]);
    } finally {
      await lease.release();
      await runtime.close();
    }
  });

  it("requires MCP approval before preparing an unlabelled direct write", async () => {
    const prepared: string[] = [];
    const runtime = await Mycelium.make({
      tools: [direct("apply_patch", {}, prepared)],
      access: { confirmWrites: true },
      sandbox: directSandbox(() => Promise.resolve(null)),
    });
    const lease = await runtime.acquire();
    try {
      const [patch] = lease.tools;
      if (patch === undefined) throw new Error("Missing direct tool");
      expect(JSON.parse(await invoke(patch))).toMatchObject({
        reason: "confirmation-unavailable",
      });
      expect(prepared).toEqual([]);
    } finally {
      await lease.release();
      await runtime.close();
    }
  });

  it("refuses a confirm direct tool with no handler", async () => {
    const prepared: string[] = [];
    const runtime = await Mycelium.make({
      tools: [direct("deploy", { confirm: true }, prepared)],
      sandbox: directSandbox(() => Promise.resolve(null)),
    });
    const lease = await runtime.acquire();
    try {
      const [deploy] = lease.tools;
      if (deploy === undefined) throw new Error("missing tool");
      expect(JSON.parse(await invoke(deploy))).toMatchObject({
        reason: "confirmation-unavailable",
      });
      expect(prepared).toEqual([]);
    } finally {
      await lease.release();
      await runtime.close();
    }
  });
});
