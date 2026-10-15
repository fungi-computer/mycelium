import { directSandbox } from "@fungi.computer/mycelium/testing";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import { expect, it } from "vitest";
import { z } from "zod";

import { Mycelium as Define } from "../src/define.js";
import { Mycelium, type MyceliumJsonValue } from "../src/index.js";
import {
  describeModule,
  invokeModuleOperation,
  remoteModule,
} from "../src/remote.js";

const context = {
  executionId: "execution-1",
  deadlineAt: null,
  signal: new AbortController().signal,
};

const bookmarks = Define.module({
  id: "example.bookmarks",
  name: "bookmarks",
  description: "Bookmark tools",
  operations: {
    check: Define.operation({
      replay: "safe",
      description: "Check a link",
      input: z.strictObject({ url: z.string().max(2048) }),
      output: z.strictObject({ ok: z.boolean(), host: z.string() }),
      execute: ({ url }) =>
        Promise.resolve(
          URL.canParse(url)
            ? { ok: true, host: new URL(url).host }
            : { ok: false, host: "" },
        ),
    }),
    tags: Define.module({
      id: "example.bookmarks.tags",
      name: "tags",
      operations: {
        count: Define.operation({
          input: z.strictObject({ tags: z.array(z.string()) }),
          execute: ({ tags }) => Promise.resolve(tags.length),
        }),
      },
    }),
  },
});

it("describes a module tree as dotted JSON Schema operations", () => {
  const descriptor = describeModule(bookmarks);
  expect(descriptor.description).toBe("Bookmark tools");
  expect(Object.keys(descriptor.operations).sort()).toEqual([
    "check",
    "tags.count",
  ]);
  expect(descriptor.operations.check).toMatchObject({
    replay: "safe",
    description: "Check a link",
    input: { type: "object", required: ["url"] },
    output: { type: "object" },
  });
  expect(descriptor.operations["tags.count"]).toMatchObject({
    replay: "unsafe",
    output: null,
  });
  expect(JSON.parse(JSON.stringify(descriptor))).toEqual(descriptor);
});

it("invokes a described operation through its own schemas", async () => {
  await expect(
    invokeModuleOperation(
      bookmarks,
      "check",
      { url: "https://shiit.app/x" },
      context,
    ),
  ).resolves.toEqual({ ok: true, host: "shiit.app" });
  await expect(
    invokeModuleOperation(
      bookmarks,
      "tags.count",
      { tags: ["a", "b"] },
      context,
    ),
  ).resolves.toBe(2);
  await expect(
    invokeModuleOperation(bookmarks, "check", { url: 7 }, context),
  ).rejects.toThrow();
  await expect(
    invokeModuleOperation(bookmarks, "missing", {}, context),
  ).rejects.toThrow("Unknown remote operation: missing");
});

it("rebuilds a described module on the host and forwards each call", async () => {
  const forwarded: { path: string; input: MyceliumJsonValue }[] = [];
  const host = remoteModule({
    id: "hosted:bookmarks",
    name: "BOOKMARKS",
    descriptor: describeModule(bookmarks),
    invoke: async (path, input, invocation) => {
      forwarded.push({ path, input });
      return invokeModuleOperation(bookmarks, path, input, invocation);
    },
  });
  const results: unknown[] = [];
  const runtime = await Mycelium.make({
    modules: [host],
    sandbox: directSandbox(async (bindings) => {
      const check = bindings.BOOKMARKS?.check;
      const count = bindings.BOOKMARKS?.["tags.count"];
      if (check === undefined || count === undefined)
        throw new Error("remote operations missing");
      results.push(await check({ url: "https://example.com/a" }));
      results.push(await count({ tags: ["x"] }));
      // The host-side schema refuses bad input before anything is forwarded.
      await expect(check({ url: 1 })).rejects.toThrow();
      results.push("refused");
      return null;
    }),
  });
  const lease = await runtime.acquire();
  try {
    expect(
      lease.catalog.describe({ path: "BOOKMARKS.tags.count" }),
    ).toMatchObject({ path: "BOOKMARKS.tags.count" });
    const prepared = await Effect.runPromise(
      lease.executeTool.prepare({
        type: "toolCall",
        id: "remote-call",
        name: "execute",
        arguments: { code: "return null" },
      }),
    );
    await Effect.runPromise(Stream.runCollect(prepared.execute()));
  } finally {
    await lease.release();
    await runtime.close();
  }
  expect(results).toEqual([{ ok: true, host: "example.com" }, 1, "refused"]);
  expect(forwarded).toEqual([
    { path: "check", input: { url: "https://example.com/a" } },
    { path: "tags.count", input: { tags: ["x"] } },
  ]);
});

it("refuses a descriptor whose paths collide", () => {
  expect(() =>
    remoteModule({
      id: "bad",
      name: "bad",
      descriptor: {
        operations: {
          a: { replay: "safe", input: {}, output: null },
          "a.b": { replay: "safe", input: {}, output: null },
        },
      },
      invoke: () => Promise.resolve(null),
    }),
  ).toThrow("collides");
});

it("runs a runtime-free definition in the full runtime unchanged", async () => {
  const runtime = await Mycelium.make({
    modules: [bookmarks],
    sandbox: directSandbox(() => Promise.resolve(null)),
  });
  const lease = await runtime.acquire();
  try {
    expect(lease.catalog.describe({ path: "bookmarks.check" })).toMatchObject({
      path: "bookmarks.check",
      kind: "operation",
    });
  } finally {
    await lease.release();
    await runtime.close();
  }
});
