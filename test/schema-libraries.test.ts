import { directSandbox } from "@fungi.computer/mycelium/testing";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { expect, it } from "vitest";
import { z } from "zod";

import {
  Mycelium,
  type ModuleRegistration,
  type Sandbox,
  type SandboxValue,
} from "../src/index.js";

const WebUrl = Schema.String.check(
  Schema.isMaxLength(2048),
  Schema.makeFilter((value) => /^https?:\/\//u.test(value), {
    message: "Use an http or https URL",
  }),
);

async function runOnce(module: ModuleRegistration, sandbox: Sandbox) {
  const runtime = await Mycelium.make({ modules: [module], sandbox });
  const lease = await runtime.acquire();
  try {
    const prepared = await Effect.runPromise(
      lease.executeTool.prepare({
        type: "toolCall",
        id: "schema-call",
        name: "execute",
        arguments: { code: "return links.save({ url: 'https://shiit.app/' })" },
      }),
    );
    const exit = await Effect.runPromiseExit(
      Stream.runCollect(prepared.execute()),
    );
    return {
      ...lease.catalog.describe({ path: "links.save" }),
      // What reaches the model: the tool parts, or the execute failure.
      modelSaw: Exit.isSuccess(exit)
        ? JSON.stringify(exit.value)
        : Cause.pretty(exit.cause),
    };
  } finally {
    await lease.release();
    await runtime.close();
  }
}

it("decodes Effect Schema input, encodes Effect Schema output and describes both", async () => {
  const received: unknown[] = [];
  const results: unknown[] = [];
  const failures: unknown[] = [];
  const links = Mycelium.module({
    id: "links",
    operations: {
      save: Mycelium.operation({
        input: Schema.Struct({ url: WebUrl }),
        output: Schema.Struct({ saved: Schema.NumberFromString }),
        execute: (input) => {
          received.push(input);
          return Promise.resolve({ saved: 1 });
        },
      }),
    },
  });
  const described = await runOnce(
    links,
    directSandbox(async (bindings) => {
      const save = bindings.links?.save;
      if (save === undefined) throw new Error("links.save missing");
      results.push(await save({ url: "https://shiit.app/" }));
      const [refused] = await Promise.allSettled([
        save({ url: "javascript:alert(1)" }),
      ]);
      if (refused.status === "rejected") failures.push(refused.reason);
      return null;
    }),
  );
  expect(received).toEqual([{ url: "https://shiit.app/" }]);
  // The output codec ran in the encode direction: 1 → "1".
  expect(results).toEqual([{ saved: "1" }]);
  expect(failures).toMatchObject([
    { _tag: "CapabilityFailed", cause: { kind: "InvalidInput" } },
  ]);
  const failure = z
    .object({ cause: z.object({ message: z.string() }) })
    .parse(failures[0]);
  expect(failure.cause.message).toContain("Use an http or https URL");
  expect(described.types).toContain("url: string");
  expect(described.types).toContain("saved: string");
});

it("accepts any Standard Schema that also describes itself as JSON Schema", async () => {
  const results: unknown[] = [];
  const linkInput = z.object({ url: z.string() });
  const standardInput = {
    "~standard": {
      version: 1,
      vendor: "example",
      // A hand-written Standard Schema; any validator library fits here.
      validate: (value: SandboxValue) => {
        const parsed = linkInput.safeParse(value);
        return parsed.success
          ? { value: parsed.data }
          : { issues: parsed.error.issues };
      },
      jsonSchema: {
        input: () => ({
          type: "object",
          properties: { url: { type: "string" } },
          required: ["url"],
        }),
        output: () => ({
          type: "object",
          properties: { url: { type: "string" } },
          required: ["url"],
        }),
      },
    },
  } as const;
  const links = Mycelium.module({
    id: "links",
    operations: {
      save: Mycelium.operation({
        input: standardInput,
        execute: ({ url }) => Promise.resolve({ url }),
      }),
    },
  });
  const described = await runOnce(
    links,
    directSandbox(async (bindings) => {
      const save = bindings.links?.save;
      if (save === undefined) throw new Error("links.save missing");
      results.push(await save({ url: "https://shiit.app/" }));
      return null;
    }),
  );
  expect(results).toEqual([{ url: "https://shiit.app/" }]);
  expect(described.types).toContain("url: string");
});

/** Call `links.save` once with `input` and collect what the guest saw. */
async function callSave(
  module: ModuleRegistration,
  input: SandboxValue,
): Promise<Readonly<{ result: unknown; error: unknown }>> {
  let outcome: PromiseSettledResult<unknown> | undefined;
  await runOnce(
    module,
    directSandbox(async (bindings) => {
      const save = bindings.links?.save;
      if (save === undefined) throw new Error("links.save missing");
      [outcome] = await Promise.allSettled([save(input)]);
      return null;
    }),
  );
  return outcome?.status === "fulfilled"
    ? { result: outcome.value, error: undefined }
    : { result: undefined, error: outcome?.reason };
}

it("keeps a throwing Effect filter a host error, never model-visible invalid input", async () => {
  const leaky = Schema.String.check(
    Schema.makeFilter(() => {
      throw new Error("db password=hunter2");
    }),
  );
  const links = Mycelium.module({
    id: "links",
    operations: {
      save: Mycelium.operation({
        input: Schema.Struct({ url: leaky }),
        execute: () => Promise.resolve(null),
      }),
    },
  });
  const { modelSaw } = await runOnce(
    links,
    directSandbox(async (bindings) => {
      const save = bindings.links?.save;
      if (save === undefined) throw new Error("links.save missing");
      return save({ url: "https://shiit.app/" });
    }),
  );
  expect(modelSaw).not.toContain("hunter2");
  expect(modelSaw).not.toContain("Invalid operation input");
});

it("strips unknown keys from Effect input like Zod does", async () => {
  const received: unknown[] = [];
  const links = Mycelium.module({
    id: "links",
    operations: {
      save: Mycelium.operation({
        input: Schema.Struct({ url: Schema.String }),
        execute: (input) => {
          received.push(input);
          return Promise.resolve(null);
        },
      }),
    },
  });
  await callSave(links, { url: "https://shiit.app/", extra: true });
  expect(received).toEqual([{ url: "https://shiit.app/" }]);
});

it("reports an Effect output that fails to encode as a bad response for operators", async () => {
  const links = Mycelium.module({
    id: "links",
    operations: {
      save: Mycelium.operation({
        input: Schema.Struct({ url: Schema.String }),
        output: Schema.Struct({
          saved: Schema.Number.check(Schema.isGreaterThan(0)),
        }),
        execute: () => Promise.resolve({ saved: -1 }),
      }),
    },
  });
  const { error } = await callSave(links, { url: "https://a.b/" });
  expect(error).toMatchObject({
    _tag: "BadResponse",
    reason: "Capability output failed its declared schema",
    rawCause: { name: "InvalidOperationOutput" },
  });
});

class Link extends Schema.Class<Link>("Link")({
  url: Schema.String,
  title: Schema.optional(Schema.String),
}) {}

type Folder = {
  readonly name: string;
  readonly children: readonly Folder[];
};
const Folder: Schema.Codec<Folder> = Schema.Struct({
  name: Schema.String,
  children: Schema.Array(Schema.suspend((): Schema.Codec<Folder> => Folder)),
}).annotate({ identifier: "Folder" });

it("describes Effect classes, identified fields, recursion and numbers as the JSON it accepts", async () => {
  const received: unknown[] = [];
  const links = Mycelium.module({
    id: "links",
    operations: {
      save: Mycelium.operation({
        input: Schema.Struct({
          link: Link,
          folder: Folder,
          rating: Schema.optional(Schema.Number),
        }),
        execute: (input) => {
          received.push(input);
          return Promise.resolve(null);
        },
      }),
    },
  });
  const described = await runOnce(
    links,
    directSandbox(() => Promise.resolve(null)),
  );
  // The decoder accepts exactly the JSON the description advertises.
  const { error } = await callSave(links, {
    link: { url: "https://a.b/", title: null },
    folder: { name: "root", children: [{ name: "child", children: [] }] },
    rating: "Infinity",
  });
  expect(error).toBeUndefined();
  expect(received).toMatchObject([
    {
      link: { url: "https://a.b/" },
      folder: { name: "root" },
      rating: Infinity,
    },
  ]);
  expect(described.types).toMatchInlineSnapshot(`
    "export type links$save_Input = {
      folder: {
        children: unknown[];
        name: string;
      };
      link: {
        title?: string | null;
        url: string;
      };
      rating?: number | "Infinity" | "-Infinity" | "NaN" | null;
    };
    export type links$save_Output = unknown;
    export declare const env: { ["links"]: { ["save"]: (input: links$save_Input) => Promise<links$save_Output> } };"
  `);
});
