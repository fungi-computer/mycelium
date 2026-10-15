import { Mycelium } from "@fungi.computer/mycelium";
import { directSandbox } from "@fungi.computer/mycelium/testing";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import { z } from "zod";

const ping = Mycelium.operation({
  input: z.object({}),
  output: z.object({ value: z.string() }),
  execute: async () => ({ value: "pong" }),
});
const runtime = await Mycelium.make({
  modules: [Mycelium.module({ id: "demo", operations: { ping } })],
  sandbox: directSandbox((bindings) => bindings.demo.ping({})),
});
const lease = await runtime.acquire();
const call = {
  type: "toolCall" as const,
  id: "1",
  name: "execute",
  arguments: { code: "return demo.ping({})" },
};
const prepared = await Effect.runPromise(lease.executeTool.prepare(call));
const parts = await Effect.runPromise(Stream.runCollect(prepared.execute()));
const text = parts[0]?.result.content[0];
if (text?.type === "text") console.log(text.text); // {"value":"pong"}
await lease.release();
await runtime.close();
