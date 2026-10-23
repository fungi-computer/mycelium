import {
  run,
  type MuleEvent,
  type AssistantMessage,
  type MuleToolExecutionPart,
} from "@fungi.computer/mule/effect";
import { fauxModel } from "@fungi.computer/mule/testing";
import * as Effect from "effect/Effect";
import * as Cause from "effect/Cause";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import { expect, it } from "vitest";
import { z } from "zod";
import { Mycelium } from "../src/index.js";
import { make, fromTypedSandbox } from "../src/effect.js";

it.each(["throttled success", "immediate success", "immediate failure"])(
  "delivers real capability output before Mule's %s terminal",
  async (outcome) => {
    const events: MuleEvent[] = [];
    const faux = fauxModel([]);
    const assistant: AssistantMessage = {
      role: "assistant",
      api: faux.model.api,
      model: faux.model.id,
      provider: faux.model.provider,
      timestamp: 1,
      stopReason: "toolUse",
      content: [
        {
          type: "toolCall",
          id: "build-call",
          name: "execute",
          arguments: { code: "build" },
        },
      ],
      usage: {
        input: 0,
        output: 0,
        cacheRead: 0,
        cacheWrite: 0,
        totalTokens: 0,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
      },
    };
    faux.setResponses([
      assistant,
      {
        ...assistant,
        stopReason: "stop",
        content: [{ type: "text", text: "Finished" }],
      },
    ]);
    const module = Mycelium.module({
      id: "build",
      description: "Build fixture",
      operations: {
        run: Mycelium.operation({
          replay: "unsafe",
          description: "Build",
          input: z.object({}),
          output: z.string(),
          execute: async (_input, context) => {
            context.output?.write("stdout", new Uint8Array([0xe2, 0x82]));
            context.output?.write(
              "stderr",
              new TextEncoder().encode("warning\n"),
            );
            context.output?.write("stdout", new Uint8Array([0xac, 0x0a]));
            if (outcome === "throttled success")
              await new Promise((resolve) => setTimeout(resolve, 150));
            // A process can end with an incomplete scalar. Its EOF replacement
            // must be visible before the unchanged durable result.
            context.output?.write("stdout", new Uint8Array([0xf0, 0x9f]));
            if (outcome === "immediate failure")
              throw new Error("operation failed");
            return "complete";
          },
        }),
      },
    });
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const runtime = yield* make({
            modules: [module],
            sandbox: fromTypedSandbox({
              execute: async (request) => {
                const operation = request.bindings.build?.run;
                if (operation === undefined)
                  throw new Error("Missing build operation");
                return await operation({});
              },
            }),
          });
          const lease = yield* runtime.acquire();
          yield* run(
            {
              model: faux.model,
              retryable: faux.retryable,
              stream: faux.stream,
              systemPrompt: "Build",
              messages: [{ role: "user", content: "Build", timestamp: 1 }],
              tools: [lease.executeTool],
            },
            (event) =>
              Effect.sync(() => {
                events.push(event);
              }),
          );
        }),
      ),
    );
    const updates = events.filter(
      (event) => event.type === "tool_execution_update",
    );
    expect(updates).toHaveLength(outcome === "throttled success" ? 2 : 1);
    if (outcome === "throttled success")
      expect(updates[0]?.result).toMatchObject({
        content: [{ type: "text", text: "warning\n€\n" }],
        data: { droppedBytes: 0, totalBytes: 12 },
      });
    expect(updates.at(-1)?.result).toMatchObject({
      content: [{ type: "text", text: "warning\n€\n\uFFFD" }],
      data: { droppedBytes: 0, totalBytes: 14 },
    });
    const terminal = events.find(
      (event) => event.type === "tool_execution_end",
    );
    expect(terminal?.result.isError).toBe(outcome === "immediate failure");
    if (outcome === "immediate failure") {
      expect(terminal?.result.content).toHaveLength(1);
      const content = terminal?.result.content[0];
      if (content?.type !== "text") throw new Error("Missing Mule error text");
      expect(content.text).toMatch(
        /^The sandbox failed for an unrecognized reason \(ref .+\)$/,
      );
    } else {
      expect(terminal?.result.content).toEqual([
        { type: "text", text: '"complete"' },
      ]);
    }
    expect(
      events.filter((event) => event.type === "tool_execution_end"),
    ).toHaveLength(1);
    expect(
      events.findIndex((event) => event.type === "tool_execution_update"),
    ).toBeLessThan(
      events.findIndex((event) => event.type === "tool_execution_end"),
    );
    const terminalIndex = events.findIndex(
      (event) => event.type === "tool_execution_end",
    );
    expect(
      events
        .slice(terminalIndex + 1)
        .some((event) => event.type === "tool_execution_update"),
    ).toBe(false);
  },
);

it("flushes diagnostics but keeps an immediate real operation throw in the stream error channel", async () => {
  const parts: MuleToolExecutionPart[] = [];
  const module = Mycelium.module({
    id: "failure",
    description: "Failure fixture",
    operations: {
      run: Mycelium.operation({
        replay: "unsafe",
        description: "Fail immediately",
        input: z.object({}),
        output: z.string(),
        execute: (_input, context) => {
          context.output?.write(
            "stderr",
            new TextEncoder().encode("diagnostic"),
          );
          context.output?.write("stdout", new Uint8Array([0xe2, 0x82]));
          throw new Error("private operation cause");
        },
      }),
    },
  });
  const exit = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* make({
          modules: [module],
          sandbox: fromTypedSandbox({
            execute: async (request) => {
              const operation = request.bindings.failure?.run;
              if (operation === undefined)
                throw new Error("Missing failure operation");
              return await operation({});
            },
          }),
        });
        const lease = yield* runtime.acquire();
        const prepared = yield* lease.executeTool.prepare({
          type: "toolCall",
          id: "failure-call",
          name: "execute",
          arguments: { code: "fail" },
        });
        return yield* Effect.exit(
          prepared.execute().pipe(
            Stream.runForEach((part) =>
              Effect.sync(() => {
                parts.push(part);
              }),
            ),
          ),
        );
      }),
    ),
  );
  expect(parts).toEqual([
    {
      type: "update",
      result: {
        isError: false,
        content: [{ type: "text", text: "diagnostic\uFFFD" }],
        data: { totalBytes: 12, droppedBytes: 0 },
      },
    },
  ]);
  expect(Exit.isFailure(exit)).toBe(true);
  if (!Exit.isFailure(exit)) throw new Error("Expected operation failure");
  const failure = Cause.findErrorOption(exit.cause);
  expect(Option.isSome(failure)).toBe(true);
  if (!Option.isSome(failure)) throw new Error("Expected typed Mule error");
  expect(failure.value).toMatchObject({
    _tag: "MuleError",
    phase: "tool",
  });
  expect(failure.value.message).toMatch(
    /^The sandbox failed for an unrecognized reason \(ref .+\)$/,
  );
  expect(Cause.hasInterruptsOnly(exit.cause)).toBe(false);
});
