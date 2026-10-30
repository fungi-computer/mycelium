import { isSandboxFailure } from "./sandbox-boundary.js";
import {
  ResultTooLarge,
  SandboxBusy,
  sandboxFailureMessage,
} from "./sandbox-failure.js";
import { nanoid } from "@fungi.computer/cairn";
import {
  forageDescribeInput,
  forageSearchInput,
  forageListInput,
  type ForageCatalog,
} from "@fungi.computer/forage";
import {
  Type,
  type ImageContent,
  type MuleError,
  type MuleTool,
  type MuleToolExecutionPart,
  type MuleToolResult,
} from "@fungi.computer/mule/effect";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import { checkAccess, type AccessOptions } from "./access.js";
import type { CompiledOperation } from "./compiler.js";
import {
  normalizeExecutionPolicy,
  runExecution,
  type BackingWorkGroup,
  type RawBindings,
  type SerializedSandboxValue,
} from "./internal/execution-lifecycle.js";
import { ImageCollector, type CollectedImage } from "./image-collector.js";
import { OutputCollector } from "./output-collector.js";
import { InvalidOperationInput, type MyceliumError } from "./errors.js";
import type { z } from "zod";
import type { InvocationContext, SandboxValue } from "./index.js";
import type { EffectSandbox } from "./runtime.js";
import type { SandboxAdmissionOptions } from "./sandbox-admission.js";

/** The one outer Mule input authority for the Mycelium execute door. */
const ExecuteInputSchema = Schema.Struct({ code: Schema.String });

/** Shared discovery scope for hosts offering Mycelium's execute tool. */
export const forageDiscoveryInstructions =
  "Inside execute, call forage.search, forage.list or forage.describe to discover operations and inspect their inputs. forage is available only there, not as a separate tool.";

const executeDescription =
  `${forageDiscoveryInstructions} ` +
  'Run async JavaScript in the sandbox. Each call is a fresh sandbox, not a REPL: nothing survives between calls, so redefine helpers or persist state to a file. Never shadow injected module names with local bindings. Find operations with await forage.search({ query }) using one short keyword such as read or shell, or page them all with forage.list({limit:50}), optionally by namespace, until nextCursor is null. Inspect input with await forage.describe({ path }) before calling; describe known paths directly. Call an operation as await env.<path>({ ... }) with the exact dotted path, for example await env.computers.local.exec({ command: "pwd" }); a bare computers.local.exec is not defined. Put multiline shell commands in a template literal. Code is JavaScript first, so double each backslash the shell needs (`\\\\(` passes `\\(`; `\\(` becomes `(`). For file edits, prefer Files operations when available. Explicitly return the final value. Before treating a shell or process result as success, check status, exitCode and stderr.';

/** Host execution limits plus the reserved, host-only admission contract. */
export type ExecutionOptions = Readonly<{
  /** Total execution deadline in milliseconds. */
  timeoutMs?: number;
  /** Maximum acknowledgement wait after cancellation in milliseconds. */
  abortGraceMs?: number;
}> &
  SandboxAdmissionOptions;

/** One bound operation: its labels are checked against the host's grants first. */
function guardedInvoke(
  entry: CompiledOperation,
  context: InvocationContext,
  access: AccessOptions,
  sessionId: string | null,
): (input: SandboxValue) => Promise<SandboxValue> {
  const label = {
    module: entry.grantKey,
    operation: entry.name,
    access: entry.operation.access,
    confirm: entry.operation.confirm,
  };
  return async (input) => {
    const refused = await checkAccess(
      access,
      { sessionId },
      label,
      input,
      context.signal,
    );
    return refused ?? (await entry.operation.invoke(input, context));
  };
}

function operationBindings(
  operations: readonly CompiledOperation[],
  context: InvocationContext,
  access: AccessOptions,
  sessionId: string | null,
): RawBindings {
  // One counter per execute, shared by every bound method. Allocate before
  // awaiting access or operation work, not in asynchronous completion order.
  let nextOperationOrdinal = 0;
  const groups = new Map<
    string,
    Map<string, (input: SandboxValue) => Promise<SandboxValue>>
  >();
  for (const entry of operations) {
    const namespace = entry.namespace;
    const operationName = entry.name;
    const group =
      groups.get(namespace) ??
      new Map<string, (input: SandboxValue) => Promise<SandboxValue>>();
    group.set(operationName, (input) =>
      guardedInvoke(
        entry,
        Object.freeze({
          ...context,
          operationOrdinal: nextOperationOrdinal++,
        }),
        access,
        sessionId,
      )(input),
    );
    groups.set(namespace, group);
  }
  return Object.freeze(
    Object.fromEntries(
      [...groups].map(([namespace, operations]) => [
        namespace,
        Object.freeze(Object.fromEntries(operations)),
      ]),
    ),
  );
}

/** Parse a built-in Forage request; a bad request is the model's invalid input. */
function forageRequest<T>(schema: z.ZodType<T>, input: SandboxValue): T {
  const request = schema.safeParse(input);
  if (!request.success) throw new InvalidOperationInput(request.error.issues);
  return request.data;
}

function forageBindings(
  catalog: ForageCatalog,
): Readonly<Record<string, (input: SandboxValue) => Promise<SandboxValue>>> {
  return Object.freeze({
    list: (input) =>
      Promise.resolve().then(() =>
        catalog.list(forageRequest(forageListInput, input)),
      ),
    describe: (input) =>
      Promise.resolve().then(() =>
        catalog.describe(forageRequest(forageDescribeInput, input)),
      ),
    search: (input) =>
      Promise.resolve().then(() =>
        catalog.search(forageRequest(forageSearchInput, input)),
      ),
  });
}

function bindings(
  operations: readonly CompiledOperation[],
  catalog: ForageCatalog,
  context: InvocationContext,
  access: AccessOptions,
  sessionId: string | null,
): RawBindings {
  return Object.freeze({
    ...operationBindings(operations, context, access, sessionId),
    forage: forageBindings(catalog),
  });
}

/** The Effect-owned execute implementation shared by both public projections. */
export type ExecuteDoor = Readonly<{
  readonly effectTool: MuleTool;
  readonly execute: (
    code: string,
    signal?: AbortSignal,
  ) => Effect.Effect<SerializedSandboxValue, MyceliumError>;
}>;

/** Construct one execute door over a stable compiled surface and sandbox adapter. */
export function createExecuteDoor(
  operations: readonly CompiledOperation[],
  catalog: ForageCatalog,
  sandbox: EffectSandbox,
  backingWork: BackingWorkGroup,
  modules: Readonly<Record<string, string>>,
  namespacePaths: Readonly<Record<string, readonly string[]>>,
  sessionId: string | null,
  access: AccessOptions,
): ExecuteDoor {
  const policy = sandbox.execution ?? normalizeExecutionPolicy(undefined);
  const imageCollector = new ImageCollector();
  const run = (
    executionId: string,
    code: string,
    signal: AbortSignal | undefined,
    toolCallId?: string,
    output?: InvocationContext["output"],
  ): Effect.Effect<SerializedSandboxValue, MyceliumError> =>
    runExecution({
      executionId,
      toolCallId,
      output,
      images: Object.freeze({
        push: (image: CollectedImage) =>
          imageCollector.push(executionId, image),
      }),
      code,
      callerSignal: signal,
      policy,
      sandbox,
      backingWork,
      createBindings: (context) =>
        bindings(
          operations,
          catalog,
          Object.freeze({ ...context, toolCallCode: code }),
          access,
          sessionId,
        ),
      modules,
      namespacePaths,
      sessionId,
    });
  /** Run one execution, then clear whatever images it collected. */
  const execute = (
    code: string,
    signal?: AbortSignal,
  ): Effect.Effect<SerializedSandboxValue, MyceliumError> =>
    Effect.suspend(() => {
      const executionId = nanoid();
      return run(executionId, code, signal).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            imageCollector.dispose(executionId);
          }),
        ),
      );
    });
  /**
   * The Mule projection: after the run, drain the collector once and append
   * its images after the text, on the oversized-result path too.
   */
  const executeTerminal = (
    code: string,
    toolCallId: string,
    output: NonNullable<InvocationContext["output"]>,
  ): Effect.Effect<MuleToolExecutionPart, MuleError> =>
    Effect.suspend(() => {
      const executionId = nanoid();
      const drain = () => imageCollector.drain(executionId);
      return run(executionId, code, undefined, toolCallId, output).pipe(
        Effect.matchEffect({
          onSuccess: (value) =>
            Effect.succeed(
              terminal({ text: value.text, isError: false }, drain()),
            ),
          onFailure: (error) => {
            const images = drain();
            return error instanceof ResultTooLarge && images.length > 0
              ? Effect.succeed(
                  terminal(
                    { text: sandboxFailureMessage(error), isError: true },
                    images,
                  ),
                )
              : Effect.fail(toMuleError(error));
          },
        }),
        Effect.ensuring(
          Effect.sync(() => {
            imageCollector.dispose(executionId);
          }),
        ),
      );
    });
  const effectTool: MuleTool = {
    // Guest JavaScript can invoke mutating operations, even when some bindings
    // declare safe reads. Mule must never blindly replay this whole program.
    replay: "unsafe",
    descriptor: {
      name: "execute",
      description: executeDescription,
      parameters: Type.Object(
        { code: Type.String() },
        { additionalProperties: false },
      ),
    },
    prepare: (call) =>
      Schema.decodeUnknownEffect(ExecuteInputSchema)(call.arguments).pipe(
        Effect.map(({ code }) => ({
          execute: () =>
            Stream.unwrap(
              Effect.gen(function* () {
                const collector = yield* OutputCollector.make();
                yield* Effect.addFinalizer(() =>
                  Effect.sync(() => {
                    collector.dispose();
                  }),
                );
                // Start execution in this stream's scope before starting readers:
                // admission/deadlines must not wait for an output subscriber.
                const execution = yield* Effect.forkScoped(
                  executeTerminal(code, call.id, {
                    write: (stream, bytes) => {
                      collector.write(stream, bytes);
                    },
                  }),
                  { startImmediately: true },
                );
                return Stream.merge(
                  collector.updates,
                  Stream.fromEffect(Fiber.await(execution)).pipe(
                    Stream.flatMap((exit) => {
                      const finalOutput = collector.finish();
                      // Observe completion as data so diagnostics precede either
                      // outcome. Re-yield the original Exit (including its Cause);
                      // Mule alone projects failures into error terminal results.
                      return Stream.concat(
                        Stream.fromIterable(
                          finalOutput === undefined ? [] : [finalOutput],
                        ),
                        Stream.fromEffect(exit),
                      );
                    }),
                  ),
                  { haltStrategy: "right" },
                ).pipe(Stream.takeUntil((part) => part.type === "result"));
              }),
            ),
        })),
        Effect.mapError(() => ({ message: "Invalid execute arguments" })),
      ),
  };
  return Object.freeze({ effectTool, execute });
}

/**
 * Project one finished run into Mule's only terminal part. Each collected
 * image becomes an {@link ImageContent} part after the text, the same
 * `MuleToolResult.content` every other tool result uses; its path, size and
 * type go into application `data`, separate from the model content.
 */
function terminal(
  head: Readonly<{ text: string; isError: boolean }>,
  collected: readonly CollectedImage[],
): MuleToolExecutionPart {
  const images: ImageContent[] = collected.map((image) => ({
    type: "image",
    data: image.data,
    mimeType: image.mimeType,
  }));
  const content: MuleToolResult["content"] = [
    { type: "text", text: head.text },
    ...images,
  ];
  if (collected.length === 0)
    return { type: "result", result: { content, isError: head.isError } };
  const data = {
    images: collected.map((image) => ({
      bytes: image.bytes,
      mimeType: image.mimeType,
      path: image.path,
    })),
  };
  return {
    type: "result",
    result: { content, data, isError: head.isError },
  };
}

function toMuleError(error: MyceliumError): MuleError {
  const result = {
    _tag: "MuleError",
    phase: "tool",
    message: isSandboxFailure(error)
      ? sandboxFailureMessage(error)
      : error.message,
  } satisfies MuleError;
  return error instanceof SandboxBusy ? { ...result, cause: error } : result;
}
