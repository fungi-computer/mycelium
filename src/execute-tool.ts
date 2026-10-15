import { isSandboxFailure } from "./sandbox-boundary.js";
import { ResultTooLarge, sandboxFailureMessage } from "./sandbox-failure.js";
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
import { InvalidOperationInput, type MyceliumError } from "./errors.js";
import type { z } from "zod";
import type { InvocationContext, SandboxValue } from "./index.js";
import type { EffectSandbox } from "./runtime.js";

/** The one outer Mule input authority for the Mycelium execute door. */
const ExecuteInputSchema = Schema.Struct({ code: Schema.String });

const executeDescription =
  "Run async JavaScript in the sandbox. To enumerate all available operations, use forage.list({limit:50}) and follow nextCursor until null; optionally filter by namespace. Use await forage.search({ query: ... }) with short operation keywords, such as read or shell. Search matches operation names and descriptions; if no results match, retry with one relevant keyword. Then use await forage.describe({ path: ... }) to inspect the input before calling an operation. Describe known paths directly. For multiline shell commands, put the command in a JavaScript template literal, for example const command = `pwd\\nprintf done`; a physical newline inside a double-quoted JavaScript string is invalid source. Code is evaluated as JavaScript before shell commands run: if a shell command needs a backslash, write it doubled in the JavaScript string (for example, `\\\\(` passes `\\(` to the shell; `\\(` becomes `(`). For file edits, prefer Files operations when available. Explicitly return the final value. For shell or process results, inspect status, exitCode, and stderr when present before treating the operation as successful. Each execute call runs in a fresh sandbox: no variable, function, or other global survives from a previous call, so redefine any helpers you need every time, or persist state to a file and read it back instead.";

/** Host-selectable execution limits; neither budget can be disabled. */
export type ExecutionOptions = Readonly<{
  /** Total execution deadline in milliseconds. */
  timeoutMs?: number;
  /** Maximum acknowledgement wait after cancellation in milliseconds. */
  abortGraceMs?: number;
}>;

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
    group.set(operationName, guardedInvoke(entry, context, access, sessionId));
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
  ): Effect.Effect<SerializedSandboxValue, MyceliumError> =>
    runExecution({
      executionId,
      toolCallId,
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
  ): Effect.Effect<MuleToolExecutionPart, MuleError> =>
    Effect.suspend(() => {
      const executionId = nanoid();
      const drain = () => imageCollector.drain(executionId);
      return run(executionId, code, undefined, toolCallId).pipe(
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
          execute: () => Stream.fromEffect(executeTerminal(code, call.id)),
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
 * type go into `details`.
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
  const details = {
    images: collected.map((image) => ({
      bytes: image.bytes,
      mimeType: image.mimeType,
      path: image.path,
    })),
  };
  return {
    type: "result",
    result: { content, details, isError: head.isError },
  };
}

function toMuleError(error: MyceliumError): MuleError {
  return {
    _tag: "MuleError",
    phase: "tool",
    message: isSandboxFailure(error)
      ? sandboxFailureMessage(error)
      : error.message,
  };
}
