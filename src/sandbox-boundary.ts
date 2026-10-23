import { nanoid } from "@fungi.computer/cairn";
import { ForageError } from "@fungi.computer/forage";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import { z } from "zod";
import { InvalidOperationInput } from "./errors.js";
import type {
  MyceliumJsonValue,
  SandboxRequest,
  SandboxBinding,
  SandboxValue,
} from "./sandbox.js";
import {
  SourceInvalid,
  GuestThrew,
  DeadlineExceeded,
  SandboxBusy,
  Cancelled,
  DispatchLost,
  SandboxCrashed,
  BadResponse,
  ResultTooLarge,
  ResultNotJson,
  CapabilityFailed,
  Unknown,
  referenceSandboxFailure,
  sandboxFailureMessage,
  type SandboxFailure,
  type CapabilityDiagnostic,
} from "./sandbox-failure.js";

const ownedFailureSchema = z.lazy(() =>
  z.union([
    z.instanceof(SourceInvalid),
    z.instanceof(GuestThrew),
    z.instanceof(DeadlineExceeded),
    z.instanceof(SandboxBusy),
    z.instanceof(Cancelled),
    z.instanceof(DispatchLost),
    z.instanceof(SandboxCrashed),
    z.instanceof(BadResponse),
    z.instanceof(ResultTooLarge),
    z.instanceof(ResultNotJson),
    z.instanceof(CapabilityFailed),
    z.instanceof(Unknown),
  ]),
);

/** The one interpretation boundary for foreign in-process values and rejections. */
const foreignValueSchema = z.lazy(() =>
  z
    .unknown()
    .pipe(
      z.union([
        z
          .instanceof(Error)
          .transform((value) => ({ kind: "error" as const, value })),
        z.null().transform((value) => ({ kind: "null" as const, value })),
        z
          .undefined()
          .transform((value) => ({ kind: "undefined" as const, value })),
        z.string().transform((value) => ({ kind: "string" as const, value })),
        z.boolean().transform((value) => ({ kind: "boolean" as const, value })),
        z
          .union([
            z.number(),
            z.nan(),
            z.literal(Infinity),
            z.literal(-Infinity),
          ])
          .transform((value) => ({ kind: "number" as const, value })),
        z.bigint().transform((value) => ({ kind: "bigint" as const, value })),
        z.symbol().transform((value) => ({ kind: "symbol" as const, value })),
        z
          .instanceof(Function)
          .transform((value) => ({ kind: "function" as const, value })),
        z
          .array(z.unknown())
          .transform((value) => ({ kind: "array" as const, value })),
        z
          .union([z.instanceof(Object), z.record(z.string(), z.unknown())])
          .transform((value) => ({ kind: "object" as const, value })),
        z.unknown().transform(() => ({ kind: "opaque" as const })),
      ]),
    ),
);

/** Admit only errors constructed by the sandbox owner, using the maintained decoder. */
export function isSandboxFailure(cause: unknown): cause is SandboxFailure {
  return ownedFailureSchema.safeParse(cause).success;
}

/** Foreign throws stay private until an owner admits a diagnostic. */
export function sandboxFailure(cause: unknown): SandboxFailure {
  const owned = ownedFailureSchema.safeParse(cause);
  return owned.success ? owned.data : new Unknown({ rawCause: cause });
}

/** Label a rejected JSON value without caller-owned reflection. */
export function sandboxValueKind(value: SandboxValue): string {
  const decoded = foreignValueSchema.parse(value);
  switch (decoded.kind) {
    case "error":
    case "array":
    case "null":
      return "object";
    default:
      return decoded.kind;
  }
}

const bindingInputSchema = z.lazy(() => z.json().optional());
/** JSON argument or the SDK's explicit absence of an argument. */
export type SandboxBindingInput = MyceliumJsonValue | undefined;

/** Decode the RPC argument before any capability callback observes it. */
export function sandboxBindingInput(value: SandboxValue): SandboxBindingInput {
  return bindingInputSchema.parse(value);
}

/** Full operator representation, including nested errors, aggregates and cycles. */
export function sandboxOperatorValue(cause: unknown): z.core.util.JSONType {
  return operatorValue(cause, new Set());
}

function operatorValue(
  cause: unknown,
  seen: Set<unknown>,
): z.core.util.JSONType {
  const decoded = foreignValueSchema.parse(cause);
  switch (decoded.kind) {
    case "null":
    case "undefined":
      return null;
    case "string":
    case "boolean":
      return decoded.value;
    case "number":
      return Number.isFinite(decoded.value)
        ? decoded.value
        : String(decoded.value);
    case "bigint":
    case "symbol":
      return String(decoded.value);
    case "function":
      return { kind: "function", name: decoded.value.name };
    case "opaque":
      return { kind: "opaque" };
    case "error":
    case "array":
    case "object": {
      const value = decoded.value;
      // Zod copies arrays and records. Track their original input identities,
      // retaining references only for this traversal's active ancestors.
      if (seen.has(cause)) return { kind: "circular_reference" };
      seen.add(cause);
      try {
        if (decoded.kind === "array")
          return decoded.value.map((item) => operatorValue(item, seen));
        const fields = Object.fromEntries(
          Object.entries(value).map(([key, item]) => [
            key,
            operatorValue(item, seen),
          ]),
        );
        if (decoded.kind === "error") {
          const error = decoded.value;
          fields.name = error.name;
          fields.message = error.message;
          if (error.stack !== undefined) fields.stack = error.stack;
          if (error.cause !== undefined)
            fields.cause = operatorValue(error.cause, seen);
          if (error instanceof AggregateError)
            fields.errors = operatorValue(error.errors, seen);
        }
        return fields;
      } finally {
        seen.delete(cause);
      }
    }
  }
}

const missingVariable = z.lazy(() =>
  z
    .object({
      name: z.literal("ReferenceError"),
      message: z
        .string()
        .regex(/^[\p{ID_Start}_$][\p{ID_Continue}$]* is not defined$/u),
    })
    .transform((value) => ({
      ...value,
      missingVariable: value.message.slice(0, -" is not defined".length),
    })),
);
const guestDiagnosticSchema = z.lazy(() =>
  z
    .union([
      missingVariable,
      z.object({ name: z.string(), message: z.string() }),
    ])
    .and(z.object({ transportMessage: z.string().max(512).optional() })),
);

/** Decode the engine's guest-owned diagnostic once; renderers receive typed fields. */
export function sandboxGuestFailure(
  cause: unknown,
  bindings: SandboxRequest["bindings"] = {},
): GuestThrew {
  const diagnostic = guestDiagnosticSchema.parse(cause);
  if ("missingVariable" in diagnostic) {
    for (const [namespace, operations] of Object.entries(bindings))
      for (const operation of Object.keys(operations))
        if (`${namespace}_${operation}` === diagnostic.missingVariable)
          return new GuestThrew({
            name: diagnostic.name,
            message: `${diagnostic.message}. Call the operation as a method: ${namespace}.${operation}(...).`,
          });
  }
  return new GuestThrew(diagnostic);
}

const transportEnvelopeSchema = z.lazy(() =>
  z.strictObject({
    v: z.literal(1),
    kind: z.literal("sandbox_failure"),
    tag: z.enum([
      "SourceInvalid",
      "GuestThrew",
      "DeadlineExceeded",
      "SandboxBusy",
      "Cancelled",
      "DispatchLost",
      "SandboxCrashed",
      "BadResponse",
      "ResultTooLarge",
      "ResultNotJson",
      "CapabilityFailed",
      "Unknown",
    ]),
    ref: z.string().min(1).max(32),
    message: z.string().max(160),
  }),
);
const decodeJsonText = Schema.decodeUnknownResult(
  Schema.fromJsonString(Schema.Unknown),
);
const jsonTextSchema = z.lazy(() =>
  z.string().transform((text, context) => {
    const decoded = decodeJsonText(text);
    if (Result.isFailure(decoded)) {
      context.addIssue({
        code: "custom",
        message: "Invalid sandbox transport JSON",
      });
      return z.NEVER;
    }
    return decoded.success;
  }),
);
const transportMessageSchema = jsonTextSchema.pipe(transportEnvelopeSchema);
const returnedOperationFailure = z.strictObject({
  error: transportMessageSchema,
});

/** Error.message is the SDK's wire carrier; only this safe, bounded envelope crosses it. */
export function sandboxTransportMessage(
  failure: SandboxFailure,
  ref: string,
): string {
  const envelope = transportEnvelopeSchema.parse({
    v: 1,
    kind: "sandbox_failure",
    tag: failure._tag,
    ref,
    message: sandboxFailureMessage(failure, false)
      .replace(/\p{Cc}|\p{Cf}/gu, " ")
      .slice(0, 160),
  });
  return JSON.stringify(envelope);
}

/** Recovery uses the decoded identity, never a match against diagnostic text. */
export function recoverSandboxFailure(
  failure: SandboxFailure,
  failures: ReadonlyMap<string, SandboxFailure>,
): SandboxFailure {
  if (failure._tag !== "GuestThrew") return failure;
  const decoded = transportMessageSchema.safeParse(
    failure.transportMessage ?? failure.message,
  );
  if (!decoded.success) return failure;
  const original = failures.get(decoded.data.ref);
  return original?._tag === decoded.data.tag ? original : failure;
}

const executorFailureSchema = z.lazy(() =>
  z.unknown().pipe(
    z.union([
      z
        .literal("Execution timed out")
        .transform(() => ({ kind: "deadline" as const })),
      z.unknown().transform((rawCause) => ({
        kind: "bad_response" as const,
        rawCause,
      })),
    ]),
  ),
);

/** Decode Codemode's documented error carrier before adapters choose a failure. */
export function sandboxExecutorFailure(
  cause: unknown,
  limitMs: number,
  elapsedMs: number,
): SandboxFailure {
  const decoded = executorFailureSchema.parse(cause);
  return decoded.kind === "deadline"
    ? new DeadlineExceeded({ limitMs, elapsedMs })
    : new BadResponse({
        reason: "executor failed outside guest handler",
        rawCause: decoded.rawCause,
      });
}

/** Decode a transport-owned errno without reflecting on a foreign throw in adapters. */
export function sandboxTransportErrno(cause: unknown): string | undefined {
  const error = z.instanceof(Error).safeParse(cause);
  if (!error.success) return undefined;
  const decoded = z.object({ code: z.string() }).safeParse(error.data);
  return decoded.success ? decoded.data.code : undefined;
}

/** Admit Acorn's documented offset from a syntax exception at this boundary. */
export function sandboxSourceError(
  cause: unknown,
): Readonly<{ pos: number }> | undefined {
  const error = z.instanceof(SyntaxError).safeParse(cause);
  if (!error.success) return undefined;
  const decoded = z
    .object({ pos: z.number().int().nonnegative() })
    .safeParse(error.data);
  return decoded.success ? { pos: decoded.data.pos } : undefined;
}

/** Capability owners supply an admission schema; decoding and foreign fallback stay here. */
export function sandboxCapabilityFailure(
  cause: unknown,
  namespace: string,
  operation: string,
  admitted: z.ZodType<CapabilityDiagnostic> = z.never(),
): SandboxFailure {
  const diagnostic = (fields: CapabilityDiagnostic) =>
    new CapabilityFailed({
      namespace,
      operation,
      cause: fields,
      rawCause: cause,
    });
  return z
    .unknown()
    .pipe(
      z.union([
        ownedFailureSchema,
        admitted.transform(diagnostic),
        z
          .instanceof(ForageError)
          .transform((error) =>
            diagnostic({ kind: "Forage", message: error.message }),
          ),
        z.instanceof(InvalidOperationInput).transform((error) =>
          diagnostic({
            kind: "InvalidInput",
            message: `Invalid operation input: ${z.prettifyError({ issues: error.issues })}; use forage.describe`,
          }),
        ),
        z.unknown().transform((rawCause) => new Unknown({ rawCause })),
      ]),
    )
    .parse(cause);
}

/** Host operation callbacks after decoding JSON or an absent RPC argument. */
export type SandboxOperationBindings = Readonly<
  Record<
    string,
    Readonly<
      Record<string, (input: SandboxBindingInput) => Promise<MyceliumJsonValue>>
    >
  >
>;
const maximumRecordedFailures = 128;

/** One boundary owns RPC decoding, safe transport and invocation-scoped recovery. */
export function sandboxBindingBoundary(
  bindings: SandboxOperationBindings,
): Readonly<{
  /** Guest-visible callbacks with validated inputs and safe error carriers. */
  bindings: SandboxRequest["bindings"];
  /** Retain one typed provider refusal and return its safe guest carrier. */
  capture: (failure: SandboxFailure) => string;
  /** Restore an invocation-owned typed error from its exact wire envelope. */
  recover: (failure: SandboxFailure) => SandboxFailure;
  /** Decode a provider error deliberately returned by guest code. */
  returnedFailure: (value: SandboxValue) => SandboxFailure | undefined;
}> {
  const failures = new Map<string, SandboxFailure>();
  const capture = (failure: SandboxFailure): string => {
    if (failures.size >= maximumRecordedFailures)
      throw new Error(
        "Too many failed capability calls; split this execute call",
      );
    const ref = nanoid();
    const referenced = referenceSandboxFailure(failure, ref);
    failures.set(ref, referenced);
    return sandboxTransportMessage(referenced, ref);
  };
  // Store the original cause in this invocation before constructing the safe
  // guest carrier. It must never become an Error.cause exposed to the guest.
  const captureError = (failure: SandboxFailure): Error =>
    new Error(capture(failure));
  return Object.freeze({
    capture,
    bindings: Object.freeze(
      Object.fromEntries(
        Object.entries(bindings).map(([namespace, operations]) => [
          namespace,
          Object.freeze(
            Object.fromEntries(
              Object.entries(operations).map(([operation, invoke]) => {
                const call: SandboxBinding = async (input) => {
                  if (failures.size >= maximumRecordedFailures)
                    throw new Error(
                      "Too many failed capability calls; split this execute call",
                    );
                  try {
                    return await invoke(sandboxBindingInput(input));
                  } catch (rawCause) {
                    throw captureError(
                      sandboxCapabilityFailure(rawCause, namespace, operation),
                    );
                  }
                };
                return [operation, call];
              }),
            ),
          ),
        ]),
      ),
    ),
    recover: (failure) => recoverSandboxFailure(failure, failures),
    returnedFailure: (value) => {
      const decoded = returnedOperationFailure.safeParse(value);
      if (!decoded.success) return undefined;
      const original = failures.get(decoded.data.error.ref);
      return original?._tag === decoded.data.error.tag ? original : undefined;
    },
  });
}
