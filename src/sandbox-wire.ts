import {
  sandboxOperatorValue,
  sandboxGuestFailure,
} from "./sandbox-boundary.js";
import { z } from "zod";
import {
  SourceInvalid,
  DeadlineExceeded,
  ResultNotJson,
  SandboxCrashed,
  BadResponse,
  DispatchLost,
  Cancelled,
  ResultTooLarge,
  CapabilityFailed,
  Unknown,
  type SandboxFailure,
} from "./sandbox-failure.js";

/** The host-to-host result contract; raw diagnostic fields never go to guest code. */
export const sandboxWireSchema = z.discriminatedUnion("ok", [
  z.strictObject({ ok: z.literal(true), value: z.json() }),
  z.strictObject({
    ok: z.literal(false),
    error: z.discriminatedUnion("kind", [
      z.strictObject({
        kind: z.literal("invalid_source"),
        line: z.number().int().min(1),
        column: z.number().int().min(1),
        excerpt: z.string().max(160),
      }),
      z.strictObject({
        kind: z.literal("guest_threw"),
        name: z.string().max(80),
        message: z.string().max(512),
        transportMessage: z.string().max(512).optional(),
      }),
      z.strictObject({
        kind: z.literal("deadline_exceeded"),
        limitMs: z.number().int().positive(),
        elapsedMs: z.number().nonnegative(),
      }),
      z.strictObject({
        kind: z.literal("result_not_json"),
        valueKind: z.string().max(80),
        rawCause: z.json(),
      }),
      z.strictObject({
        kind: z.literal("sandbox_crashed"),
        rawCause: z.json(),
      }),
      z.strictObject({
        kind: z.literal("bad_response"),
        reason: z.string(),
        rawCause: z.json(),
      }),
      z.strictObject({
        kind: z.literal("dispatch_lost"),
        elapsedMs: z.number().nonnegative(),
        transport: z.string(),
        errno: z.string().optional(),
        rawCause: z.json(),
      }),
      z.strictObject({
        kind: z.literal("cancelled"),
        by: z.enum(["caller", "operator"]),
      }),
      z.strictObject({
        kind: z.literal("result_too_large"),
        size: z.number().nonnegative(),
      }),
      z.strictObject({
        kind: z.literal("capability_failed"),
        namespace: z.string(),
        operation: z.string(),
        cause: z.strictObject({
          kind: z.enum([
            "Files",
            "Forage",
            "InvalidInput",
            "Unavailable",
            "OperationFailed",
          ]),
          message: z.string().max(1000),
        }),
        rawCause: z.json(),
      }),
      z.strictObject({ kind: z.literal("unknown"), rawCause: z.json() }),
    ]),
  }),
]);
/** Typed, decoded host wire value. */
export type SandboxWire = z.output<typeof sandboxWireSchema>;

/** Convert one validated wire failure into the shared error channel. */
export function sandboxWireFailure(
  error: Extract<SandboxWire, { ok: false }>["error"],
): SandboxFailure {
  switch (error.kind) {
    case "invalid_source":
      return new SourceInvalid(error);
    case "guest_threw":
      return sandboxGuestFailure(error);
    case "deadline_exceeded":
      return new DeadlineExceeded(error);
    case "result_not_json":
      return new ResultNotJson({
        kind: error.valueKind,
        rawCause: error.rawCause,
      });
    case "sandbox_crashed":
      return new SandboxCrashed(error);
    case "bad_response":
      return new BadResponse(error);
    case "dispatch_lost":
      return new DispatchLost(error);
    case "cancelled":
      return new Cancelled(error);
    case "result_too_large":
      return new ResultTooLarge(error);
    case "capability_failed":
      return new CapabilityFailed(error);
    case "unknown":
      return new Unknown(error);
  }
}

/** Host-only encoding; the guest receives only the admitted diagnostic text. */
export function sandboxFailureWire(
  failure: SandboxFailure,
): z.core.util.JSONType {
  let error: Extract<SandboxWire, { ok: false }>["error"];
  switch (failure._tag) {
    case "SourceInvalid":
      error = {
        kind: "invalid_source",
        line: failure.line,
        column: failure.column,
        excerpt: failure.excerpt,
      };
      break;
    case "GuestThrew":
      error = {
        kind: "guest_threw",
        name: failure.name.slice(0, 80),
        message: failure.message.slice(0, 512),
      };
      if (failure.transportMessage !== undefined)
        error.transportMessage = failure.transportMessage;
      break;
    case "DeadlineExceeded":
      error = {
        kind: "deadline_exceeded",
        limitMs: failure.limitMs,
        elapsedMs: failure.elapsedMs,
      };
      break;
    case "Cancelled":
      error = { kind: "cancelled", by: failure.by };
      break;
    case "DispatchLost":
      error = {
        kind: "dispatch_lost",
        elapsedMs: failure.elapsedMs,
        transport: failure.transport,
        rawCause: sandboxOperatorValue(failure.rawCause),
      };
      if (failure.errno !== undefined) error.errno = failure.errno;
      break;
    case "SandboxCrashed":
      error = {
        kind: "sandbox_crashed",
        rawCause: sandboxOperatorValue(failure.rawCause),
      };
      break;
    case "BadResponse":
      error = {
        kind: "bad_response",
        reason: failure.reason,
        rawCause: sandboxOperatorValue(failure.rawCause),
      };
      break;
    case "ResultNotJson":
      error = {
        kind: "result_not_json",
        valueKind: failure.kind,
        rawCause: sandboxOperatorValue(failure.rawCause),
      };
      break;
    case "ResultTooLarge":
      error = { kind: "result_too_large", size: failure.size };
      break;
    case "CapabilityFailed":
      error = {
        kind: "capability_failed",
        namespace: failure.namespace,
        operation: failure.operation,
        cause: {
          kind: failure.cause.kind,
          message: failure.cause.message.slice(0, 1000),
        },
        rawCause: sandboxOperatorValue(failure.rawCause),
      };
      break;
    case "Unknown":
      error = {
        kind: "unknown",
        rawCause: sandboxOperatorValue(failure.rawCause),
      };
      break;
  }
  return z.json().parse({ ok: false, error });
}
