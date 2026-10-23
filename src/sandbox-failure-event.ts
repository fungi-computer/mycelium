import * as Schema from "effect/Schema";
import type { z } from "zod";

const operatorJsonValue: Schema.Codec<z.core.util.JSONType> = Schema.suspend(
  () =>
    Schema.Union([
      Schema.Null,
      Schema.Boolean,
      Schema.Finite,
      Schema.String,
      Schema.mutable(Schema.Array(operatorJsonValue)),
      Schema.mutableKey(Schema.Record(Schema.String, operatorJsonValue)),
    ]),
);
const reference = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(256),
);
const identity = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(128),
);

/** Shared structural grammar for durable operator facts and client inspection. */
export const SandboxFailureEventSchema = Schema.Struct({
  kind: Schema.Literal("sandbox_failure"),
  ref: reference,
  executionId: reference,
  adapter: identity,
  buildSha: Schema.NullOr(identity),
  tag: Schema.Literals([
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
  fields: Schema.Record(Schema.String, operatorJsonValue),
  healthAlert: Schema.Boolean,
});

/** Full operator diagnostics, excluded from model conversation and default reads. */
export type SandboxFailureEvent = typeof SandboxFailureEventSchema.Type;
