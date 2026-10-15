import type { JsonSchemaSource } from "@fungi.computer/forage";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as SchemaIssue from "effect/SchemaIssue";
import type { SandboxValue } from "./index.js";

/** One Standard Schema issue. */
export type OperationSchemaIssue = Readonly<{
  message: string;
  path?: readonly (PropertyKey | Readonly<{ key: PropertyKey }>)[] | undefined;
}>;

/** Standard Schema validation result. */
export type OperationSchemaResult<T> =
  | Readonly<{ value: T; issues?: undefined }>
  | Readonly<{ issues: readonly OperationSchemaIssue[] }>;

/**
 * Any Standard Schema that also implements Standard JSON Schema, such as a
 * Zod 4 schema. Mycelium validates with `validate` and describes the operation
 * to the model with `jsonSchema`.
 */
export type StandardOperationSchema<T> = Readonly<{
  "~standard": Readonly<{
    /** Decode or validate one value, synchronously or not. */
    validate: (
      value: SandboxValue,
    ) => OperationSchemaResult<T> | Promise<OperationSchemaResult<T>>;
    /** Describe the accepted and produced JSON to the model. */
    jsonSchema: JsonSchemaSource["~standard"]["jsonSchema"];
  }>;
}>;

/**
 * Schema for one operation's input or output: an Effect schema (exchanged as
 * its canonical JSON codec, `Schema.toCodecJson`), or any Standard Schema with
 * Standard JSON Schema such as Zod 4.
 */
export type OperationSchema<T> =
  Schema.Codec<T, unknown> | StandardOperationSchema<T>;

/** One operation schema reduced to the three things Mycelium needs. */
export type CompiledOperationSchema<T> = Readonly<{
  /** What Forage renders for the model: always the JSON the sandbox exchanges. */
  description: JsonSchemaSource;
  /** Untrusted sandbox JSON → the value the operation receives. */
  decode: (value: SandboxValue) => Promise<OperationSchemaResult<T>>;
  /** The operation's result → the JSON value the sandbox receives. */
  encode: (value: T) => Promise<OperationSchemaResult<unknown>>;
}>;

const formatIssues = SchemaIssue.makeFormatterStandardSchemaV1();

/**
 * Run one Effect parse. A schema failure becomes issues; a defect (a throwing
 * filter or transformation) is a host bug and rethrows as a host error, so its
 * message and stack never reach the model as "invalid input".
 */
async function parsed<A>(
  parse: Effect.Effect<A, Schema.SchemaError>,
): Promise<OperationSchemaResult<A>> {
  const exit = await Effect.runPromiseExit(parse);
  if (Exit.isSuccess(exit)) return { value: exit.value };
  const failure = Cause.findError(exit.cause);
  if (Result.isFailure(failure)) throw Cause.squash(exit.cause);
  return { issues: formatIssues(failure.success.issue).issues };
}

function fromEffect<T>(
  schema: Schema.Codec<T, unknown>,
): CompiledOperationSchema<T> {
  // One canonical JSON codec owns decode, encode and the description, so the
  // model is told exactly the JSON Mycelium accepts and returns.
  const codec = Schema.toCodecJson(schema);
  const json = Schema.toStandardJSONSchemaV1(codec)["~standard"].jsonSchema;
  const decode = Schema.decodeUnknownEffect(codec);
  const encode = Schema.encodeUnknownEffect(codec);
  return Object.freeze({
    description: {
      "~standard": { jsonSchema: { input: json.input, output: json.input } },
    },
    decode: (value: SandboxValue) => parsed(decode(value)),
    encode: (value: T) => parsed(encode(value)),
  });
}

function fromStandard<T>(
  schema: StandardOperationSchema<T>,
): CompiledOperationSchema<T> {
  const validate = async (
    value: SandboxValue,
  ): Promise<OperationSchemaResult<T>> => schema["~standard"].validate(value);
  return Object.freeze({
    description: schema,
    decode: validate,
    // Standard Schema has no encoder: the result must already be JSON and is
    // validated as declared.
    encode: validate,
  });
}

/** Reduce an Effect or Standard operation schema to Mycelium's internal port. */
export function compileOperationSchema<T>(
  schema: OperationSchema<T>,
): CompiledOperationSchema<T> {
  return Schema.isSchema(schema) ? fromEffect(schema) : fromStandard(schema);
}
