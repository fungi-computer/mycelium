import { z } from "zod";
import { sandboxNamespaceSegmentSchema } from "./sandbox-guest.js";

import { parseAccessLabels, type Access } from "./access.js";
import type {
  InvocationContext,
  ModuleDefinition,
  MyceliumJsonValue,
  OperationDefinition,
  SandboxValue,
} from "./index.js";
import {
  InvalidOperationInput,
  InvalidOperationOutput,
  MyceliumDefinitionError,
} from "./errors.js";
import type { JsonSchemaSource } from "@fungi.computer/forage";
import { compileOperationSchema } from "./operation-schema.js";
import { BadResponse } from "./sandbox-failure.js";

const operationBrand: unique symbol = Symbol("mycelium.operation");
const operationTypes: unique symbol = Symbol("mycelium.operation.types");
const moduleBrand: unique symbol = Symbol("mycelium.module");

/** Opaque handle for one schema-described operation. */
export type OperationRegistration<I = unknown, O = unknown> = Readonly<{
  readonly [operationBrand]: true;
  readonly [operationTypes]?: Readonly<{ input: I; output: O }>;
}>;

/** Opaque handle for one namespaced capability module. */
export type ModuleRegistration = Readonly<{
  readonly [moduleBrand]: true;
}>;

/** Captured operation data available only to the package compiler. */
export type StoredOperation = Readonly<{
  /** Replay classification of this operation, not of the enclosing execute tool. */
  replay: "safe" | "unsafe";
  /** What the operation does, as its author declared it; unlabelled is `write`. */
  access: Access;
  /** The author asks that a person approve each call first. */
  confirm: boolean;
  description?: string;
  /** Model-facing description of the JSON the operation accepts. */
  input: JsonSchemaSource;
  /** Model-facing description of the JSON the operation returns. */
  output?: JsonSchemaSource;
  /** Returns the declared result; the execute door checks it is JSON. */
  invoke: (
    input: SandboxValue,
    context: InvocationContext,
  ) => Promise<SandboxValue>;
}>;

/** Captured module data available only to the package compiler. */
export type StoredModule = Readonly<{
  id: string;
  /** The key a host's grant names for this module; its id unless reassigned. */
  grantKey: string;
  name: string;
  namespace: readonly string[];
  description?: string;
  operations: Readonly<
    Record<string, OperationRegistration | ModuleRegistration>
  >;
}>;

const operationDefinitions = new WeakMap<object, StoredOperation>();
const moduleDefinitions = new WeakMap<object, StoredModule>();

type CapturedOperation = {
  replay: "safe" | "unsafe";
  access: Access;
  confirm: boolean;
  description?: string;
  input: JsonSchemaSource;
  output?: JsonSchemaSource;
  invoke: StoredOperation["invoke"];
};

type CapturedModule = {
  id: string;
  grantKey: string;
  name: string;
  namespace: readonly string[];
  description?: string;
  operations: Readonly<
    Record<string, OperationRegistration | ModuleRegistration>
  >;
};

/** Construct an opaque operation handle and retain its private definition. */
export function defineOperation<I, O>(
  definition: OperationDefinition<I, O>,
): OperationRegistration<I, O> {
  const registration: OperationRegistration<I, O> = Object.freeze({
    [operationBrand]: true,
  });
  const input = compileOperationSchema(definition.input);
  const output =
    definition.output === undefined
      ? undefined
      : compileOperationSchema(definition.output);
  const captured: CapturedOperation = {
    replay: definition.replay ?? "unsafe",
    ...parseAccessLabels(definition),
    input: input.description,
    invoke: async (raw, context) => {
      const decoded = await input.decode(raw);
      if (decoded.issues !== undefined)
        throw new InvalidOperationInput(decoded.issues);
      const result = await definition.execute(decoded.value, context);
      if (output === undefined) return result;
      const encoded = await output.encode(result);
      if (encoded.issues !== undefined)
        throw new BadResponse({
          reason: "Capability output failed its declared schema",
          rawCause: new InvalidOperationOutput(encoded.issues),
        });
      return encoded.value;
    },
  };
  if (definition.description !== undefined)
    captured.description = definition.description;
  if (output !== undefined) captured.output = output.description;
  const stored: StoredOperation = Object.freeze(captured);
  operationDefinitions.set(registration, stored);
  return registration;
}

/** Construct an opaque module handle and retain a frozen declaration copy. */
export function defineModule(definition: ModuleDefinition): ModuleRegistration {
  const namespace = z
    .array(sandboxNamespaceSegmentSchema)
    .safeParse(definition.namespace ?? []);
  if (!namespace.success)
    throw new MyceliumDefinitionError("Invalid module namespace");
  const registration: ModuleRegistration = Object.freeze({
    [moduleBrand]: true,
  });
  const captured: CapturedModule = {
    id: definition.id,
    grantKey: definition.id,
    name: definition.name ?? definition.id,
    namespace: Object.freeze(namespace.data),
    operations: Object.freeze({ ...definition.operations }),
  };
  if (definition.description !== undefined)
    captured.description = definition.description;
  const stored: StoredModule = Object.freeze(captured);
  moduleDefinitions.set(registration, stored);
  return registration;
}

/** Distinguish registered child modules without exposing their private declarations. */
export function isModuleRegistration(
  registration: OperationRegistration | ModuleRegistration,
): registration is ModuleRegistration {
  return moduleDefinitions.has(registration);
}

/** Resolve private operation data for a compiler-owned handle. */
export function readOperation(
  registration: OperationRegistration,
): StoredOperation | undefined {
  return operationDefinitions.get(registration);
}

/** Resolve private module data for a compiler-owned handle. */
export function readModule(
  registration: ModuleRegistration,
): StoredModule | undefined {
  return moduleDefinitions.get(registration);
}

/** A gate's verdict for one call: run the operation, or return `value` instead. */
export type GateDecision =
  | Readonly<{ kind: "allow" }>
  | Readonly<{ kind: "refuse"; value: MyceliumJsonValue }>;

/** Asked before every call of a gated module's operations. */
export type OperationGate = (
  call: Readonly<{ operation: string; context: InvocationContext }>,
) => GateDecision | Promise<GateDecision>;

/**
 * Wrap every operation of one module behind a host gate, keeping its
 * identity, alias, description and schemas. A refused call never reaches the
 * operation (not even its input decoder) and returns the gate's typed value.
 */
function gateModuleAt(
  registration: ModuleRegistration,
  gate: OperationGate,
  prefix: string,
): ModuleRegistration {
  const stored = readModule(registration);
  if (stored === undefined)
    throw new MyceliumDefinitionError("Unknown module registration");
  const operations: Record<string, OperationRegistration | ModuleRegistration> =
    {};
  for (const [name, handle] of Object.entries(stored.operations)) {
    const operation = prefix === "" ? name : `${prefix}.${name}`;
    if (isModuleRegistration(handle)) {
      const child = readModule(handle);
      if (child === undefined)
        throw new MyceliumDefinitionError("Unknown child module registration");
      operations[name] = gateModuleAt(
        handle,
        gate,
        [operation, ...child.namespace].join("."),
      );
      continue;
    }
    const inner = readOperation(handle);
    if (inner === undefined)
      throw new MyceliumDefinitionError(
        `Unknown operation registration: ${name}`,
      );
    const gated: OperationRegistration = Object.freeze({
      [operationBrand]: true,
    });
    operationDefinitions.set(
      gated,
      Object.freeze({
        ...inner,
        invoke: async (input: SandboxValue, context: InvocationContext) => {
          const decision = await gate({ operation, context });
          return decision.kind === "refuse"
            ? decision.value
            : await inner.invoke(input, context);
        },
      }),
    );
    operations[name] = gated;
  }
  const definition: ModuleDefinition =
    stored.description === undefined
      ? {
          id: stored.id,
          name: stored.name,
          namespace: stored.namespace,
          operations,
        }
      : {
          id: stored.id,
          name: stored.name,
          namespace: stored.namespace,
          description: stored.description,
          operations,
        };
  return withGrantKey(defineModule(definition), stored.grantKey);
}

/** Copy one module registration under a different grant key. */
function withGrantKey(
  registration: ModuleRegistration,
  grantKey: string,
): ModuleRegistration {
  const stored = readModule(registration);
  if (stored === undefined)
    throw new MyceliumDefinitionError("Unknown module registration");
  const copy: ModuleRegistration = Object.freeze({ [moduleBrand]: true });
  moduleDefinitions.set(copy, Object.freeze({ ...stored, grantKey }));
  return copy;
}

/**
 * Look this module's grant up by `grantKey` instead of its id, for a host
 * that grants by a wider owner, such as the extension that built the module.
 */
export function grantModuleAs(
  registration: ModuleRegistration,
  grantKey: string,
): ModuleRegistration {
  if (grantKey.length === 0 || grantKey.length > 160)
    throw new MyceliumDefinitionError(
      "Grant key must be non-empty and bounded",
    );
  return withGrantKey(registration, grantKey);
}

/** Gate an entire module tree while preserving its authority identity. */
export function gateModule(
  registration: ModuleRegistration,
  gate: OperationGate,
): ModuleRegistration {
  return gateModuleAt(registration, gate, "");
}
