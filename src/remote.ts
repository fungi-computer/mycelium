import { jsonSchemaToZod } from "@fungi.computer/forage";
import { z } from "zod";

import type {
  InvocationContext,
  MyceliumJsonValue,
  SandboxValue,
} from "./index.js";
import { MyceliumDefinitionError } from "./errors.js";
import {
  defineModule,
  defineOperation,
  isModuleRegistration,
  readModule,
  readOperation,
  type ModuleRegistration,
  type OperationRegistration,
  type StoredOperation,
} from "./registration.js";

/** One operation's model-facing contract with its schemas as JSON Schema. */
export type RemoteOperationDescriptor = Readonly<{
  replay: "safe" | "unsafe";
  description?: string | undefined;
  input: MyceliumJsonValue;
  /** Absent when the operation declares no output schema. */
  output: MyceliumJsonValue | null;
}>;

/**
 * A module's operations keyed by their dotted path below the module, for a
 * host that runs the module in another isolate. Identity, alias and placement
 * stay the host's decision; this carries only what the model may call.
 */
export type RemoteModuleDescriptor = Readonly<{
  description?: string | undefined;
  operations: Readonly<Record<string, RemoteOperationDescriptor>>;
}>;

function operationsOf(
  registration: ModuleRegistration,
): ReadonlyMap<string, StoredOperation> {
  const stored = readModule(registration);
  if (stored === undefined)
    throw new MyceliumDefinitionError("Unknown module registration");
  const operations = new Map<string, StoredOperation>();
  const visit = (
    handles: Readonly<
      Record<string, OperationRegistration | ModuleRegistration>
    >,
    prefix: readonly string[],
  ) => {
    for (const [name, handle] of Object.entries(handles)) {
      if (isModuleRegistration(handle)) {
        const child = readModule(handle);
        if (child === undefined)
          throw new MyceliumDefinitionError(
            "Unknown child module registration",
          );
        visit(child.operations, [...prefix, name, ...child.namespace]);
        continue;
      }
      const operation = readOperation(handle);
      if (operation === undefined)
        throw new MyceliumDefinitionError(
          `Unknown operation registration: ${name}`,
        );
      operations.set([...prefix, name].join("."), operation);
    }
  };
  visit(stored.operations, []);
  return operations;
}

/** Project one module's callable surface; unrepresentable schemas throw. */
export function describeModule(
  registration: ModuleRegistration,
): RemoteModuleDescriptor {
  const stored = readModule(registration);
  if (stored === undefined)
    throw new MyceliumDefinitionError("Unknown module registration");
  const operations: Record<string, RemoteOperationDescriptor> = {};
  for (const [path, operation] of operationsOf(registration)) {
    const input = z.json().parse(
      operation.input["~standard"].jsonSchema.input({
        target: "draft-2020-12",
      }),
    );
    const output =
      operation.output === undefined
        ? null
        : z.json().parse(
            operation.output["~standard"].jsonSchema.output({
              target: "draft-2020-12",
            }),
          );
    operations[path] =
      operation.description === undefined
        ? { replay: operation.replay, input, output }
        : {
            replay: operation.replay,
            description: operation.description,
            input,
            output,
          };
  }
  return stored.description === undefined
    ? { operations }
    : { description: stored.description, operations };
}

/** Run one described operation through its own input and output schemas. */
export function invokeModuleOperation(
  registration: ModuleRegistration,
  path: string,
  input: SandboxValue,
  context: InvocationContext,
): Promise<MyceliumJsonValue> {
  const operation = operationsOf(registration).get(path);
  if (operation === undefined)
    return Promise.reject(
      new MyceliumDefinitionError(`Unknown remote operation: ${path}`),
    );
  return operation
    .invoke(input, context)
    .then((value) => z.json().parse(value));
}

type Tree = Map<string, RemoteOperationDescriptor | Tree>;

/**
 * Rebuild a described module on the host side. Each operation parses its input
 * and output against the described JSON Schema, then forwards to `invoke`.
 */
export function remoteModule(
  options: Readonly<{
    id: string;
    name: string;
    descriptor: RemoteModuleDescriptor;
    invoke: (
      path: string,
      input: MyceliumJsonValue,
      context: InvocationContext,
    ) => Promise<MyceliumJsonValue>;
  }>,
): ModuleRegistration {
  const root: Tree = new Map();
  for (const [path, operation] of Object.entries(
    options.descriptor.operations,
  )) {
    const segments = path.split(".");
    const leaf = segments.pop();
    if (leaf === undefined)
      throw new MyceliumDefinitionError("Empty remote operation path");
    let node = root;
    for (const segment of segments) {
      const next: RemoteOperationDescriptor | Tree =
        node.get(segment) ??
        new Map<string, RemoteOperationDescriptor | Tree>();
      if (!(next instanceof Map))
        throw new MyceliumDefinitionError(
          `Remote operation path collides with an operation: ${path}`,
        );
      node.set(segment, next);
      node = next;
    }
    if (node.has(leaf))
      throw new MyceliumDefinitionError(
        `Duplicate remote operation path: ${path}`,
      );
    node.set(leaf, operation);
  }
  const build = (
    tree: Tree,
    prefix: readonly string[],
  ): Record<string, OperationRegistration | ModuleRegistration> =>
    Object.fromEntries(
      [...tree].map(
        ([name, entry]): [
          string,
          OperationRegistration | ModuleRegistration,
        ] => {
          const path = [...prefix, name];
          if (entry instanceof Map)
            return [
              name,
              defineModule({
                id: `${options.id}.${path.join(".")}`,
                name,
                operations: build(entry, path),
              }),
            ];
          const output =
            entry.output === null
              ? undefined
              : z.intersection(jsonSchemaToZod(entry.output), z.json());
          const forward = (
            value: MyceliumJsonValue,
            context: InvocationContext,
          ) => options.invoke(path.join("."), value, context);
          const base = {
            replay: entry.replay,
            input: z.intersection(jsonSchemaToZod(entry.input), z.json()),
            execute: forward,
          };
          const described =
            entry.description === undefined
              ? base
              : { ...base, description: entry.description };
          return [
            name,
            defineOperation(
              output === undefined ? described : { ...described, output },
            ),
          ];
        },
      ),
    );
  return defineModule(
    options.descriptor.description === undefined
      ? { id: options.id, name: options.name, operations: build(root, []) }
      : {
          id: options.id,
          name: options.name,
          description: options.descriptor.description,
          operations: build(root, []),
        },
  );
}
