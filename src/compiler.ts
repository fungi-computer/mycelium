import {
  createCatalog,
  forageDescribeDefinition,
  forageSearchDefinition,
  forageListDefinition,
  type ForageCatalog,
  type Operation,
} from "@fungi.computer/forage";

import {
  sandboxNamespaceSegmentSchema,
  sandboxTransportAliasSchema,
} from "./sandbox-guest.js";
import { MyceliumDefinitionError } from "./errors.js";
import {
  readModule,
  isModuleRegistration,
  readOperation,
  type ModuleRegistration,
  type StoredModule,
  type StoredOperation,
} from "./registration.js";

/** One deterministically named user operation in an installed capability surface. */
export type CompiledOperation = Readonly<{
  /** Grant key of the top-level module whose grant decides this operation. */
  readonly grantKey: string;
  readonly path: string;
  readonly namespace: string;
  readonly name: string;
  readonly moduleDescription: string | undefined;
  readonly operation: StoredOperation;
}>;

/** Immutable catalog and user-operation graph installed as one revision. */
export type CompiledSurface = Readonly<{
  readonly revision: string;
  readonly catalog: ForageCatalog;
  readonly operations: readonly CompiledOperation[];
  /** Every allocated execution alias mapped to its stable module id. */
  readonly moduleAliases: Readonly<Record<string, string>>;
  readonly namespacePaths: Readonly<Record<string, readonly string[]>>;
}>;

/** Secret-free source identity included only in the opaque installed revision. */
export type SourceRevisionFrame = Readonly<{
  readonly id: string;
  readonly version: string;
}>;

function compareCodeUnits(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}

function definitionMessage(cause: unknown): string {
  return cause instanceof Error
    ? cause.message
    : "Capability declarations could not be compiled";
}

const maximumModuleIdLength = 160;

function assertModuleId(value: string): void {
  if (value.length === 0 || value.length > maximumModuleIdLength) {
    throw new MyceliumDefinitionError(
      "Module id must be non-empty and bounded",
    );
  }
}

function assertOperationName(value: string): void {
  if (
    !value
      .split(".")
      .every(
        (segment) => sandboxNamespaceSegmentSchema.safeParse(segment).success,
      )
  ) {
    throw new MyceliumDefinitionError(
      "Operation key must be a path segment or a dotted relative path",
    );
  }
}

function namespaceName(name: string): string {
  const sanitized = name.replace(/[^A-Za-z0-9_]/g, "_");
  return /^[A-Za-z]/.test(sanitized) ? sanitized : `module_${sanitized}`;
}

function executionAlias(name: string): string {
  const segment = namespaceName(name);
  return sandboxTransportAliasSchema.safeParse(segment).success
    ? segment
    : `module_${segment}`;
}

function allocateAliases(
  modules: readonly StoredModule[],
  scope: "transport" | "logical",
): Map<string, string> {
  const used = new Set<string>(["forage", "env"]);
  const aliases = new Map<string, string>();
  for (const module of modules) {
    const prefix = scope === "logical" ? module.namespace : [];
    const base =
      scope === "logical"
        ? namespaceName(module.name)
        : executionAlias(module.name);
    const fullPath = (name: string) => [...prefix, name].join(".");
    let alias = base;
    let suffix = 2;
    while (used.has(fullPath(alias))) {
      alias = `${base}_${String(suffix)}`;
      suffix += 1;
    }
    used.add(fullPath(alias));
    aliases.set(module.id, alias);
  }
  return aliases;
}

const fnvOffsetBasis = 0xcbf29ce484222325n;
const fnvPrime = 0x100000001b3n;
const unsigned64Mask = 0xffffffffffffffffn;

/** Hash one length-framed UTF-16 string into the bounded installed identity. */
function hashFrame(hash: bigint, value: string): bigint {
  let result = hash;
  let length = BigInt(value.length);
  for (let index = 0; index < 8; index += 1) {
    result = ((result ^ (length & 0xffn)) * fnvPrime) & unsigned64Mask;
    length >>= 8n;
  }
  for (let index = 0; index < value.length; index += 1) {
    const codeUnit = value.charCodeAt(index);
    result = ((result ^ BigInt(codeUnit & 0xff)) * fnvPrime) & unsigned64Mask;
    result =
      ((result ^ BigInt((codeUnit >>> 8) & 0xff)) * fnvPrime) & unsigned64Mask;
  }
  return result;
}

/** Keep content identity and installed stable-id alias identity distinct. */
function installedRevision(
  contentStamp: string,
  aliases: ReadonlyMap<string, string>,
  sourceFrames: readonly SourceRevisionFrame[],
): string {
  let hash = hashFrame(fnvOffsetBasis, contentStamp);
  const mapping = [...aliases.entries()].sort(([left], [right]) =>
    compareCodeUnits(left, right),
  );
  for (const [stableId, alias] of mapping) {
    hash = hashFrame(hash, stableId);
    hash = hashFrame(hash, alias);
  }
  const frames = [...sourceFrames].sort((left, right) =>
    compareCodeUnits(left.id, right.id),
  );
  for (const frame of frames) {
    hash = hashFrame(hash, frame.id);
    hash = hashFrame(hash, frame.version);
  }
  return `installed-${hash.toString(16).padStart(16, "0")}`;
}

function intrinsicOperations(): readonly Operation[] {
  return [
    forageDescribeDefinition,
    forageSearchDefinition,
    forageListDefinition,
  ];
}

/** Flatten child modules beneath their parent's single authority identity. */
function flattenOperations(
  grantKey: string,
  module: StoredModule,
  namespace: string,
  path: readonly string[],
  description: string | undefined,
  names: readonly string[] = [],
): CompiledOperation[] {
  return Object.entries(module.operations).flatMap(([name, registration]) => {
    assertOperationName(name);
    if (isModuleRegistration(registration)) {
      const child = readModule(registration);
      if (child === undefined)
        throw new MyceliumDefinitionError("Unknown child module registration");
      return flattenOperations(
        grantKey,
        child,
        namespace,
        [...path, name, ...child.namespace],
        child.description ?? description,
        [...names, name, ...child.namespace],
      );
    }
    const operation = readOperation(registration);
    if (operation === undefined)
      throw new MyceliumDefinitionError(
        `Unknown operation registration: ${[...path, name].join(".")}`,
      );
    return [
      Object.freeze({
        grantKey,
        path: [...path, name].join("."),
        namespace,
        name: [...names, name].join("."),
        moduleDescription: description,
        operation,
      }),
    ];
  });
}

/** Compile opaque declarations and Forage intrinsics into one immutable surface. */
export function compileSurface(
  registrations: readonly ModuleRegistration[],
  sourceFrames: readonly SourceRevisionFrame[] = [],
): CompiledSurface {
  const modules = registrations
    .map((registration) => {
      const module = readModule(registration);
      if (module === undefined) {
        throw new MyceliumDefinitionError("Unknown module registration");
      }
      assertModuleId(module.id);
      return module;
    })
    .sort((left, right) => compareCodeUnits(left.id, right.id));
  const ids = new Set<string>();
  for (const module of modules) {
    if (ids.has(module.id)) {
      throw new MyceliumDefinitionError(`Duplicate module id: ${module.id}`);
    }
    ids.add(module.id);
  }
  const aliases = allocateAliases(modules, "transport");
  const logicalAliases = allocateAliases(modules, "logical");
  const namespacePaths = Object.freeze(
    Object.fromEntries(
      modules.map((module) => {
        const alias = aliases.get(module.id);
        if (alias === undefined)
          throw new MyceliumDefinitionError(
            `Missing module alias: ${module.id}`,
          );
        const logical = logicalAliases.get(module.id);
        if (logical === undefined)
          throw new MyceliumDefinitionError(
            `Missing logical alias: ${module.id}`,
          );
        return [alias, Object.freeze([...module.namespace, logical])];
      }),
    ),
  );
  const operations = modules
    .flatMap((module) => {
      const alias = aliases.get(module.id);
      if (alias === undefined)
        throw new MyceliumDefinitionError(`Missing module alias: ${module.id}`);
      const path = namespacePaths[alias];
      if (path === undefined)
        throw new MyceliumDefinitionError(
          `Missing namespace path: ${module.id}`,
        );
      return flattenOperations(
        module.grantKey,
        module,
        alias,
        path,
        module.description,
      );
    })
    .sort((left, right) => compareCodeUnits(left.path, right.path));
  const operationPaths = new Set(operations.map((operation) => operation.path));
  if (operationPaths.size !== operations.length)
    throw new MyceliumDefinitionError("Duplicate operation path");
  for (const intrinsic of intrinsicOperations()) {
    if (operationPaths.has(intrinsic.path))
      throw new MyceliumDefinitionError(
        `Reserved intrinsic path: ${intrinsic.path}`,
      );
    operationPaths.add(intrinsic.path);
  }
  for (const path of operationPaths) {
    const segments = path.split(".");
    for (let length = 1; length < segments.length; length += 1)
      if (operationPaths.has(segments.slice(0, length).join(".")))
        throw new MyceliumDefinitionError(
          `Operation conflicts with namespace: ${path}`,
        );
  }

  let prepared: ForageCatalog;
  try {
    prepared = createCatalog({
      revision: "prepared-content-stamp",
      operations: [
        ...operations.map(
          ({ path, moduleDescription, operation }): Operation => {
            const catalogOperation: Operation = {
              path,
              input: operation.input,
            };
            if (operation.description !== undefined) {
              catalogOperation.description = operation.description;
            }
            if (operation.output !== undefined) {
              catalogOperation.output = operation.output;
            }
            if (moduleDescription !== undefined) {
              catalogOperation.instructions = moduleDescription;
            }
            return catalogOperation;
          },
        ),
        ...intrinsicOperations(),
      ],
    });
  } catch (cause) {
    throw new MyceliumDefinitionError(definitionMessage(cause));
  }
  const revision = installedRevision(prepared.stamp(), aliases, sourceFrames);
  const catalog: ForageCatalog = Object.freeze({
    revision,
    search: prepared.search,
    describe: prepared.describe,
    list: prepared.list,
    stamp: prepared.stamp,
  });
  const moduleAliases = Object.freeze(
    Object.fromEntries(
      [...aliases.entries()].map(([moduleId, alias]) => [alias, moduleId]),
    ),
  );
  return Object.freeze({
    revision,
    catalog,
    operations: Object.freeze(operations),
    moduleAliases,
    namespacePaths,
  });
}
