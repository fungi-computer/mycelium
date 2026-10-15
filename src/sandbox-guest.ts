import { parse } from "acorn";
import { z } from "zod";
import { sandboxSourceError } from "./sandbox-boundary.js";

/** Closed guest envelope: guest exceptions cannot masquerade as host failures. */
export const sandboxGuestResultSchema = z.discriminatedUnion("ok", [
  z.strictObject({ ok: z.literal(true), value: z.unknown() }),
  z.strictObject({
    ok: z.literal(false),
    error: z.strictObject({
      kind: z.literal("guest_threw"),
      name: z.string().max(80),
      message: z.string().max(512),
      transportMessage: z.string().max(512).optional(),
    }),
  }),
]);

/** Locate syntax in the submitted code, accounting for Codemode's normalization. */
export function submittedSourceLocation(
  normalized: string,
  submitted: string,
): Readonly<{
  line: number;
  column: number;
  excerpt: string;
}> | null {
  try {
    parse(`(${normalized})`, { ecmaVersion: "latest", sourceType: "module" });
    return null;
  } catch (cause) {
    const diagnostic = sandboxSourceError(cause);
    if (diagnostic === undefined) throw cause;
    const { pos } = diagnostic;
    const prelude = "async () => {\n";
    const source = normalized.startsWith(prelude)
      ? normalized.slice(prelude.length, -"\n}".length)
      : normalized;
    const start = submitted.indexOf(source);
    if (start < 0) throw cause;
    const offset = Math.min(
      submitted.length,
      Math.max(0, pos - 1 - normalized.indexOf(source) + start),
    );
    const prefix = submitted.slice(0, offset).split(/\r\n|[\n\r\u2028\u2029]/u);
    const line = prefix.length;
    return {
      line,
      column: (prefix.at(-1)?.length ?? 0) + 1,
      excerpt: (submitted.split(/\r\n|[\n\r\u2028\u2029]/u)[line - 1] ?? "")
        .replace(/\p{Cc}|\p{Cf}/gu, " ")
        .slice(0, 160),
    };
  }
}

/** Parsed guest namespace segments; callbacks never cross this wire. */
export const sandboxNamespaceSegmentSchema = z
  .string()
  .regex(/^[A-Za-z][A-Za-z0-9_]*$/u);
/** A transport alias is interpolated as a strict JavaScript binding identifier. */
export const sandboxTransportAliasSchema = sandboxNamespaceSegmentSchema.refine(
  (alias) => {
    if (alias === "env") return false;
    try {
      parse(`let ${alias};`, { ecmaVersion: "latest", sourceType: "module" });
      return true;
    } catch (cause) {
      if (!(cause instanceof SyntaxError)) throw cause;
      return false;
    }
  },
  "Invalid sandbox transport alias",
);

/** Transport aliases mapped to their logical namespace paths. */
export const sandboxNamespacePathsSchema = z.record(
  sandboxTransportAliasSchema,
  z.array(sandboxNamespaceSegmentSchema).min(1),
);
/** Serializable flat transport table; operation keys may have any namespace depth. */
export const sandboxBindingNamesSchema = z.record(
  sandboxTransportAliasSchema,
  z.array(
    z.string().regex(/^[A-Za-z][A-Za-z0-9_]*(?:\.[A-Za-z][A-Za-z0-9_]*)*$/u),
  ),
);

/** Serializable operation names; callbacks stay in the host's existing binding table. */
export function sandboxBindingNames<A>(
  bindings: Readonly<Record<string, Readonly<Record<string, A>>>>,
): Readonly<Record<string, readonly string[]>> {
  return Object.fromEntries(
    Object.entries(bindings).map(([namespace, operations]) => [
      namespace,
      Object.keys(operations),
    ]),
  );
}

type GuestNamespace = {
  kind: "namespace";
  children: Map<
    string,
    GuestNamespace | Readonly<{ kind: "operation"; call: string }>
  >;
};

/** Project compiler-owned paths without changing any callback or authority identity. */
function guestEnvironment(
  bindings: Readonly<Record<string, readonly string[]>>,
  namespacePaths: Readonly<Record<string, readonly string[]>>,
  transportName: (operation: string) => string,
): string {
  const root: GuestNamespace = { kind: "namespace", children: new Map() };
  const paths = sandboxNamespacePathsSchema.parse(namespacePaths);
  for (const [alias, operations] of Object.entries(
    sandboxBindingNamesSchema.parse(bindings),
  )) {
    for (const operation of operations) {
      const path = [...(paths[alias] ?? [alias]), ...operation.split(".")];
      let parent = root;
      for (const segment of path.slice(0, -1)) {
        const found = parent.children.get(segment);
        if (found?.kind === "operation")
          throw new Error("Operation conflicts with namespace");
        const child: GuestNamespace = found ?? {
          kind: "namespace",
          children: new Map(),
        };
        parent.children.set(segment, child);
        parent = child;
      }
      const name = path.at(-1);
      if (name === undefined || parent.children.has(name))
        throw new Error("Duplicate guest operation path");
      parent.children.set(name, {
        kind: "operation",
        call: `(...args) => ${alias}[${JSON.stringify(transportName(operation))}](...args)`,
      });
    }
  }
  const expression = (node: GuestNamespace): string =>
    `Object.freeze({${[...node.children]
      .map(
        ([name, child]) =>
          `[${JSON.stringify(name)}]: ${child.kind === "operation" ? child.call : expression(child)}`,
      )
      .join(",")}})`;
  return expression(root);
}

/** Injective provider identifier: Codemode sanitizes dotted function names. */
export function sandboxProviderOperationName(operation: string): string {
  return `operation_${Array.from(new TextEncoder().encode(operation), (byte) => byte.toString(16).padStart(2, "0")).join("")}`;
}

/** Capture name and text inside the guest, before Codemode erases Error.name. */
export function sandboxGuestProgram(
  normalized: string,
  bindings: Readonly<Record<string, readonly string[]>> = {},
  namespacePaths: Readonly<Record<string, readonly string[]>> = {},
  transportName?: (operation: string) => string,
): string {
  return `async () => {
    const env = ${guestEnvironment(bindings, namespacePaths, transportName ?? ((operation) => operation))};
    return await (async (${Object.keys(bindings).join(",")}) => {
    try { return { ok: true, value: await (${normalized})() }; }
    catch (cause) {
      const name = cause instanceof Error ? cause.name : "ThrownValue";
      const message = cause instanceof Error ? cause.message : String(cause);
      const transportMessage = cause instanceof Error && cause.cause instanceof Error ? cause.cause.message.slice(0, 512) : undefined;
      return { ok: false, error: { kind: "guest_threw", name: String(name).slice(0, 80), message: String(message).replace(/\\p{Cc}/gu, " ").slice(0, 512), ...(transportMessage === undefined ? {} : { transportMessage }) } };
    }
    })(${Object.keys(bindings)
      .map((alias) =>
        transportName === undefined
          ? alias
          : (namespacePaths[alias] ?? [alias]).reduce(
              (value, segment) => `${value}[${JSON.stringify(segment)}]`,
              "env",
            ),
      )
      .join(",")});
  }`;
}
