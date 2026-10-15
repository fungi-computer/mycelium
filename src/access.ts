import type { MuleTool } from "@fungi.computer/mule/effect";
import { MyceliumDefinitionError } from "./errors.js";
import type { SandboxValue } from "./index.js";
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

/**
 * What one operation does, declared by its author as a fixed fact: `read`
 * observes, `write` changes something. An unlabelled operation is `write`.
 */
export const ToolAccessSchema = Schema.Literals(["read", "write"]);
/** What one operation does: `read` observes, `write` changes something. */
export type Access = typeof ToolAccessSchema.Type;

/** Whether the author asks that a person approve each call first. */
export const ToolConfirmSchema = Schema.Boolean;

/**
 * An author's labels for one operation or direct tool: the one definition
 * both the TypeScript type and the registration-time parse derive from.
 */
export const ToolPolicySchema = Schema.Struct({
  access: Schema.optional(ToolAccessSchema),
  confirm: Schema.optional(ToolConfirmSchema),
});
/** An author's labels for one operation or direct tool. */
export type ToolPolicy = typeof ToolPolicySchema.Type;

/**
 * The access a host grants one module: `write` allows every operation,
 * `read` allows only `read` operations, `none` allows nothing.
 */
export const GrantSchema = Schema.Literals(["none", "read", "write"]);
/** The access a host grants one module. */
export type Grant = typeof GrantSchema.Type;

/**
 * A host's grants, keyed by module id. An unnamed module uses defaultGrant
 * unless it is required, in which case missing authority is refused.
 */
export type Grants = Readonly<Record<string, Grant>>;

/** The labels an operation or direct tool carries, as its author declared them. */
export type AccessLabel = Readonly<{
  /** Stable module id the grant is looked up by. */
  module: string;
  /** Operation path inside the module, or the direct tool's name. */
  operation: string;
  access: Access;
  /** The author asks that a person approve each call first. */
  confirm: boolean;
}>;

/** The Session a runtime serves; null for a session-free host. */
export type AccessContext = Readonly<{ sessionId: string | null }>;

/** One call the host is asked to approve before it runs. */
export type ConfirmRequest = Readonly<
  Omit<AccessLabel, "confirm"> &
    AccessContext & {
      /** The requested input exactly as the caller supplied it, not yet decoded. */
      input: SandboxValue;
    }
>;

/** Why a call was refused before it ran. */
export type AccessRefusalReason =
  "not-granted" | "read-only" | "confirmation-unavailable" | "declined";

/** The ordinary result a refused call returns in place of running. */
export type AccessRefusal = Readonly<{
  refused: true;
  module: string;
  operation: string;
  reason: AccessRefusalReason;
  message: string;
}>;

/**
 * Host-supplied access data and the one way to ask a person. Every field is
 * optional: hosted defaults allow unnamed modules, and no `confirm` means an
 * operation requiring approval is refused, never run unasked. MCP catalogs
 * explicitly default unnamed modules to none and require approval for writes.
 */
export type AccessOptions = Readonly<{
  /** Grant for unnamed modules; MCP catalogs use none. Defaults to write for hosted callers. */
  defaultGrant?: Grant;
  /** The default grants as plain data; absent is full access. */
  grants?: Grants;
  /**
   * Module keys under grant control. A required key that neither the Session
   * grants nor the default names is refused, so a missing delivery fails
   * closed. Other unnamed keys use defaultGrant.
   */
  required?: readonly string[];
  /**
   * Read the current Session's stored grants at every call; they override
   * `grants` key by key, so a revoked grant applies to the very next call.
   */
  sessionGrants?: (context: AccessContext) => Grants;
  /** Refresh authority before a call and again after approval. Failure refuses execution. */
  beforeCall?: (signal: AbortSignal) => Promise<void>;
  /** Require approval for every write, including operations without an explicit confirm label. */
  confirmWrites?: boolean;
  /**
   * Read, at every call, the module ids whose writes a person already
   * approved for this caller. Such a write needs no per-call `confirm`; an
   * operation its author labelled `confirm` still asks, and grants still apply.
   */
  approvedWrites?: (context: AccessContext) => readonly string[];
  /** Ask a person to approve one call; resolve true to run it. */
  confirm?:
    | ((request: ConfirmRequest, signal: AbortSignal) => Promise<boolean>)
    | undefined;
  /** Operator-only observer of every refusal; never changes the decision. */
  onRefused?: (refusal: AccessRefusal, context: AccessContext) => void;
}>;

/** A Mule tool that carries its author's access labels outside execute. */
export type LabelledTool = MuleTool &
  Readonly<{
    /** Stable module id the grant is looked up by. */
    module: string;
    /** Unlabelled means `write`. */
    access?: Access;
    /** Ask a person before each call. */
    confirm?: boolean;
  }>;

const decodeToolPolicy = Schema.decodeUnknownResult(ToolPolicySchema);
const decodeGrant = Schema.decodeUnknownResult(GrantSchema);

/**
 * Parse an author's labels at definition. Only the exact `"read"` and
 * `"write"` and a boolean `confirm` are labels; anything else is a definition
 * error, never quietly read as a weaker label.
 */
export function parseAccessLabels(
  labels: Readonly<{ access?: SandboxValue; confirm?: SandboxValue }>,
): Pick<AccessLabel, "access" | "confirm"> {
  const parsed = decodeToolPolicy({
    access: labels.access,
    confirm: labels.confirm,
  });
  if (Result.isFailure(parsed))
    throw new MyceliumDefinitionError(
      'Access labels must be access: "read" | "write" and confirm: boolean',
    );
  return {
    access: parsed.success.access ?? "write",
    confirm: parsed.success.confirm ?? false,
  };
}

function messageFor(
  reason: AccessRefusalReason,
  label: Pick<AccessLabel, "module" | "operation">,
): string {
  const name = `${label.module}.${label.operation}`;
  switch (reason) {
    case "not-granted":
      return `${name} is not granted to this Agent.`;
    case "read-only":
      return `${name} changes something, and this Agent has read-only access to ${label.module}.`;
    case "confirmation-unavailable":
      return `${name} needs a person's approval, and no one can be asked here.`;
    case "declined":
      return `${name} was not approved.`;
  }
}

function refusal(
  reason: AccessRefusalReason,
  label: Pick<AccessLabel, "module" | "operation">,
): AccessRefusal {
  return Object.freeze({
    refused: true,
    module: label.module,
    operation: label.operation,
    reason,
    message: messageFor(reason, label),
  });
}

/**
 * The grant for one module. An unnamed module uses the configured default
 * unless it is required; a present entry that is not a grant refuses, so a
 * malformed host value fails closed.
 */
function grantFor(
  options: AccessOptions,
  context: AccessContext,
  module: string,
): Grant {
  const session = options.sessionGrants?.(context);
  const owner =
    session !== undefined && Object.hasOwn(session, module)
      ? session
      : options.grants;
  if (owner === undefined || !Object.hasOwn(owner, module))
    return options.required?.includes(module) === true
      ? "none"
      : (options.defaultGrant ?? "write");
  const parsed = decodeGrant(owner[module]);
  return Result.isSuccess(parsed) ? parsed.success : "none";
}

/**
 * Decide one call against its labels and the host's current grants: the grant
 * first, then the person. Returns the refusal, or undefined to run the call.
 */
export async function checkAccess(
  options: AccessOptions,
  context: AccessContext,
  label: AccessLabel,
  input: SandboxValue,
  signal: AbortSignal,
): Promise<AccessRefusal | undefined> {
  const decided = await decide(options, context, label, input, signal);
  if (decided !== undefined) options.onRefused?.(decided, context);
  return decided;
}

async function decide(
  options: AccessOptions,
  context: AccessContext,
  label: AccessLabel,
  input: SandboxValue,
  signal: AbortSignal,
): Promise<AccessRefusal | undefined> {
  await options.beforeCall?.(signal);
  signal.throwIfAborted();
  const granted = grantRefusal(options, context, label);
  if (granted !== undefined) return granted;
  if (
    !label.confirm &&
    !(options.confirmWrites === true && label.access !== "read")
  )
    return undefined;
  if (
    !label.confirm &&
    options.approvedWrites?.(context).includes(label.module) === true
  )
    return undefined;
  if (options.confirm === undefined)
    return refusal("confirmation-unavailable", label);
  const approved = await options.confirm(
    {
      module: label.module,
      operation: label.operation,
      access: label.access,
      sessionId: context.sessionId,
      input,
    },
    signal,
  );
  if (!approved) return refusal("declined", label);
  await options.beforeCall?.(signal);
  signal.throwIfAborted();
  // A grant revoked while the person was deciding still applies.
  return grantRefusal(options, context, label);
}

/** The refusal the current grant gives this label, if any. */
function grantRefusal(
  options: AccessOptions,
  context: AccessContext,
  label: AccessLabel,
): AccessRefusal | undefined {
  const grant = grantFor(options, context, label.module);
  if (grant === "none") return refusal("not-granted", label);
  // Only an exact "read" label is a read; every other operation writes.
  if (grant === "read" && label.access !== "read")
    return refusal("read-only", label);
  return undefined;
}

/**
 * Put one labelled direct tool behind the host's access options. A refused
 * call never reaches the tool's decoder and returns the refusal as its
 * ordinary result, as a refused operation does inside execute.
 */
export function guardTool(
  tool: LabelledTool,
  options: AccessOptions,
  context: AccessContext,
): MuleTool {
  const label: AccessLabel = {
    module: tool.module,
    operation: tool.descriptor.name,
    ...parseAccessLabels(tool),
  };
  return {
    ...tool,
    prepare: (call) =>
      Effect.tryPromise({
        try: (signal) =>
          checkAccess(options, context, label, call.arguments, signal),
        // Host failures stay out of the model's view; the call fails closed.
        catch: () => ({ message: "Access check failed" }),
      }).pipe(
        Effect.flatMap((refused) =>
          refused === undefined
            ? tool.prepare(call)
            : Effect.succeed({
                execute: () =>
                  Stream.succeed({
                    type: "result" as const,
                    result: {
                      content: [
                        {
                          type: "text" as const,
                          text: JSON.stringify(refused),
                        },
                      ],
                      isError: false,
                    },
                  }),
              }),
        ),
      ),
  };
}
