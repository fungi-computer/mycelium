export {
  sandboxFailure,
  isSandboxFailure,
  sandboxOperatorValue,
} from "./sandbox-boundary.js";
import type { ForageCatalog } from "@fungi.computer/forage";
import type { MuleTool } from "@fungi.computer/mule/effect";
import type { Sandbox, MyceliumJsonValue } from "./sandbox.js";
export type {
  MyceliumJsonValue,
  SandboxValue,
  SandboxBinding,
  SandboxRequest,
  Sandbox,
} from "./sandbox.js";
import type * as Schema from "effect/Schema";
import type { OperationSchema } from "./operation-schema.js";

import type {
  CapabilitySession,
  CapabilitySource,
} from "./capability-source.js";
import type { SandboxFailureEvent } from "./sandbox-failure-event.js";
import type { ExecutionImages } from "./image-collector.js";
import type { ExecutionOptions } from "./execute-tool.js";
import type { Access, AccessOptions, LabelledTool } from "./access.js";
import { openPromiseRuntime } from "./promise-facade.js";
import {
  defineModule,
  defineOperation,
  gateModule,
  grantModuleAs,
  type ModuleRegistration,
  type OperationGate,
  type OperationRegistration,
} from "./registration.js";

export type {
  CapabilitySession,
  CapabilitySource,
  CapabilitySourceAcquisition,
  CapabilitySourceContext,
  CapabilitySourceSnapshot,
} from "./capability-source.js";
export {
  InvalidOperationInput,
  InvalidOperationOutput,
  MyceliumClosedError,
  MyceliumDefinitionError,
  MyceliumSourceError,
  type MyceliumError,
  type MyceliumSourcePhase,
} from "./errors.js";
export {
  SourceInvalid,
  GuestThrew,
  DeadlineExceeded,
  Cancelled,
  DispatchLost,
  SandboxCrashed,
  BadResponse,
  ResultTooLarge,
  ResultNotJson,
  CapabilityFailed,
  Unknown,
  sandboxFailureMessage,
  type SandboxFailure,
  type CapabilityDiagnostic,
  type CapabilityFailureFields,
} from "./sandbox-failure.js";
export {
  SandboxFailureEventSchema,
  type SandboxFailureEvent,
} from "./sandbox-failure-event.js";
export type { ExecutionOptions } from "./execute-tool.js";
export {
  GrantSchema,
  ToolAccessSchema,
  ToolConfirmSchema,
  ToolPolicySchema,
} from "./access.js";
export type {
  Access,
  AccessContext,
  AccessLabel,
  AccessOptions,
  AccessRefusal,
  AccessRefusalReason,
  ConfirmRequest,
  Grant,
  Grants,
  LabelledTool,
  ToolPolicy,
} from "./access.js";
export {
  MAX_EXECUTION_IMAGES,
  type CollectedImage,
  type ExecutionImages,
} from "./image-collector.js";
export type {
  OperationSchema,
  OperationSchemaIssue,
  OperationSchemaResult,
  StandardOperationSchema,
} from "./operation-schema.js";
export type {
  GateDecision,
  ModuleRegistration,
  OperationGate,
  OperationRegistration,
} from "./registration.js";

/** The only context an operation callback may observe. */
export type InvocationContext = Readonly<{
  /** Fresh identity for this invocation, independent from Mule's tool-call ID. */
  executionId: string;
  /** Enclosing model tool call; absent for direct operation/execute calls. */
  toolCallId?: string;
  /** Enclosing sandbox program supplied by the host; never an operation argument. */
  toolCallCode?: string;
  /** Absolute epoch-millisecond execute deadline; null for direct calls outside execute. */
  deadlineAt: number | null;
  /** Cooperative cancellation signal owned by the active Mule run. */
  signal: AbortSignal;
  /**
   * This execution's image collector: an operation that lets the model look
   * at an image pushes it here, and `execute` appends it as image content.
   * Absent when a host invokes an operation directly, outside `execute`,
   * where no tool result would carry the image.
   */
  images?: ExecutionImages;
}>;

/** Inputs used to construct the Promise-facing runtime. */
export type MyceliumOptions = Readonly<{
  /** Unordered module registrations compiled into one immutable revision. */
  modules?: readonly ModuleRegistration[];
  /** Versioned host projections reconciled into immutable revisions. */
  sources?: readonly CapabilitySource[];
  /** Session every source acquisition is made for; absent for session-free hosts. */
  session?: CapabilitySession;
  /** Bounded Mycelium-owned deadline and cancellation acknowledgement policy. */
  execution?: ExecutionOptions;
  /** Host-owned execution boundary for model-authored programs. */
  sandbox: Sandbox;
  /** Operator-only failure sink; never contributes a model message. */
  onSandboxFailure?: (event: SandboxFailureEvent) => Promise<void>;
  /** Labelled direct tools offered beside execute under the same access check. */
  tools?: readonly LabelledTool[];
  /** Host grants and approval, checked against each call's labels. */
  access?: AccessOptions;
}>;

/** Cancellation options for one Promise-facing lease acquisition. */
export type AcquireOptions = Readonly<{
  /** Optional cancellation signal for lease acquisition. */
  signal?: AbortSignal;
}>;

/** One immutable Promise-facing capability revision. */
export type Lease = Readonly<{
  /** Installed capability revision captured by this immutable lease. */
  revision: string;
  /** Bounded discovery catalog for the captured revision. */
  catalog: ForageCatalog;
  /** Single Mule tool that exposes discovery and scoped execution. */
  executeTool: MuleTool;
  /** The runtime's labelled direct tools, each behind the access check. */
  tools: readonly MuleTool[];
  /** Release the lease; safe to await more than once. */
  release: () => Promise<void>;
}>;

/** Warm Promise-facing lifecycle for acquiring immutable run leases. */
export type Runtime = Readonly<{
  /** Acquire one immutable lease from the currently installed revision. */
  acquire: (options?: AcquireOptions) => Promise<Lease>;
  /** Stop new acquisition and await runtime-owned cleanup. */
  close: () => Promise<void>;
}>;

/**
 * Declaration for one operation behind the execute door. `input` and `output`
 * may be Effect schemas (encoded as JSON) or any Standard Schema with
 * Standard JSON Schema, such as Zod 4.
 */
export type OperationDefinition<I, O> = Readonly<{
  /** Declaration for inventory; the enclosing model-facing execute tool remains unsafe. */
  replay?: "safe" | "unsafe";
  /**
   * What this operation does: `read` observes, `write` changes something.
   * A fixed fact the author owns; unlabelled is `write`. A host's `read`
   * grant for this module runs only `read` operations.
   */
  access?: Access;
  /** Ask a person to approve each call first; refused when no one can be asked. */
  confirm?: boolean;
  /** Bounded discovery text for the model. */
  description?: string;
  /** Schema that decodes untrusted operation input. */
  input: OperationSchema<I>;
  /** Optional schema that encodes (Effect) or validates (Standard) the result. */
  output?: OperationSchema<O>;
  /** Host implementation invoked only after input validation. */
  execute: (input: I, context: InvocationContext) => Promise<O>;
}>;

/** Declaration for one stable module and its related operations. */
export type ModuleDefinition = Readonly<{
  /** Stable authority identity; independent from the model-facing alias. */
  id: string;
  /** Optional friendly name used to derive the model-facing alias. */
  name?: string;
  /** Parent namespaces in the guest environment, independent of authority identity. */
  namespace?: readonly string[];
  /** Bounded namespace instructions projected into Forage. */
  description?: string;
  /** Operations or child modules, keyed by a path segment or a dotted relative path such as `repos.list` (the guest calls it as `env.<module>.repos.list()`). A key that is both an operation and a namespace prefix is refused. */
  operations: Readonly<
    Record<string, OperationRegistration | ModuleRegistration>
  >;
}>;

function operation<I, O>(
  definition: OperationDefinition<I, O> & {
    readonly output: Schema.Codec<O, unknown>;
  },
): OperationRegistration<I, O>;
function operation<I, O extends MyceliumJsonValue>(
  definition: OperationDefinition<I, O>,
): OperationRegistration<I, O>;
function operation<I, O>(
  definition: OperationDefinition<I, O>,
): OperationRegistration<I, O> {
  return defineOperation(definition);
}

function module(definition: ModuleDefinition): ModuleRegistration {
  return defineModule(definition);
}

/** Public convenience namespace contract. */
export type MyceliumApi = Readonly<{
  /** Declare one namespaced collection of related operations. */
  module: (definition: ModuleDefinition) => ModuleRegistration;
  /**
   * Declare one schema-described operation behind the execute door. An Effect
   * output schema encodes the result to JSON; otherwise the result must
   * already be a JSON value.
   */
  operation: {
    <I, O>(
      definition: OperationDefinition<I, O> & {
        readonly output: Schema.Codec<O, unknown>;
      },
    ): OperationRegistration<I, O>;
    <I, O extends MyceliumJsonValue>(
      definition: OperationDefinition<I, O>,
    ): OperationRegistration<I, O>;
  };
  /** Open the Promise-shaped facade over Mycelium's Effect runtime. */
  make: (options: MyceliumOptions) => Promise<Runtime>;
  /** Put every operation of one module behind a host gate. */
  gate: (module: ModuleRegistration, gate: OperationGate) => ModuleRegistration;
  /** Look one module's grant up by `grantKey` instead of its id. */
  grantAs: (module: ModuleRegistration, grantKey: string) => ModuleRegistration;
}>;

/** Schema-described authoring and Promise-facing runtime construction. */
export const Mycelium: MyceliumApi = Object.freeze({
  module,
  operation,
  make: openPromiseRuntime,
  gate: gateModule,
  grantAs: grantModuleAs,
});
