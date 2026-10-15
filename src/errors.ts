import type { OperationSchemaIssue } from "./operation-schema.js";
import type { SandboxFailure } from "./sandbox-failure.js";

const invalidDefinitionCode = "invalid_definition";
const runtimeClosedCode = "runtime_closed";
const sourceFailureCode = "source_failure";

/** Lifecycle phase in which a capability source failed. */
export type MyceliumSourcePhase = "acquire" | "release" | "stale";

/** A capability declaration could not form one coherent installed surface. */
export class MyceliumDefinitionError extends Error {
  /** Stable machine-readable failure code. */
  readonly code = invalidDefinitionCode;

  constructor(message: string) {
    super(message);
    this.name = "MyceliumDefinitionError";
  }
}

/** An operation attempted to use a closed Mycelium runtime. */
export class MyceliumClosedError extends Error {
  /** Stable machine-readable failure code. */
  readonly code = runtimeClosedCode;

  constructor(message = "Mycelium runtime is closed") {
    super(message);
    this.name = "MyceliumClosedError";
  }
}

/** A capability source failed at its bounded host boundary. */
export class MyceliumSourceError extends Error {
  /** Stable machine-readable failure code. */
  readonly code = sourceFailureCode;

  constructor(
    /** Stable identity of the source, absent for whole-candidate failures. */
    readonly sourceId: string | undefined,
    /** Lifecycle phase that could not settle. */
    readonly phase: MyceliumSourcePhase,
  ) {
    super(
      sourceId === undefined
        ? `Capability source ${phase} failed`
        : `Capability source ${phase} failed: ${sourceId}`,
    );
    this.name = "MyceliumSourceError";
  }
}

/** Closed failure vocabulary for the first Mycelium runtime slice. */
export type MyceliumError =
  | MyceliumClosedError
  | MyceliumDefinitionError
  | MyceliumSourceError
  | SandboxFailure;

/** Untrusted operation input failed the operation's declared input schema. */
export class InvalidOperationInput extends Error {
  constructor(
    /** Schema issues, formatted for the model by the execute door. */
    readonly issues: readonly OperationSchemaIssue[],
  ) {
    super("Invalid operation input");
    this.name = "InvalidOperationInput";
  }
}

/** An operation's result failed its declared output schema. */
export class InvalidOperationOutput extends Error {
  constructor(
    /** Schema issues, kept for operators; never shown to the model. */
    readonly issues: readonly OperationSchemaIssue[],
  ) {
    super("Operation output failed its declared schema");
    this.name = "InvalidOperationOutput";
  }
}
