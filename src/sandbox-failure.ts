import * as Data from "effect/Data";

/** Optional source reference carried by sandbox failures. */
export type SandboxFailureReference = {
  readonly ref?: string | undefined;
};

/** A location in the submitted program, never in the host's source. */
export class SourceInvalid extends Data.TaggedError("SourceInvalid")<
  SandboxFailureReference & {
    readonly line: number;
    readonly column: number;
    readonly excerpt: string;
  }
> {}
/** Text captured inside the guest's own exception handler. */
export class GuestThrew extends Data.TaggedError("GuestThrew")<
  SandboxFailureReference & {
    readonly name: string;
    readonly message: string;
    readonly missingVariable?: string | undefined;
    /** Safe provider envelope carried in the guest exception's cause, never model text. */
    readonly transportMessage?: string | undefined;
  }
> {}
/** The invocation owner decided its execution budget was spent. */
export class DeadlineExceeded extends Data.TaggedError("DeadlineExceeded")<
  SandboxFailureReference & {
    readonly limitMs: number;
    readonly elapsedMs: number;
    readonly acknowledged?: boolean | undefined;
  }
> {}
/** Admission expired before any sandbox work started. */
export class SandboxBusy extends Data.TaggedError("SandboxBusy")<
  SandboxFailureReference & {
    readonly capacity: number;
    readonly waitedMs: number;
  }
> {
  /** Stable contention code, not a sandbox health failure. */
  readonly code = "sandbox-busy";
}

/** The invocation owner accepted cancellation. */
export class Cancelled extends Data.TaggedError("Cancelled")<
  SandboxFailureReference & {
    readonly by: "caller" | "operator";
    readonly acknowledged?: boolean | undefined;
  }
> {}
/** Dispatch started, but its transport did not deliver an answer. */
export class DispatchLost extends Data.TaggedError("DispatchLost")<
  SandboxFailureReference & {
    readonly elapsedMs: number;
    readonly transport: string;
    readonly errno?: string | undefined;
    readonly rawCause: unknown;
  }
> {}
/** The sandbox process could not start or stopped before answering. */
export class SandboxCrashed extends Data.TaggedError("SandboxCrashed")<
  SandboxFailureReference & {
    readonly exit?: number | undefined;
    readonly errno?: string | undefined;
    readonly rawCause: unknown;
  }
> {}
/** A received envelope could not be decoded. Its reason is operator only. */
export class BadResponse extends Data.TaggedError("BadResponse")<
  SandboxFailureReference & {
    readonly reason: string;
    readonly rawCause: unknown;
  }
> {}
/** A JSON result exceeded Mycelium's output budget. */
export class ResultTooLarge extends Data.TaggedError("ResultTooLarge")<
  SandboxFailureReference & {
    readonly size: number;
  }
> {}
/** A result could not be represented as JSON. */
export class ResultNotJson extends Data.TaggedError("ResultNotJson")<
  SandboxFailureReference & {
    readonly kind: string;
    readonly rawCause?: unknown;
  }
> {}
/** A capability owner explicitly admits this bounded diagnostic as safe. */
export type CapabilityDiagnostic = Readonly<{
  kind: "Files" | "Forage" | "InvalidInput" | "Unavailable" | "OperationFailed";
  message: string;
}>;
/** Fields admitted by a capability owner, with the original cause kept private. */
export type CapabilityFailureFields = {
  readonly ref?: string;
  readonly namespace: string;
  readonly operation: string;
  readonly cause: CapabilityDiagnostic;
  readonly rawCause?: unknown;
};
/** Failure at a named capability boundary, with an owner-admitted diagnostic. */
export class CapabilityFailed extends Data.TaggedError(
  "CapabilityFailed",
)<CapabilityFailureFields> {
  /** Retain the admitted diagnostic for ordinary Error consumers through the same safe projection. */
  constructor(fields: CapabilityFailureFields) {
    super(fields);
    this.message = sandboxFailureMessage(this);
  }
}
/** A foreign throw violated the typed adapter contract. */
export class Unknown extends Data.TaggedError("Unknown")<
  SandboxFailureReference & {
    readonly rawCause: unknown;
  }
> {}

/** Closed error channel shared by sandbox adapters and the invocation owner. */
export type SandboxFailure =
  | SourceInvalid
  | GuestThrew
  | DeadlineExceeded
  | SandboxBusy
  | Cancelled
  | DispatchLost
  | SandboxCrashed
  | BadResponse
  | ResultTooLarge
  | ResultNotJson
  | CapabilityFailed
  | Unknown;

/** Copy the failure to correlate one execution without mutating shared host errors. */
export function referenceSandboxFailure(
  failure: SandboxFailure,
  ref: string,
): SandboxFailure {
  switch (failure._tag) {
    case "SourceInvalid":
      return new SourceInvalid({
        line: failure.line,
        column: failure.column,
        excerpt: failure.excerpt,
        ref,
      });
    case "GuestThrew":
      return new GuestThrew({
        name: failure.name,
        message: failure.message,
        missingVariable: failure.missingVariable,
        transportMessage: failure.transportMessage,
        ref,
      });
    case "DeadlineExceeded":
      return new DeadlineExceeded({
        limitMs: failure.limitMs,
        elapsedMs: failure.elapsedMs,
        acknowledged: failure.acknowledged,
        ref,
      });
    case "SandboxBusy":
      return new SandboxBusy({
        capacity: failure.capacity,
        waitedMs: failure.waitedMs,
        ref,
      });
    case "Cancelled":
      return new Cancelled({
        by: failure.by,
        acknowledged: failure.acknowledged,
        ref,
      });
    case "DispatchLost":
      return new DispatchLost({
        elapsedMs: failure.elapsedMs,
        transport: failure.transport,
        errno: failure.errno,
        rawCause: failure.rawCause,
        ref,
      });
    case "SandboxCrashed":
      return new SandboxCrashed({
        exit: failure.exit,
        errno: failure.errno,
        rawCause: failure.rawCause,
        ref,
      });
    case "BadResponse":
      return new BadResponse({
        reason: failure.reason,
        rawCause: failure.rawCause,
        ref,
      });
    case "ResultTooLarge":
      return new ResultTooLarge({ size: failure.size, ref });
    case "ResultNotJson":
      return new ResultNotJson({
        kind: failure.kind,
        rawCause: failure.rawCause,
        ref,
      });
    case "CapabilityFailed":
      return new CapabilityFailed({
        namespace: failure.namespace,
        operation: failure.operation,
        cause: failure.cause,
        rawCause: failure.rawCause,
        ref,
      });
    case "Unknown":
      return new Unknown({ rawCause: failure.rawCause, ref });
  }
}

/** The sole model projection. Transport, parser and raw cause fields stay private. */
export function sandboxFailureMessage(
  failure: SandboxFailure,
  includeReference = true,
): string {
  let text: string;
  switch (failure._tag) {
    case "SourceInvalid":
      text = `SyntaxError in submitted source line ${String(failure.line)}, column ${String(failure.column)}: ${failure.excerpt.slice(0, 160)}. Nothing ran. Fix quote/heredoc nesting: quote multi-line shell text with a JavaScript template literal or \\n escapes; escape backticks and \${ inside it. If a nested heredoc is hard to quote, write the script with Files, then execute it.`;
      break;
    case "GuestThrew": {
      const name = failure.name.slice(0, 80);
      const message = failure.message.slice(0, 512);
      const missing = failure.missingVariable;
      text = `${name}: ${message}${missing === undefined ? "" : `. Declare ${missing} with const or let before assigning it; this sandbox does not create implicit globals. Each execute call has its own variables.`}`;
      break;
    }
    case "DeadlineExceeded":
      text = `execute hit its ${String(failure.limitMs)} ms deadline after ${String(failure.elapsedMs)} ms; split the work or persist progress${failure.acknowledged === false ? "; sandbox did not acknowledge cancellation" : ""}`;
      break;
    case "SandboxBusy":
      text = `sandbox-busy: all ${String(failure.capacity)} sandbox slots remained occupied after waiting ${String(failure.waitedMs)} ms. Nothing ran; retry this call after capacity becomes available.`;
      break;
    case "Cancelled":
      text =
        failure.acknowledged === false
          ? "Sandbox did not acknowledge cancellation"
          : "Execution was cancelled";
      break;
    case "DispatchLost":
      text = `The sandbox lost contact after ${String(failure.elapsedMs)} ms; a running command may still be running; check its effects before retrying`;
      break;
    case "SandboxCrashed":
      text =
        "The sandbox process stopped before answering; check effects before retrying";
      break;
    case "BadResponse":
      text =
        "The sandbox returned an unreadable result; retry once, split the call if it recurs";
      break;
    case "ResultTooLarge":
      text = "Sandbox result exceeds 256 KiB";
      break;
    case "ResultNotJson":
      text = "Sandbox returned non-JSON data";
      break;
    case "CapabilityFailed":
      text = failure.cause.message.slice(0, 1000);
      break;
    case "Unknown":
      return `The sandbox failed for an unrecognized reason${includeReference ? ` (ref ${failure.ref ?? "unavailable"})` : ""}`;
  }
  return !includeReference || failure.ref === undefined
    ? text
    : `${text} (ref ${failure.ref})`;
}
