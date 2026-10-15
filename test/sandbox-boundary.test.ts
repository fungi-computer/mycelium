import { describe, expect, expectTypeOf, it } from "vitest";
import * as Effect from "effect/Effect";
import { z } from "zod";
import type { SandboxBinding } from "../src/index.js";
import { submittedSourceLocation } from "../src/sandbox-guest.js";
import type { Sandbox as EffectSandbox } from "../src/effect.js";
import {
  CapabilityFailed,
  GuestThrew,
  sandboxFailureMessage,
} from "../src/sandbox-failure.js";
import {
  sandboxBindingBoundary,
  sandboxOperatorValue,
  sandboxExecutorFailure,
  sandboxGuestFailure,
  type SandboxBindingInput,
} from "../src/sandbox-boundary.js";

async function transportedError(
  operation: SandboxBinding | undefined,
): Promise<Error> {
  if (operation === undefined) throw new Error("Expected fixture operation");
  try {
    await operation({});
  } catch (cause) {
    return z.instanceof(Error).parse(cause);
  }
  throw new Error("Expected a transport rejection");
}

describe("Codemode's safe failure envelope", () => {
  it("treats malformed transport JSON as a refused provider failure", () => {
    const boundary = sandboxBindingBoundary({});
    expect(boundary.returnedFailure({ error: "{" })).toBeUndefined();
  });

  it("recovers an uncaught typed broker refusal without changing its guest diagnostic", () => {
    const cause = new Error("private-owner-canary");
    const failure = new CapabilityFailed({
      namespace: "forage",
      operation: "describe",
      cause: { kind: "Unavailable", message: "forage.read refused: failed" },
      rawCause: cause,
    });
    const boundary = sandboxBindingBoundary({});
    const transportMessage = boundary.capture(failure);
    const guest = sandboxGuestFailure({
      name: "BrokerRefusalError",
      message: "forage.read refused: failed",
      transportMessage,
    });
    expect(sandboxFailureMessage(guest)).toContain(
      "forage.read refused: failed",
    );
    expect(sandboxFailureMessage(guest)).not.toContain(transportMessage);
    expect(boundary.recover(guest)).toMatchObject({
      _tag: "CapabilityFailed",
      rawCause: cause,
    });
    expect(sandboxBindingBoundary({}).recover(guest)).toBe(guest);
    const nested = sandboxGuestFailure({
      name: "Error",
      message: "outer guest diagnostic",
      transportMessage: "ordinary nested guest cause",
    });
    expect(boundary.recover(nested)).toBe(nested);
    expect(sandboxFailureMessage(nested)).toContain("outer guest diagnostic");
  });
  it("preserves the SDK's absent argument for a no-argument operation", async () => {
    const argumentsSeen: SandboxBindingInput[] = [];
    const boundary = sandboxBindingBoundary({
      agents: {
        list: (input) => {
          argumentsSeen.push(input);
          return Promise.resolve([]);
        },
      },
    });
    const list = boundary.bindings.agents?.list;
    if (list === undefined) throw new Error("Expected fixture operation");
    await expect(list(undefined)).resolves.toEqual([]);
    expect(argumentsSeen).toEqual([undefined]);
  });
  it("recovers only an exact envelope owned by this invocation", async () => {
    const cause = new Error("private-host-canary");
    const failure = new CapabilityFailed({
      namespace: "web",
      operation: "fetch",
      cause: { kind: "OperationFailed", message: "disallowed_url" },
      rawCause: cause,
    });
    const boundary = sandboxBindingBoundary({
      web: { fetch: () => Promise.reject(failure) },
    });
    const transported = await transportedError(boundary.bindings.web?.fetch);
    expect(transported).toBeInstanceOf(Error);
    expect(transported.cause).toBeUndefined();
    expect(transported.message).not.toContain("private-host-canary");
    const decoded = z
      .strictObject({
        v: z.literal(1),
        kind: z.literal("sandbox_failure"),
        ref: z.string(),
        tag: z.string(),
        message: z.string(),
      })
      .parse(JSON.parse(transported.message));
    expect(decoded).toMatchObject({
      kind: "sandbox_failure",
      tag: "CapabilityFailed",
      message: "disallowed_url",
    });
    const guest = new GuestThrew({
      name: "Error",
      message: transported.message,
    });
    expect(boundary.recover(guest)).toMatchObject({
      _tag: "CapabilityFailed",
      rawCause: cause,
    });
    expect(
      boundary.returnedFailure({ error: transported.message }),
    ).toMatchObject({ _tag: "CapabilityFailed" });
    const prefixed = new GuestThrew({
      name: "Error",
      message: `unrelated ${transported.message}`,
    });
    expect(boundary.recover(prefixed)).toBe(prefixed);
    const wrongTag = new GuestThrew({
      name: "Error",
      message: JSON.stringify({ ...decoded, tag: "Unknown" }),
    });
    expect(boundary.recover(wrongTag)).toBe(wrongTag);
    const next = sandboxBindingBoundary({});
    expect(next.recover(guest)).toBe(guest);
    expect(sandboxFailureMessage(boundary.recover(guest))).not.toContain(
      "private-host-canary",
    );
  });

  it("keeps a maximally escaped safe diagnostic decodable through the guest's message budget", async () => {
    const boundary = sandboxBindingBoundary({
      files: {
        read: () =>
          Promise.reject(
            new CapabilityFailed({
              namespace: "files",
              operation: "read",
              cause: { kind: "Files", message: '\\"\n'.repeat(400) },
            }),
          ),
      },
    });
    const error = await transportedError(boundary.bindings.files?.read);
    expect(error.message.length).toBeLessThanOrEqual(512);
    expect(
      boundary.recover(
        new GuestThrew({ name: "Error", message: error.message }),
      ),
    ).toMatchObject({ _tag: "CapabilityFailed" });
  });

  it("serializes complete operator error chains and cycles without exposing them in model text", () => {
    const nested = new Error("operator-only", {
      cause: new TypeError("nested"),
    });
    const errors = new AggregateError([nested, 7n], "aggregate", {
      cause: nested,
    });
    const cycle = { self: {} };
    cycle.self = cycle;
    expect(sandboxOperatorValue(errors)).toMatchObject({
      name: "AggregateError",
      message: "aggregate",
      cause: { name: "Error", cause: { name: "TypeError", message: "nested" } },
      errors: [expect.objectContaining({ message: "operator-only" }), "7"],
    });
    expect(sandboxOperatorValue(cycle)).toEqual({
      self: { kind: "circular_reference" },
    });
    const array = new Array<object>();
    array.push(array);
    expect(sandboxOperatorValue(array)).toEqual([
      { kind: "circular_reference" },
    ]);
  });

  it("decodes only the SDK's exact timeout result and keeps other host text private", () => {
    expect(
      sandboxExecutorFailure("Execution timed out", 300, 350),
    ).toMatchObject({ _tag: "DeadlineExceeded", limitMs: 300, elapsedMs: 350 });
    const failed = sandboxExecutorFailure(
      "private-host-canary: Execution timed out",
      300,
      350,
    );
    expect(failed).toMatchObject({
      _tag: "BadResponse",
      rawCause: "private-host-canary: Execution timed out",
    });
    expect(sandboxFailureMessage(failed)).not.toContain("private-host-canary");
  });

  it("decodes a missing guest variable into a declaration hint once", () => {
    const failed = sandboxGuestFailure({
      name: "ReferenceError",
      message: "cat_r is not defined",
    });
    expect(failed).toMatchObject({
      _tag: "GuestThrew",
      missingVariable: "cat_r",
    });
    expect(sandboxFailureMessage(failed)).toContain(
      "Declare cat_r with const or let",
    );
    const unrelated = sandboxGuestFailure({
      name: "Error",
      message: "cat_r is not defined",
    });
    expect(sandboxFailureMessage(unrelated)).not.toContain("Declare");
  });

  it("decodes a real parser SyntaxError without merging an Error instance into a plain object", () => {
    const source = 'return "unterminated\ntext";';
    expect(
      submittedSourceLocation(`async () => {\n${source}\n}`, source),
    ).toMatchObject({ line: 1, column: 8 });
  });
});

it("requires a typed failure channel at the Effect sandbox seam", () => {
  type UntypedSandbox = { execute: () => Effect.Effect<unknown, unknown> };
  expectTypeOf<UntypedSandbox>().not.toExtend<EffectSandbox>();
  expectTypeOf<{
    execute: () => Effect.Effect<null, CapabilityFailed>;
  }>().toExtend<EffectSandbox>();
});
