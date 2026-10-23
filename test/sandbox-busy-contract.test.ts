import { expect, it } from "vitest";
import {
  sandboxBindingBoundary,
  sandboxFailure,
  sandboxGuestFailure,
} from "../src/sandbox-boundary.js";
import {
  referenceSandboxFailure,
  sandboxFailureMessage,
} from "../src/sandbox-failure.js";
import { SandboxFailureEventSchema } from "../src/sandbox-failure-event.js";
import {
  sandboxFailureWire,
  sandboxWireFailure,
  sandboxWireSchema,
} from "../src/sandbox-wire.js";
import * as Schema from "effect/Schema";

it("retains sandbox-busy across host wire, owned admission and guest recovery", () => {
  const wire = {
    ok: false,
    error: {
      kind: "sandbox_busy",
      code: "sandbox-busy",
      capacity: 12,
      waitedMs: 1_000,
    },
  };
  const decoded = sandboxWireSchema.parse(wire);
  if (decoded.ok) throw new Error("Expected a sandbox refusal");
  const failure = sandboxWireFailure(decoded.error);
  expect(sandboxFailure(failure)).toBe(failure);
  expect(failure).toMatchObject({
    _tag: "SandboxBusy",
    code: "sandbox-busy",
    capacity: 12,
    waitedMs: 1_000,
  });
  expect(sandboxFailureWire(failure)).toEqual(wire);
  const referenced = referenceSandboxFailure(failure, "busy-ref");
  expect(referenced).toMatchObject({
    _tag: "SandboxBusy",
    code: "sandbox-busy",
    capacity: 12,
    waitedMs: 1_000,
    ref: "busy-ref",
  });
  expect(sandboxFailureMessage(referenced)).toContain("sandbox-busy");
  expect(sandboxFailureMessage(referenced)).toContain("Nothing ran");

  const boundary = sandboxBindingBoundary({});
  const transportMessage = boundary.capture(referenced);
  const guest = sandboxGuestFailure({
    name: "Error",
    message: transportMessage,
  });
  expect(boundary.recover(guest)).toMatchObject({
    _tag: "SandboxBusy",
    capacity: 12,
    waitedMs: 1_000,
  });
  expect(
    Schema.decodeUnknownSync(SandboxFailureEventSchema)({
      kind: "sandbox_failure",
      ref: "busy-ref",
      executionId: "busy-ref",
      adapter: "effect",
      buildSha: null,
      tag: "SandboxBusy",
      fields: { code: "sandbox-busy", capacity: 12, waitedMs: 1_000 },
      healthAlert: false,
    }),
  ).toMatchObject({ tag: "SandboxBusy", healthAlert: false });
});
