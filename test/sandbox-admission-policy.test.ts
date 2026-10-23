import { expect, it } from "vitest";
import { normalizeSandboxAdmissionPolicy } from "../src/index.js";

it("resolves a bounded core default without clamping explicit host capacity", () => {
  expect(normalizeSandboxAdmissionPolicy(undefined, 1)).toEqual({
    capacity: 2,
    waitMs: 5_000,
  });
  expect(normalizeSandboxAdmissionPolicy(undefined, 64)).toEqual({
    capacity: 32,
    waitMs: 5_000,
  });
  expect(normalizeSandboxAdmissionPolicy(undefined)).toEqual({
    capacity: 2,
    waitMs: 5_000,
  });
  expect(
    normalizeSandboxAdmissionPolicy(
      { sandboxCapacity: 48, admissionWaitMs: 700 },
      4,
    ),
  ).toEqual({ capacity: 48, waitMs: 700 });
  expect(normalizeSandboxAdmissionPolicy({ sandboxCapacity: 1 }, 64)).toEqual({
    capacity: 1,
    waitMs: 5_000,
  });
});

it.each([0, -1, 1.5, Infinity, NaN])(
  "rejects invalid host capacity and wait %s",
  (invalid) => {
    expect(() =>
      normalizeSandboxAdmissionPolicy({ sandboxCapacity: invalid }),
    ).toThrow("Sandbox admission policy is invalid");
    expect(() =>
      normalizeSandboxAdmissionPolicy({ admissionWaitMs: invalid }),
    ).toThrow("Sandbox admission policy is invalid");
  },
);
