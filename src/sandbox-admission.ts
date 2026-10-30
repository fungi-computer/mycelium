import { z } from "zod";
import { MyceliumDefinitionError } from "./errors.js";

export const sandboxAdmissionOptionsSchema = z.strictObject({
  sandboxCapacity: z.int().positive().optional(),
  admissionWaitMs: z.int().min(1).max(900_000).optional(),
});

/** Host-owned admission inputs; never part of guest execute arguments. */
export type SandboxAdmissionOptions = Readonly<{
  /** Positive host-selected capacity; omission derives the host default. */
  sandboxCapacity?: number | undefined;
  /** Maximum admission wait in milliseconds, from 1 through 900,000. */
  admissionWaitMs?: number | undefined;
}>;

/** Captured once per runtime and shared across backing-work groups. */
export type SandboxAdmissionPolicy = Readonly<{
  capacity: number;
  waitMs: number;
}>;

/** Validate host policy and resolve defaults from a platform-owned core observation. */
export function normalizeSandboxAdmissionPolicy(
  options: SandboxAdmissionOptions | undefined,
  availableCores?: number,
): SandboxAdmissionPolicy {
  const parsed = sandboxAdmissionOptionsSchema.safeParse(options ?? {});
  const cores = z
    .int()
    .positive()
    .safeParse(availableCores ?? 2);
  if (!parsed.success || !cores.success) {
    throw new MyceliumDefinitionError("Sandbox admission policy is invalid");
  }
  return Object.freeze({
    capacity:
      parsed.data.sandboxCapacity ?? Math.min(32, Math.max(2, cores.data)),
    waitMs: parsed.data.admissionWaitMs ?? 180_000,
  });
}
