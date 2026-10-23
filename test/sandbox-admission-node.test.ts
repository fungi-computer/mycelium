import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { expect, it } from "vitest";
import { z } from "zod";

// Python's maintained affinity API supplies CPU ids without parsing /proc or
// taskset's display text. Only disposable Node children have their affinity set.
const available = z
  .array(z.int().nonnegative())
  .nonempty()
  .parse(
    JSON.parse(
      execFileSync(
        "python3",
        [
          "-c",
          "import json,os; print(json.dumps(sorted(os.sched_getaffinity(0))))",
        ],
        { encoding: "utf8" },
      ),
    ),
  );
const ordinary =
  available.length >= 4 ? { cores: 4, expected: 4 } : { cores: 1, expected: 2 };
const wider = available.length >= 32 ? { cores: 32, expected: 32 } : ordinary;
const observation = z.strictObject({
  availableBefore: z.int().positive(),
  availableAfter: z.int().positive(),
  startedBeforeRelease: z.int().nonnegative(),
  startedAfterRelease: z.int().nonnegative(),
});

it.each([
  { label: "lower clamp", cores: 1, expected: 2, capacity: undefined },
  { label: "captured affinity", ...ordinary, capacity: undefined },
  { label: "wider available affinity", ...wider, capacity: undefined },
  { label: "explicit capacity", cores: 1, expected: 48, capacity: 48 },
])("uses real Node affinity for host policy: $label", (policy) => {
  const changedAffinity = available.slice(0, policy.cores === 1 ? 4 : 1);
  const result = observation.parse(
    JSON.parse(
      execFileSync(
        "taskset",
        [
          "--cpu-list",
          available.slice(0, policy.cores).join(","),
          process.execPath,
          fileURLToPath(
            new URL("./fixtures/sandbox-admission-node.mjs", import.meta.url),
          ),
          JSON.stringify({
            capacity: policy.capacity,
            attempts: policy.expected + 1,
            changedAffinity,
          }),
        ],
        { encoding: "utf8" },
      ),
    ),
  );
  expect(result.availableBefore).toBe(policy.cores);
  expect(result.availableAfter).toBe(changedAffinity.length);
  expect(result.startedBeforeRelease).toBe(policy.expected);
  expect(result.startedAfterRelease).toBe(policy.expected + 1);
});
