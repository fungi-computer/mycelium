import { execFileSync } from "node:child_process";
import { lstatSync } from "node:fs";
import { writeFile } from "node:fs/promises";

// Only `pnpm pack` runs this (package.json's `pack` script), never the
// ordinary `build` task. A packed/registry-installed copy has no `.git`
// directory, so the Node-only dist's runtime git lookup (src/internal/
// build-identity.node.ts, loaded only by a literal Node process via the
// package's `#build-identity` `node` condition) would always answer null
// there; this stamps a literal identity into the packed copy instead, at
// pack time, which is late and outside Turbo entirely -- it cannot bust the
// `build` task's cache (2026-10-07 cache-hash-stability audit, S1). A source
// archive or registry install has no `.git` directory either, so git is
// unavailable; that is expected, not an error, and the stamped identity
// falls back to the same `null` the unbuilt source carries. The
// platform-neutral default (src/internal/build-identity.ts, loaded by every
// non-Node bundle) is untouched -- it always answers `null` and must never
// gain a `node:*` import.
const packageRoot = new URL("../", import.meta.url);
let sha = null;
const gitMetadata = lstatSync(new URL("../../.git", packageRoot), {
  throwIfNoEntry: false,
});
if (gitMetadata !== undefined) {
  const candidate = execFileSync("git", ["rev-parse", "HEAD"], {
    cwd: packageRoot,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "ignore"],
  }).trim();
  if (!/^[0-9a-f]{40}$/u.test(candidate)) {
    throw new Error("Git returned an invalid build identity");
  }
  sha = candidate;
}
await writeFile(
  new URL("dist/internal/build-identity.node.js", packageRoot),
  `export function sandboxBuildSha() {\n  return ${JSON.stringify(sha)};\n}\n`,
);
