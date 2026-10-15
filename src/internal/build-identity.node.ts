import { execFileSync } from "node:child_process";
import { URL } from "node:url";

// Node-only variant of build-identity.ts, selected only by the package's
// `#build-identity` internal import (package.json `imports`, `node`
// condition) -- a literal Node process (apps/exec, services/agent-host,
// runtimes/local's non-bundled dist, this package's own tests) always
// activates that condition; no bundler build target in this repo does
// (demo and the Electron probe build with esbuild `platform: "browser"`;
// Hub builds with wrangler/workerd), so this file's `node:child_process`
// import never reaches a bundle that can't resolve it.
//
// Until 2026-10-07 the package build stamped execution-lifecycle.ts's
// build-identity copy with the exact commit, and Turbo hashed that same
// commit into the `build` task to avoid serving a stale stamp from cache.
// That forced every commit to miss the cache for this package and the ~80
// tasks across ~25 packages that depend on it (cache-hash-stability audit,
// S1). The identity is read lazily instead: nothing calls this outside the
// rare sandbox-failure path, so it costs a `git` call only when something
// is actually tagging a failure, never at build, lint, typecheck or
// ordinary test time, and the build output no longer depends on which
// commit produced it. A source checkout without a reachable Git repository
// (or a packed/registry install, which carries no `.git` at all) answers
// `null`; `pnpm pack` stamps its own copy of this file with a literal
// identity instead (package.json's `pack` script, scripts/stamp-build.mjs),
// since a packed copy can't shell out to Git once installed.
let cached: string | null | undefined;

export function sandboxBuildSha(): string | null {
  if (cached !== undefined) return cached;
  try {
    const candidate = execFileSync("git", ["rev-parse", "HEAD"], {
      cwd: new URL(".", import.meta.url),
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"],
    }).trim();
    cached = /^[0-9a-f]{40}$/u.test(candidate) ? candidate : null;
  } catch {
    cached = null;
  }
  return cached;
}
