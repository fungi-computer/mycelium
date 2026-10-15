// Platform-neutral default. Browser bundles (apps/demo, esbuild
// `platform: "browser"`), Electron-renderer bundles
// (electron-local-consumer-probe) and Workers bundles (apps/hub, wrangler)
// all reach this module through execution-lifecycle.ts's `#build-identity`
// internal import (package.json `imports`) and resolve to *this* file,
// since none of those bundlers activate a `node` condition. This file must
// never import `node:*` -- the real, Git-backed implementation lives in
// build-identity.node.ts, which only a literal Node process (Node's own
// resolver always activates the `node` condition) ever loads (2026-10-07
// cache-hash-stability audit, S1 + the batch-368 gate failure it caused:
// `Could not resolve "node:child_process"` while bundling
// @fungi.computer/demo and electron-local-consumer-probe).
export function sandboxBuildSha(): string | null {
  return null;
}
