/* global fetch, AbortSignal, setTimeout */
import assert from "node:assert/strict";
import { execFileSync, spawn } from "node:child_process";
import console from "node:console";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:net";
import process from "node:process";
import { fileURLToPath, URL } from "node:url";

const root = fileURLToPath(new URL("../", import.meta.url));
const repository = fileURLToPath(new URL("../../../", import.meta.url));
const runtime = process.env.CELLD_PLATFORM_CELLD ? "celld" : "workerd";
// The maintained celld translator reads this in the spawning process.
if (runtime === "celld") process.env.CELLD_PLATFORM_LOGS = "1";
await mkdir(`${root}.botanical`, { recursive: true });
const work = await mkdtemp(`${root}.botanical/code-sandbox-`);
const socket = createServer();
await new Promise((resolve) => socket.listen(0, "127.0.0.1", resolve));
const { port } = socket.address();
await new Promise((resolve) => socket.close(resolve));
const origin = `http://127.0.0.1:${port}`;
let child;
let stopped;
let output = "";

try {
  execFileSync(
    `${repository}node_modules/.bin/esbuild`,
    [
      `${root}test/code-sandbox.fixture.mjs`,
      "--bundle",
      "--format=esm",
      "--platform=neutral",
      "--target=es2022",
      "--external:cloudflare:workers",
      "--external:node:*",
      "--main-fields=browser,module,main",
      "--conditions=workerd,worker,browser",
      `--outfile=${work}/worker.mjs`,
      "--log-level=warning",
    ],
    { cwd: repository, stdio: "pipe" },
  );
  await writeFile(
    `${work}/wrangler.json`,
    JSON.stringify({
      name: "mycelium-code-sandbox-proof",
      main: `${work}/worker.mjs`,
      compatibility_date: "2026-09-04",
      compatibility_flags: ["nodejs_compat"],
      worker_loaders: [{ binding: "LOADER" }],
      vars: { LOCAL_WORKERD: runtime === "workerd" ? "true" : "false" },
    }),
  );
  child = spawn(
    `${repository}node_modules/.bin/wrangler`,
    [
      "dev",
      "--config",
      `${work}/wrangler.json`,
      "--local",
      "--ip",
      "127.0.0.1",
      "--port",
      String(port),
      "--persist-to",
      `${work}/state`,
    ],
    { cwd: root, stdio: ["ignore", "pipe", "pipe"], env: process.env },
  );
  child.stdout.on("data", (chunk) => {
    output += chunk;
  });
  child.stderr.on("data", (chunk) => {
    output += chunk;
  });
  stopped = new Promise((resolve) => child.once("exit", resolve));
  for (let attempt = 0; ; attempt++) {
    const ready = await fetch(`${origin}/ready`, {
      signal: AbortSignal.timeout(1_000),
    }).then(
      (response) => response.ok,
      () => false,
    );
    if (ready) break;
    if (
      child.exitCode !== null ||
      output.includes("Build failed") ||
      attempt >= 120
    )
      throw new Error(
        "Native sandbox fixture did not start; see ignored startup log",
      );
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
  const read = async (route) => {
    const response = await fetch(`${origin}/${route}`, {
      signal: AbortSignal.timeout(10_000),
    });
    assert.equal(response.status, 200);
    return response.json();
  };
  assert.deepEqual(await read("input"), {
    kind: "completed",
    value: { input: { names: ["Garden"] }, authority: ["INPUT"] },
  });
  assert.deepEqual(await read("network"), { kind: "completed", value: true });
  assert.deepEqual(await read("guest"), {
    kind: "failed",
    failure: "GuestThrew",
  });
  assert.deepEqual(await read("source"), {
    kind: "failed",
    failure: "SourceInvalid",
  });
  assert.deepEqual(await read("cancel"), {
    kind: "failed",
    failure: "Cancelled",
  });
  assert.deepEqual(await read("deadline"), {
    failures: Array(6).fill("DeadlineExceeded"),
  });
  assert.deepEqual(await read("memory"), { kind: "completed", value: 524288 });
  assert.deepEqual(await read("output"), { kind: "completed", length: 70000 });
  const cpu = await read("cpu");
  if (runtime === "celld") {
    assert.equal(cpu.kind, "failed");
    assert.equal(cpu.failure, "DispatchLost");
  } else {
    assert.equal(
      output.split("CPU limits are not enforced in local dev").length - 1,
      1,
    );
  }
  for (const limit of ["Memory", "Output", "Cancellation"])
    assert.equal(
      output.split(`${limit} limits are not enforced in local dev`).length - 1,
      1,
    );
  // A failed/aborted guest does not stop the host accepting its next execution.
  assert.equal((await read("input")).kind, "completed");
  console.log(
    `Mycelium on ${runtime}: owned guest/source failures, isolation, CPU forwarding, late-abort refusal, deadline recovery, unsupported-limit notices; PASS`,
  );
} finally {
  if (child) {
    child.kill("SIGTERM");
    await stopped;
  }
  await writeFile(`${root}.botanical/code-sandbox-${runtime}.log`, output);
  await rm(work, { recursive: true, force: true });
}
