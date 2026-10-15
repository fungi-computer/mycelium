import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { defineConfig } from "vitest/config";
import { fileURLToPath } from "node:url";

export default defineConfig({
  root: fileURLToPath(new URL("../", import.meta.url)),
  plugins: [
    cloudflareTest({
      miniflare: {
        compatibilityDate: "2026-09-04",
        compatibilityFlags: ["nodejs_compat"],
        workerLoaders: { LOADER: {} },
      },
    }),
  ],
  test: { include: ["test/codemode-workers.proof.mjs"] },
});
