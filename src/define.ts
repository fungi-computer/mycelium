/**
 * Runtime-free authoring: declare modules and operations without loading the
 * execute runtime, so a definition can be bundled into an isolate that only
 * describes and invokes it (for example a hosted Agent extension).
 */
import type { MyceliumApi } from "./index.js";
import { defineModule, defineOperation, gateModule } from "./registration.js";

/** The authoring half of {@link MyceliumApi}; registrations are the same handles. */
export const Mycelium: Pick<MyceliumApi, "module" | "operation" | "gate"> =
  Object.freeze({
    module: defineModule,
    operation: defineOperation,
    gate: gateModule,
  });
