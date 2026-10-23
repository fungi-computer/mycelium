import { availableParallelism } from "node:os";

/** Node's maintained, affinity-aware observation; captured by runtime policy. */
export function availableHostCores(): number {
  return availableParallelism();
}
