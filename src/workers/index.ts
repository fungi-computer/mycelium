/**
 * A Mycelium sandbox over Cloudflare Codemode: each `execute` call runs the
 * model's program in a fresh Dynamic Worker (Worker Loader) with no network,
 * and the bound operations are its only way out. For Workers hosts.
 */
import { DynamicWorkerExecutor, normalizeCode } from "@cloudflare/codemode";
import {
  BadResponse,
  Cancelled,
  DispatchLost,
  SourceInvalid,
} from "@fungi.computer/mycelium/sandbox-failure";
import type {
  Sandbox,
  SandboxRequest,
  SandboxValue,
} from "@fungi.computer/mycelium";
import {
  sandboxBindingNames,
  sandboxGuestProgram,
  sandboxGuestResultSchema,
  sandboxProviderOperationName,
  submittedSourceLocation,
} from "@fungi.computer/mycelium/sandbox-guest";
import {
  isSandboxFailure,
  sandboxBindingBoundary,
  sandboxExecutorFailure,
  sandboxGuestFailure,
} from "@fungi.computer/mycelium/sandbox-boundary";

/** The Worker Loader binding Codemode runs guests on. */
export type CodemodeLoader = WorkerLoader;

/** Local development facts supplied by the host, never by guest code. */
export type CodemodeSandboxOptions = Readonly<{
  localDevUnenforcedLimits?: readonly (
    "cpu" | "memory" | "output" | "cancel"
  )[];
}>;

const unenforcedLimitMessages = {
  cpu: "CPU limits are not enforced in local dev",
  memory: "Memory limits are not enforced in local dev",
  output: "Output limits are not enforced in local dev",
  cancel: "Cancellation limits are not enforced in local dev",
};
const reportedUnenforcedLimits = new Set<string>();

/** Construct a network-isolated Codemode sandbox over a Worker Loader. */
export function createCodemodeSandbox(
  loader: CodemodeLoader,
  adapter = "codemode-workerd",
  options: CodemodeSandboxOptions = {},
): Sandbox {
  for (const limit of options.localDevUnenforcedLimits ?? []) {
    const message = unenforcedLimitMessages[limit];
    if (!reportedUnenforcedLimits.has(message)) {
      reportedUnenforcedLimits.add(message);
      console.warn(message);
    }
  }
  return {
    adapter,
    async execute(
      request: SandboxRequest,
      signal: AbortSignal,
    ): Promise<SandboxValue> {
      const startedAt = Date.now();
      try {
        signal.throwIfAborted();
        const normalized = normalizeCode(request.code);
        const location = submittedSourceLocation(normalized, request.code);
        if (location !== null) throw new SourceInvalid(location);
        const boundary = sandboxBindingBoundary(request.bindings);
        const withCpuBudget = (
          definition: WorkerLoaderWorkerCode,
        ): WorkerLoaderWorkerCode => ({
          ...definition,
          limits: {
            ...definition.limits,
            cpuMs: Math.min(
              request.timeoutMs,
              definition.limits?.cpuMs ?? request.timeoutMs,
            ),
          },
        });
        const executor = new DynamicWorkerExecutor({
          loader: {
            load: (definition) => loader.load(withCpuBudget(definition)),
            get: (name, getCode) =>
              loader.get(name, async () => withCpuBudget(await getCode())),
          },
          timeout: request.timeoutMs,
          globalOutbound: null,
        });
        // Codemode has no cancellation API; keep its promise until it settles
        // so Mycelium owns the cancellation grace.
        const output = await executor.execute(
          sandboxGuestProgram(
            normalized,
            sandboxBindingNames(boundary.bindings),
            request.namespacePaths,
            sandboxProviderOperationName,
          ),
          Object.entries(boundary.bindings).map(([name, operations]) => ({
            name,
            fns: Object.fromEntries(
              Object.entries(operations).map(([operation, binding]) => [
                sandboxProviderOperationName(operation),
                binding,
              ]),
            ),
          })),
        );
        signal.throwIfAborted();
        if (output.error !== undefined)
          throw sandboxExecutorFailure(
            output.error,
            request.timeoutMs,
            Date.now() - startedAt,
          );
        const decoded = sandboxGuestResultSchema.safeParse(output.result);
        if (!decoded.success)
          throw new BadResponse({
            reason: "invalid guest envelope",
            rawCause: decoded.error,
          });
        if (!decoded.data.ok)
          throw boundary.recover(
            sandboxGuestFailure(decoded.data.error, request.bindings),
          );
        const returned = boundary.returnedFailure(decoded.data.value);
        if (returned !== undefined) throw returned;
        return decoded.data.value;
      } catch (cause) {
        if (signal.aborted) throw new Cancelled({ by: "caller" });
        if (isSandboxFailure(cause)) throw cause;
        throw new DispatchLost({
          elapsedMs: Date.now() - startedAt,
          transport: adapter,
          rawCause: cause,
        });
      }
    },
  };
}
