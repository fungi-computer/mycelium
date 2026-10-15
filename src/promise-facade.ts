import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Scope from "effect/Scope";

import { fromTypedSandbox } from "./effect.js";
import type {
  AcquireOptions,
  Lease,
  MyceliumOptions,
  Runtime,
} from "./index.js";
import { openCoreRuntime } from "./runtime.js";

async function runPromiseAtBoundary<A, E extends Error>(
  effect: Effect.Effect<A, E>,
  options?: { readonly signal?: AbortSignal },
): Promise<A> {
  const exit = await Effect.runPromiseExit(effect, options);
  if (Exit.isSuccess(exit)) return exit.value;
  const failure = Cause.findErrorOption(exit.cause);
  if (Option.isSome(failure)) throw failure.value;
  if (Cause.hasInterruptsOnly(exit.cause)) {
    throw options?.signal?.reason instanceof Error
      ? options.signal.reason
      : new DOMException("The operation was aborted", "AbortError");
  }
  const squashed = Cause.squash(exit.cause);
  if (squashed instanceof Error) throw squashed;
  throw new Error("Effect failed without an Error cause");
}

/** Adapt the canonical scoped runtime at the named Promise boundary. */
export async function openPromiseRuntime(
  options: MyceliumOptions,
): Promise<Runtime> {
  const runtimeScope = await runPromiseAtBoundary(Scope.make());
  const onSandboxFailure = options.onSandboxFailure;
  try {
    const runtime = await runPromiseAtBoundary(
      openCoreRuntime({
        modules: options.modules ?? [],
        sources: options.sources ?? [],
        session: options.session,
        execution: options.execution,
        sandbox: fromTypedSandbox(options.sandbox),
        onSandboxFailure:
          onSandboxFailure === undefined
            ? undefined
            : (event) => Effect.promise(() => onSandboxFailure(event)),
        tools: options.tools ?? [],
        access: options.access ?? {},
      }).pipe(Scope.provide(runtimeScope)),
    );
    const acquire = async (acquireOptions?: AcquireOptions): Promise<Lease> => {
      const leaseScope = await runPromiseAtBoundary(Scope.make());
      try {
        const lease = await runPromiseAtBoundary(
          runtime.acquire().pipe(Scope.provide(leaseScope)),
          acquireOptions?.signal === undefined
            ? undefined
            : { signal: acquireOptions.signal },
        );
        // One release attempt; a retry observes its outcome, never a fresh success.
        let releasing: Promise<void> | undefined;
        return Object.freeze({
          revision: lease.revision,
          catalog: lease.catalog,
          executeTool: lease.effectExecuteTool,
          tools: lease.tools,
          release: () => {
            releasing ??= runPromiseAtBoundary(
              lease
                .release()
                .pipe(Effect.ensuring(Scope.close(leaseScope, Exit.void))),
            );
            return releasing;
          },
        });
      } catch (cause) {
        await runPromiseAtBoundary(Scope.close(leaseScope, Exit.void));
        throw cause;
      }
    };

    // One close attempt; a retry observes its outcome, never a fresh success.
    let closing: Promise<void> | undefined;
    const close = (): Promise<void> => {
      closing ??= runPromiseAtBoundary(
        runtime
          .close()
          .pipe(Effect.ensuring(Scope.close(runtimeScope, Exit.void))),
      );
      return closing;
    };

    return Object.freeze({ acquire, close });
  } catch (cause) {
    await runPromiseAtBoundary(Scope.close(runtimeScope, Exit.void));
    throw cause;
  }
}
