import * as Effect from "effect/Effect";
import * as Semaphore from "effect/Semaphore";

/**
 * Serialize a synchronous state claim and settle its Effect outside the gate.
 * Interruption cannot release the caller until settlement finishes.
 */
export function claimThenSettle<A, B, E>(
  gate: Semaphore.Semaphore,
  claim: () => A,
  settle: (claimed: A) => Effect.Effect<B, E>,
): Effect.Effect<B, E> {
  return Effect.uninterruptibleMask((restore) =>
    restore(gate.take(1)).pipe(
      Effect.flatMap(() =>
        Effect.sync(claim).pipe(Effect.ensuring(gate.release(1))),
      ),
      Effect.flatMap(settle),
    ),
  );
}
