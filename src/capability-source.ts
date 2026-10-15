import type * as Schema from "effect/Schema";
import type { ModuleRegistration } from "./registration.js";

/** Session on whose behalf one runtime acquires its capability projections. */
export type CapabilitySession = Readonly<{
  /** Stable Session identity supplied by the session host (Shiitake). */
  sessionId: string;
  /** Host's stored command identity; stable across run recovery. */
  commandId?: string;
  /** Opaque host metadata from that stored command, never a model argument. */
  meta?: typeof Schema.Json.Type;
}>;

/** Context supplied while acquiring one current capability projection. */
export type CapabilitySourceContext = Readonly<{
  /**
   * Session the projection is acquired for; absent for session-free hosts.
   * A source uses it to project only what that Session is permitted to use.
   */
  session?: CapabilitySession;
  /** Cooperative cancellation for this bounded acquisition attempt. */
  signal: AbortSignal;
  /** Mark the candidate stale when the source's current projection changes. */
  invalidate: () => void;
}>;

/** Bounded current projection returned by one capability source. */
export type CapabilitySourceSnapshot = Readonly<{
  /** Stable, secret-free version of this source projection. */
  version: string;
  /** Unordered modules contributed by this projection. */
  modules: readonly ModuleRegistration[];
}>;

/** Acquired source projection and the cleanup that owns its live adapter. */
export type CapabilitySourceAcquisition = Readonly<{
  /** Current immutable projection captured by this acquisition. */
  snapshot: CapabilitySourceSnapshot;
  /** Release subscriptions and adapters owned by this acquisition. */
  release: () => Promise<void>;
}>;

/** Promise-shaped host boundary for one versioned capability projection. */
export type CapabilitySource = Readonly<{
  /** Stable, bounded, secret-free source identity. */
  id: string;
  /** Acquire one current projection and its awaited cleanup. */
  acquire: (
    context: CapabilitySourceContext,
  ) => Promise<CapabilitySourceAcquisition>;
}>;
