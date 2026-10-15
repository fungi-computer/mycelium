import type { HostActor } from "@fungi.computer/puffball/app-method-runtime";

/** Authenticated credential identity, scoped by its host. Never a bearer token. */
export type McpPrincipal =
  | Readonly<{
      actor: Extract<HostActor, { kind: "member" }>;
      role: "owner" | "member";
    }>
  | Readonly<{ actor: Exclude<HostActor, { kind: "member" }> }>;

/** Compare complete admitted identities, including a canonical member's role. */
export function sameMcpPrincipal(
  left: McpPrincipal,
  right: McpPrincipal,
): boolean {
  const actor = left.actor;
  const other = right.actor;
  switch (actor.kind) {
    case "member":
      return (
        other.kind === "member" &&
        actor.memberId === other.memberId &&
        "role" in left &&
        "role" in right &&
        left.role === right.role
      );
    case "agent":
      return other.kind === "agent" && actor.agentId === other.agentId;
    case "team-key":
      return other.kind === "team-key" && actor.keyId === other.keyId;
  }
}
