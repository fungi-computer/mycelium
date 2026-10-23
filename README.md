# Mycelium (`@fungi.computer/mycelium`)

Mycelium gives an Agent typed operations through one `execute` tool. It
validates operations, acquires a coherent capability snapshot and releases it
when the run ends. You can also execute a known operation without a model.

## Set up

```sh
npm install @fungi.computer/mycelium
```

The package ships ESM only and depends on Zod, Effect, `acorn`,
[Cairn](../cairn/README.md), [Forage](../forage/README.md), and
[Mule](../mule/README.md) at runtime, all installed automatically. The root
entrypoint is the Promise host facade. `@fungi.computer/mycelium/effect`
supplies the scoped Effect API. Both produce an Effect-native Mule tool. The
`./sandbox-guest`, `./sandbox-wire`, `./sandbox-failure`, `./sandbox-boundary`,
and `./semaphore-transition` subpaths support a host's sandbox adapter. A host
supplies the sandbox that actually evaluates code and enforces isolation,
external access and cancellation.

## Quick start without a model

This example registers `demo.ping`, executes one known binding and prints
`{"value":"pong"}`. The direct callback deliberately ignores submitted code. It
is **not isolation**: it runs with host privileges and checks registration,
validation and cleanup.

```ts
import { Mycelium } from "@fungi.computer/mycelium";
import { directSandbox } from "@fungi.computer/mycelium/testing";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import { z } from "zod";

const runtime = await Mycelium.make({
  modules: [
    Mycelium.module({
      id: "example-demo",
      name: "demo",
      operations: {
        ping: Mycelium.operation({
          input: z.object({}),
          output: z.object({ value: z.string() }),
          execute: async () => ({ value: "pong" }),
        }),
      },
    }),
  ],
  sandbox: directSandbox((bindings, signal) => {
    signal.throwIfAborted();
    const ping = bindings.demo?.ping;
    if (ping === undefined) throw new Error("Missing demo.ping binding");
    return ping({});
  }),
});
const lease = await runtime.acquire();
try {
  const prepared = await Effect.runPromise(
    lease.executeTool.prepare({
      type: "toolCall",
      id: "example-execute",
      name: "execute",
      arguments: { code: "return demo.ping({})" },
    }),
  );
  const parts = await Effect.runPromise(Stream.runCollect(prepared.execute()));
  const text = parts[0]?.result.content[0];
  if (text?.type !== "text" || text.text !== '{"value":"pong"}') {
    throw new Error("Unexpected execute result");
  }
  console.log(text.text);
} finally {
  await lease.release();
  await runtime.close();
}
```

The [public-import fixture](test/stranger/runtime.mjs) uses the same
known-binding pattern. A real evaluator must implement the public sandbox
contract. Its request carries code, execution identity, limits and admitted
bindings. Do not replace it with evaluation in the host's privileged process.

## Operations and discovery

Declare input and output with an Effect schema or any Standard Schema that also
implements Standard JSON Schema, such as Zod 4. Mycelium decodes input with the
schema before the operation runs. An Effect output schema encodes the result (so
a codec such as `Schema.NumberFromString` returns `"1"`); a Standard output
schema validates it. An Effect schema is exchanged as its canonical JSON codec
(`Schema.toCodecJson`), so the description, the decoder and the encoder agree. A
throwing filter or transformation is a host error, never reported to the model
as invalid input. Stable module IDs identify authority. Names become
model-facing aliases. Input ordering does not choose either identity. Forage
prepares the bounded discovery catalog for the captured graph.

Mule receives this runtime's one top-level `execute` tool. Inner operations,
including `forage.search` and `forage.describe`, stay behind that tool. The
operation callback receives only its declared input and execution context.
Access checks and authorization gates run before the operation. Product
membership, resource ownership and final external access checks remain host
responsibilities.

Each `execute` call runs in a fresh sandbox, not a REPL: no variable, function
or global survives between calls. Redefine helpers in every call, or persist
state to a file and read it back.

### Operation identity within execute

Every registered-operation call receives a frozen `InvocationContext` with a
zero-based `operationOrdinal`. One counter spans all registered methods and
modules in an execute. Allocation happens synchronously when the binding is
called, before asynchronous admission or operation work, so sequential calls and
concurrently started calls have distinct positions. Forage's built-in discovery
helpers do not consume registered-operation positions.

The counter resets for each execute attempt. Replaying the same bound-call
sequence gives the same ordinals even though `executionId` is fresh. A host that
needs effect deduplication combines its stored Session and command identities
with `toolCallId` and `operationOrdinal`; an ordinal alone is not a globally
unique identity. Different call sequences are not promised identical positions,
and the effect owner must still compare the intended operation and payload on
replay. This metadata grants neither authority nor permission to replay unsafe
work.

Direct `invokeModuleOperation` callers supply their own context; outside an
execute there is no enclosing call sequence, so `operationOrdinal` may be
absent, just as `toolCallId` may be absent. A host requiring execute identity
must refuse a write with missing identity rather than invent a fallback.

## Access labels and grants

Each operation's author labels what it does: `access: "read"` observes,
`access: "write"` changes something, and an unlabelled operation is `write`. An
author may add `confirm: true` to ask a person before each call. Labels are
fixed facts about the operation; the host never edits them.

The host passes its decisions down as data in `access`. `grants` maps a module
id to `"write"` (every operation), `"read"` (only `read` operations) or
`"none"`. A module the host does not name keeps full access, so a host that
grants nothing writes no permission code. `sessionGrants` reads the current
Session's stored grants at every call; they override `grants` key by key, so a
revoked grant applies to the very next call. Keys in `required` fail closed:
when neither names one, it is `"none"`. Labels are parsed at definition with
`ToolPolicySchema`: anything but exactly `"read"`/`"write"` and a boolean
`confirm` is a definition error. `confirm` is how the host asks a person;
without it, a `confirm` operation is refused and never runs unasked.

```ts
const runtime = await Mycelium.make({
  modules: [notes],
  sandbox,
  access: {
    grants: { notes: "read" },
    confirm: (request) => askPerson(request),
  },
});
```

Mycelium checks the grant first, then the person, before the operation's input
decoder runs. A refused call returns an `AccessRefusal` (`module`, `operation`,
`reason` and a plain `message`) as its ordinary result. `onRefused` observes
each refusal for logs or metering and never changes the decision.
`Mycelium.grantAs` looks a module's grant up by a wider owner's key, such as the
extension that built it. Labelled direct tools passed as `tools` sit behind the
same check and come back on the lease's `tools`.
[Access tests](test/access.test.ts) cover each rule.

## Nested namespaces

Compose modules in `operations` to expose any namespace depth. The enclosing
module ID remains the authority identity; child registration IDs do not create
new authority. For example, a module named `bazaar` with `namespace: ["apps"]`
and `operations: { windows: windowModule }` exposes the child's `open` operation
as `env.apps.bazaar.windows.open(...)`. A child's own `namespace` adds segments
after its registration key. Omit `namespace` for the ordinary depth-1 form
`env.demo.ping(...)`; the existing `demo.ping(...)` form also works.

> An operation key may also be a dotted relative path:
> `operations: { "repos.list": op }` is called as
> `env.<module>.repos.list(...)`, exactly like a child module `repos` with
> operation `list`. Installation refuses a key that is both an operation and a
> namespace prefix (`files` with `files.read`).

Every leaf keeps its declared input/output types. A gate on the enclosing module
sees relative paths such as `windows.open`. Forage search, list and describe use
the complete logical path, and descriptions declare the same nested, callable
`env`. Names are allocated within their logical namespace, so an App and a
Computer can share a name without renaming either. Transport aliases and flat
operation tables remain host-only implementation details. Sandbox adapters use
`/sandbox-guest`'s parsed namespace metadata and `sandboxGuestProgram` to
project those bindings; provider adapters also use
`sandboxProviderOperationName` to avoid Codemode's dotted-name collisions.

## Changing capabilities and lifetime

Use `sources` when a host must acquire a current Computer, Connection or other
capability projection. A source returns its bounded snapshot and an awaited
release function. Its `invalidate()` callback marks that snapshot stale. The
next lease builds a new coherent graph. An existing lease keeps its captured
snapshot.

Release each lease and close the runtime. Partial acquisition unwinds already
acquired sources in reverse order. Mycelium owns the scoped resources it
acquires, while the host owns durable relationships, credentials and current
policy. A captured snapshot does not excuse a missing final authority check.

## Cancellation and errors

Execution has a deadline, a cancellation acknowledgement grace and a bounded
JSON result. Cancellation aborts the invocation signal and waits for backing
sandbox settlement. If the sandbox ignores it, the result reports
`acknowledged: false`. Do not report that work as stopped.

The typed failure channel includes syntax errors, guest exceptions, lost
transport, bad responses, failed capabilities, deadline/cancellation and invalid
or oversized results. A foreign throw becomes `Unknown`. The model receives the
bounded `sandboxFailureMessage` projection with a reference. The host receives
operator evidence through `onSandboxFailure` and the existing reporting path.
Preserve that evidence without sending host diagnostics to the guest.

### Sandbox admission

The host-only `execution` options accept `sandboxCapacity` (a positive safe
integer) and `admissionWaitMs` (an integer from 1 to 900,000 milliseconds,
default 5,000). Guests still submit only `code`. An omitted capacity derives
from available cores, clamped to 2..32; hosts without a portable core
observation use 2. Node captures its affinity-aware core count once per runtime
through `node:os.availableParallelism()`, behind a platform-conditional module
outside the Workers import graph. Explicit valid capacity overrides bypass
observation and are not clamped, including values above 32.

`normalizeSandboxAdmissionPolicy(options, availableCores?)` validates and
resolves these host facts. It does not reserve a slot or start a sandbox. Both
public runtime facades validate and capture the policy when the runtime opens.

Admission belongs to the existing runtime-wide backing-work tracker, not each
lease, group, or revision. A full pool waits until a slot opens or the earlier
of the admission-wait deadline and the **original invocation deadline**.
Admission-wait expiry yields `SandboxBusy`, stable code `sandbox-busy`, with
safe `capacity` and `waitedMs` facts; the submitted program has not run.
Invocation-deadline expiry remains `DeadlineExceeded`, including when both
deadlines coincide. Caller cancellation remains `Cancelled`. Queueing does not
restart the execution timeout. No automatic replay is authorized.

The runtime uses one maintained Effect semaphore across source revisions and
interruptible deadline APIs. Queued work is registered before waiting.
Cancellation removes queued work; no expired/cancelled waiter may start later. A
permit remains owned until actual backing settlement, including after an
unacknowledged abort. Waiting does not hold the lifecycle/revision gate needed
for release or close.

`SandboxBusy` is admitted by the existing owned-error decoder, safe transport,
host wire and operator-event grammar; it is contention, not an `Unknown` health
alert. The execute tool retains its safe typed cause and an actionable coded
message without changing Mule's error contract. Unrelated foreign failures stay
`Unknown`: do not classify diagnostic prose, unwrap arbitrary cause chains, or
mislabel an arbitrary adapter `AbortError` as caller-authorized cancellation.

An Effect host with a Promise sandbox uses `fromTypedSandbox`, which waits for
that Promise to settle when interrupted. The concrete sandbox and broker decide
how backing work is stopped. Hosts supply the Worker Loader binding and
credential custody. The Workers-only `@fungi.computer/mycelium/workers` entry
exports `createCodemodeSandbox(loader, adapter?, options?)`, a Codemode adapter
runnable on Workers and celld. It disables guest network access, binds only the
supplied operations, preserves typed failures and awaits the backing execution
when cancellation cannot be acknowledged. Import this entry only in Workers
hosts; the root and other Node entries do not load it.

The adapter forwards the request's existing `timeoutMs` budget as the Worker
Loader's `cpuMs` limit, retaining any tighter loader limit. Cloudflare and celld
enforce native CPU limits. Local workerd development does not; execution still
runs. The local host declares that fact with
`{ localDevUnenforcedLimits: ["cpu"] }` as the third argument. Mycelium prints
`CPU limits are not enforced in local dev` once per runtime, across sandbox
instances. Hosts may also declare unenforced `memory`, `output` and `cancel`
controls; each gets its own once-only diagnostic. The option reports host facts;
it adds no enforcement or guest policy. Cancellation still waits for backing
settlement, and a wall-time timeout does not prove isolate termination.

The
[Worker Loader custom-limit contract](https://developers.cloudflare.com/dynamic-workers/usage/limits/)
and the pinned celld invocation types expose `cpuMs` and `subRequests`. They do
not expose per-invocation memory or output caps, or an execution-cancel
operation. The adapter preserves supported loader limits without inventing
fields for those controls. Host-wide memory limits remain host configuration;
they are not a guest-widenable sandbox setting. The canonical Mycelium
invocation owner checks its JSON result budget; calling the bare `Sandbox`
interface does not run that owner's result validation.

An abort before dispatch starts no guest. An abort during execution remains
authoritative after backing settlement: the adapter's Promise rejects with typed
`Cancelled { by: "caller" }` instead of resolving a late success. Hosts retain
invocation custody until that Promise settles; an abort does not release custody
early. Codemode has no immediate cancellation API. The native qualification
checks this behavior through the real Worker Loader, along with the trusted
deadline, CPU forwarding and once-only local notices. These are sandbox checks;
they do not prove a Workflow host's cancellation path.

## API and examples

Use the [generated API reference](docs/api/README.md) and
[exports](package.json). Root and `/effect` expose lifecycle construction. The
sandbox subpaths expose wire, guest, failure and boundary contracts for
adapters.

[Capability lifecycle tests](test/capability-plane.test.ts) cover source updates
and release. [Gate tests](test/gate.test.ts) exercise refusals.
[Sandbox boundary tests](test/sandbox-boundary.test.ts) exercise failure
projection. These package proofs do not qualify a particular sandbox's isolation
or deployment.

## Contribute and license

Add a regression to the nearest capability, gate or sandbox test. From the root:

```sh
pnpm --dir packages/mycelium run test
pnpm --dir packages/mycelium run docs
```

MIT. See [LICENSE](LICENSE) and [THIRD_PARTY_NOTICES](THIRD_PARTY_NOTICES).

## MCP

`@fungi.computer/mycelium/mcp` exports `createMcp({ catalog, authenticate })`,
returning `{ handleRequest, close }`. Mount `handleRequest` at the host's MCP
route. Authentication runs for every HTTP request, including notifications and
disconnects, and before each operation. Return either
`{ kind: "admitted", principal }` or `{ kind: "refused", status: 401 | 403 }`.
The principal is the host's canonical actor: a member with its role
(`{ actor: { kind: "member", memberId }, role }`), or an Agent or Team key
(`{ actor: { kind: "agent", agentId } }`,
`{ actor: { kind: "team-key", keyId } }`). Its ids identify the credential's
actor, never a bearer token.

For installed-App hosts, `createMcpCatalog({ load, sandbox })` adapts ordinary
Mycelium capability sources. `load(principal)` returns the current
`{ instructions, sources, grants }` authorized for that principal. It runs again
before execution and after approval. Each invocation must begin a fresh
authority read; returning an older cached snapshot cannot establish current
grants. Grants are explicit: an omitted module is refused. The supplied sandbox
remains the host's isolated executor; this entry provides no host-fetch
fallback. This catalog keeps at most eight open connections in process memory
and releases each acquired tool runtime when its request finishes.

Durable Agent hosts implement `McpCatalogProvider` over their existing Session
owner. `open` atomically reserves the supplied connection bound, `resume`
returns only an open principal-bound Session, and `refresh` delivers current
grants. The MCP server independently checks the stored principal. The Session's
`call` must carry `McpExecutionContext.access` into its ordinary Mycelium
execution, including direct tools, without persisting request callbacks.

MCP does not force confirmation for every write. The host's durable approval
owner decides whether a write applies, waits for approval or is denied; the
catalog's grants remain authority ceilings, not approval decisions. Explicit
operation-level confirmation labels still use form elicitation and refresh
authority after acceptance. Missing or declined explicit confirmation refuses
execution. The server supports MCP `2025-11-25` Streamable HTTP so an approval
can suspend the existing execution without replaying already-completed
operations. Approval requests contain the module, operation and access label,
not credential data or operation arguments. `close()` closes transport streams;
Session retention and connection persistence remain with the catalog owner.
After transport process loss, a resumed connection has no recorded client
elicitation capability and refuses writes. Delete that connection and initialize
a fresh one to negotiate form elicitation again.
