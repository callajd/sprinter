# Implementation Plan — Part 1: Foundation

The prerequisites shared by both tracks. **This is the entire dependency surface
for Track A and Track B** — once it exits, the tracks diverge and run in parallel
depending on nothing but what is delivered here.

The goal is to reach the **divergence point** fast, so keep this lean. Internal
order: scaffold first, then schemas + the Pi spike in parallel, then the
contract, then the stub + Swift bridge in parallel.

## F0.1 — Repo & toolchain scaffold *(blocks all)*
- Bun workspace monorepo, strict `tsconfig`, biome, pinned Effect
  `4.0.0-beta.97`, verification gate (`build` / `typecheck` / `test`) — the gate
  agents must pass post-cutover.
- Proposed layout:
  ```
  packages/domain/       # Effect Schema: read model, events, commands, session I/O
  packages/contract/     # effect/unstable/rpc RpcGroup over domain
  packages/daemon/       # Track A
  packages/stub-server/  # serves the contract with fake data (unblocks Track B)
  apple/Sprinter/        # Track B — SwiftUI app + Swift RPC client
  fixtures/              # captured real pi --mode rpc transcripts
  ```

## F0.2 — Domain schemas (`packages/domain`) *(blocks contract)*
- `Schema` for the read model: Workstream / Epic / Issue / Job / Session and their
  statuses; IDs; the workstream→epic→Issue→PR mapping shape.
- The **client-facing `SessionEvent` schema** — rich enough to render a full
  transcript in the Inspector (messages, tool calls, diffs, thinking, ui-requests).
  Owned and Swift-stable — **not** Pi's `AgentSessionEvent`.
- Command payloads and session I/O (`SessionInput`, `UiResponse`).

## F0.3 — Pi-protocol spike + fixture *(de-risks Track A; feeds the stub)*
- Run `pi --mode rpc` by hand; capture the NDJSON stream to `fixtures/`.
- Use it to (a) author/validate the **owned Pi protocol schema** against real
  output, and (b) author the `SessionEvent` schema (F0.2) as a translation
  target. Seed the stub's synthetic transcript from this capture so Track B builds
  against real-shaped data.

## F0.4 — RPC contract v1 (`packages/contract`) *(the freeze point; blocks Track B)*
- The `RpcGroup` over the domain schemas, covering all four models: `snapshot`
  (query), `events` (streaming subscription), commands
  (`createWorkstream` / `controlWorkstream` / …), and the session channel
  (`sessionEvents` stream, `sessionSend`, `interrupt`, `answerUiRequest`).
- Explicitly **versioned**. "Frozen enough to mirror in Swift and build a stub."

## F0.5 — Stub server (`packages/stub-server`) *(the divergence enabler)*
- An `RpcServer` Layer serving the contract from **in-memory fixtures** + a
  synthetic event generator (replays the captured transcript over `sessionEvents`,
  emits canned board deltas over `events`, accepts and echoes commands).
- Runnable locally over the same transport the real daemon will use.

## F0.6 — Swift contract bridge *(blocks Track B's client)*
- Schema→Swift DTO codegen (or a documented hand-mirror) + **golden JSON
  fixtures** to test the Swift decoders against the contract.
- A minimal Swift RPC client skeleton that connects to the stub and round-trips
  one query + one stream.

---

## Exit criteria — the divergence point

> The **stub server** serves contract v1, and a **Swift client** decodes its
> `snapshot` and consumes one live stream.

At this point both tracks have their entire dependency surface. They diverge and
proceed in parallel — see [Track A](./track-a-daemon.md) and
[Track B](./track-b-ui.md).
