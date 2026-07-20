# Workstream `BND` — App↔Daemon boundary test plan

> Implementation spec: an epic set + sequencing graph + cross-cutting invariants;
> one issue per **task**, each with acceptance (**Done**) and dependency edges.
>
> **Goal:** make the **App↔Daemon boundary** (Swift app ↔ Bun/TS daemon, cross-process,
> NDJSON `effect/unstable/rpc` over a Unix-domain socket) covered by an automated,
> deterministic, hermetic test suite that gates CI — so serialization drift, framing
> edges, resource-lifetime hazards, reconnect/offset loss, and restart faults are
> **caught before merge**, not in production.
>
> **Prerequisites:** `CVG` landed (`main` @ `9237595`) — daemon `main`/`run.ts`, the
> served socket transport, the Swift `UnixSocketTransport`/`RpcBackend`/`WorkGraphResync`
> stack, contract goldens, and the CE4.1 acceptance harness all exist and are the
> substrate this workstream hardens.

## Cross-cutting invariants

Both repo gates apply — each task names the side(s) it touches and honours that side's gate.

| Id | Invariant | Guard |
|----|-----------|-------|
| `INV-DETERMINISM` | No test consumes real wall-clock time. All timing is driven by an injected clock (`TestClock`/`ManualClock` TS; the Swift `ManualClock`/`Clock` seam). Real `sleep`/`Task.sleep`/`setTimeout` for delay is banned in tests. | lint guard (`BT7`) + review |
| `INV-BOUNDED` | Every `await`/blocking wait in a test is bounded by a hard timeout; a deadlock **fails** (never hangs). Under a virtual clock, an un-advanced wait must surface as a test failure, not a stall. | lint guard (`BT7`) + CI wall-clock budget |
| `INV-REAL-WIRE` | Boundary (L3+) tests exercise the **real** serialization + NDJSON framing + socket + RPC dispatch + read-model projection. Only non-deterministic **leaves** are substituted, and only at a DI `Layer`/seam: `pi` (via the `ChildProcessSpawner` seam) and GitHub (the in-process `Repository`). No fake stands in for the wire itself. | review |
| `INV-TWO-SIDED` | Every wire type has golden vectors decoded by **both** the TS contract (`@sprinter/contract`) and the Swift mirror (`SprinterContract`). A contract change re-freezes both in one change; goldens are generated, never hand-edited. | `bun run check` + `make check` + review |
| `INV-HONEST-SCOPE` | Each test documents what it does **not** prove. No seeded/tautological assertion may be named as a boundary proof (e.g. asserting seeded PR data round-trips is not "the loop produced a PR"). | review |
| `INV-NO-LEAK` | Every test that starts a daemon process / binds a socket / spawns a child / opens a tempfile tears all of it down at teardown, including on failure. No leaked fd, process, socket path, or temp dir survives the test. | review + a leak-scan check (`BT7`) |
| `INV-HERMETIC-CI` | The CI suite performs **no** real network I/O, spawns **no** real `pi`, touches **no** real GitHub, and reads **no** ambient credentials. Any check requiring real infra is excluded from CI and delivered as a documented runbook step. | CI config + review |
| `INV-GATE-A` / `INV-GATE-B` | Daemon side `bun run check` green / app side `make check` green (format + lint + typecheck/build + tests + coverage). | CI |
| `INV-COV` | ≥ 75% line & function coverage on the task's touched **non-fixture** modules. | coverage gate in each side's `check` |
| `INV-NOCAST` / `INV-NOFORCE` | Daemon: no `as`/non-null `!`/`any` (`oxlint`). App: no force unwrap/cast/try, no IUOs (`SwiftLint --strict`). | linters |
| `INV-PIN` / `INV-NAMING` | New deps exact-pinned + lockfile committed; follows `conventions.md`. | `check` + review |

## Epics — set & sequencing

| Epic | Name | Side | Depends on |
|------|------|------|-----------|
| `BT7` | Determinism & anti-hang enforcement | both (tooling) | — |
| `BT1` | Contract conformance & golden completeness (L1) | both | — |
| `BT2` | Reusable real-wire harness fixture (L3 base) | daemon (+ swift opt) | — |
| `BT3` | Transport & framing edge conformance | app + daemon | `BT2` |
| `BT4` | Reconnect / resync / offset correctness | app (+ daemon) | `BT2` |
| `BT5` | Restart & durability chaos | daemon (+ app) | `BT2` |
| `BT6` | Fault injection & graceful-degradation | both | `BT2` |
| `BT8` | Cross-language full-stack (gated, non-CI) | both | `BT2`, `BT3`, `BT4` |

```
BT7 (determinism guard) ┐
BT1 (goldens)           ├─ foundational, parallel
BT2 (harness fixture) ──┴─► BT3, BT4, BT5, BT6  ─► BT8 (gated cross-language)
```

> `BT7` and `BT1` gate everything (a suite that can hang, or that lets the wire drift,
> undermines every other epic). `BT2`'s fixture is the substrate `BT3`–`BT6` reuse — do
> not let those epics each re-invent daemon boot/teardown. `BT8` is **conditional** (a
> real cross-language e2e is expensive/flaky); if not run in CI, it lands as a runbook.

---

## Epic `BT7` — Determinism & anti-hang enforcement  ·  tags: `INV-DETERMINISM`, `INV-BOUNDED`, `INV-NO-LEAK`

### BT7.1 — Ban real-time & unbounded waits in tests (lint guard)
- **Done:** a check (custom `oxlint`/eslint rule or a `bun`/`make` script over the test
  trees) that **fails** the gate when a test file (`*.test.ts`, `apple/**/Tests/**`)
  contains: a real-delay call (`setTimeout`/`setInterval`/`Bun.sleep`/`Effect.sleep`
  with a non-zero literal not derived from a test clock; Swift `Task.sleep`/`sleep`/
  `usleep` / `Thread.sleep`) OR an unbounded stream/queue `await` with no timeout
  wrapper. Provide the sanctioned primitives instead: a `withTestTimeout(effect, bound)`
  TS helper and a Swift `withTimeout(_:)` (fails the test on expiry). An allow-list
  annotation (`// deterministic: <reason>`) is permitted only with a stated reason and
  is itself reviewed. Wire the check into both gates.
- **Depends on:** —

### BT7.2 — CI wall-clock budget + leak scan
- **Done:** the CI suite runs under a **hard wall-clock budget** per suite (a failing
  timeout kills + reports, never hangs the runner); and a **post-suite leak scan**
  asserts zero surviving child processes, bound sockets under the test temp root, or
  leaked temp dirs after the run. Both are CI-config + a small script; document the
  budget and how to raise it.
- **Depends on:** BT7.1

---

## Epic `BT1` — Contract conformance & golden completeness (L1)  ·  tags: `INV-TWO-SIDED`, `INV-CONTRACT`, `INV-GATE-A`, `INV-GATE-B`

### BT1.1 — Golden coverage completeness gate
- **Done:** a check that enumerates every wire type in `packages/contract/src/rpc.ts`
  (all `Rpc` payload/success/error schemas, `WorkGraphEvent`/`OffsetEvent`, `Snapshot`,
  session-channel frames, `TranscriptEntry`, `Notice`/`NoticeEntry`, `UiRequest*`) and
  **fails** if any lacks a golden vector under
  `apple/Sprinter/Tests/SprinterContractTests/Goldens/`. Goldens are produced only by
  `apple/Sprinter/scripts/generate-goldens.ts` (regeneration is drift-free); both the TS
  decode test and the Swift `SprinterContractTests` decode every golden.
- **Depends on:** —

### BT1.2 — Negative / boundary decode vectors
- **Done:** for each wire type, negative vectors that **must fail** decode on both sides
  (unknown tagged-union `_tag`; missing required field; wrong scalar type; empty
  `NonEmptyString`; negative `NonNegativeInt`; `undefined` vs present-empty payload for
  optional-payload RPCs — the `events` `{}` vs omitted case) plus the exact
  cross-boundary asymmetries documented (Swift not re-checking refinements). Each side
  asserts the decode **rejects** (or the documented tolerant behaviour), so a
  serialization change that would silently accept garbage fails the gate.
- **Depends on:** BT1.1

### BT1.3 — Round-trip & mirror-parity property
- **Done:** a generative/round-trip test per type — encode (TS) → bytes → decode (TS)
  and (via goldens) decode (Swift) → re-encode (Swift, where the mirror encodes) →
  byte-equal — proving encode/decode are inverse and the Swift mirror is byte-parity with
  the TS contract for the frozen contract. A `contract-mirror.md` parity assertion
  (the doc's stated version == both sides' constant) is part of the gate.
- **Depends on:** BT1.1

---

## Epic `BT2` — Reusable real-wire harness fixture (L3 base)  ·  tags: `INV-REAL-WIRE`, `INV-NO-LEAK`, `INV-DETERMINISM`

### BT2.1 — Daemon-over-real-socket fixture with substitutable leaves
- **Done:** a **reusable** test fixture (factored out of `packages/daemon/src/acceptance.test.ts`)
  that: boots the **real** `mainLayer`/`bootLayer` graph on a **real** Unix socket at a
  short temp path (respect `sun_path` ≤ ~104 bytes), file-backed `StateStore` on a temp
  db, `SESSION_RESOLVE_TIMEOUT` overridable; substitutes ONLY (a) the `pi` via a
  **scripted `ChildProcessSpawner`** emitting a caller-supplied frame script to a terminal
  `SessionResult`, and (b) an **in-process sandboxed `Repository`** with programmable
  Issue/PR responses; exposes a **real `RpcClient`** (`RpcClient.layerProtocolSocket` +
  `BunSocket.layerNet` + NDJSON) byte-identical to the app's transport; and provides
  `acquireRelease` teardown that kills the daemon fiber, unlinks the socket, closes the
  db, and removes the temp root (`INV-NO-LEAK`). All readiness waits are bounded.
- **Depends on:** —

### BT2.2 — Observation contract for assertions
- **Done:** the fixture exposes the same **read-model projections** the app consumes
  (`snapshot()` + the `events`/`OffsetEvent` feed → the board projection, the session
  channel → transcript, the inspector pairing), and a bounded `awaitProjection(pred)`
  helper that converges to a **named terminal state** and fails on timeout. Assertions
  are made on observable projected state (never on internal daemon fields), so a test
  proves what a real client would see.
- **Depends on:** BT2.1

---

## Epic `BT3` — Transport & framing edge conformance  ·  tags: `INV-REAL-WIRE`, `INV-NOFORCE`, `INV-GATE-B`

Anchors: `apple/Sprinter/Sources/SprinterBackend/{UnixSocketTransport,RpcConnection,NdjsonFraming,Envelope,AckGatedStream}.swift`; daemon `RpcSerialization.layerNdjson`.

### BT3.1 — NDJSON framing edges
- **Done:** tests over a controllable socket/pipe pair asserting: a single logical frame
  **split across two writes** is reassembled; **multiple frames coalesced** in one read
  are split; a partial trailing frame is buffered until complete; oversized/garbage lines
  surface a typed decode error (never a silent drop, never a crash); EOF mid-frame
  terminates cleanly. Both directions (request encode, response decode incl. the
  `OffsetEvent` stream and `Exit`/error frames).
- **Depends on:** `BT2`

### BT3.2 — Socket resource-lifetime hazards
- **Done:** deterministic tests for: `write(2)` racing `close()` never lands on a
  reclaimed fd (no cross-talk); the read loop racing `close()` gates the real `close(2)`
  on read-loop exit; **`SO_NOSIGPIPE`** (or equivalent) so a write to a peer-closed socket
  yields `EPIPE` → typed `writeFailed`, never `SIGPIPE`/process death; a dropped
  `Backend`/connection without `close()` is asserted (or documented) re: thread+fd
  ownership. Each covers the exact reconnect-teardown ordering used in production.
- **Depends on:** `BT2`

### BT3.3 — Demand-gated backpressure & bounded buffers
- **Done:** the per-batch `Ack` is deferred until the consumer drains; a **bounded** inbound
  buffer overflows to a typed error → **snapshot-resync** (never unbounded growth, never
  silent drop); overflow is self-cleaning (an `Interrupt` is sent, no pending-request
  leak). Assert memory-boundedness through the real `RpcBackend.events` path under a fast
  producer + stalled consumer.
- **Depends on:** `BT2`

---

## Epic `BT4` — Reconnect / resync / offset correctness  ·  tags: `INV-REAL-WIRE`, `INV-BOUNDED`, `INV-GATE-B`

Anchors: `WorkGraphResync.swift`, `ContiguousOffsetTracker.swift`, `ReconnectBackoff.swift`, `SnapshotReconciler.swift`; daemon `event-journal.ts` `resyncFrom`.

### BT4.1 — Offset-based incremental resume (no loss, no dup)
- **Done:** a drop-then-reconnect over the real wire resumes from the **last-applied
  offset** via the `sinceOffset` cursor (incremental, not a snapshot re-derive), and
  the post-reconnect applied state equals a from-origin fold — asserted for: in-order
  delivery; the durable-replay/live-tail **boundary overlap** (duplicate offset at the
  seam is idempotent); and a genuinely-missing tail → resync.
- **Depends on:** `BT2`

### BT4.2 — Contiguous-prefix cursor loss-freeness
- **Done:** under **out-of-order** live delivery (offset 5 before 3), the client tracks the
  **contiguous-prefix** offset (not max-seen), so a reconnect after an out-of-order
  sequence loses no event; a permanently-missing offset stalls the prefix and triggers a
  bounded-gap **resync** (no unbounded `ahead` growth, no frozen cursor). Includes the
  origin-seed case (a higher offset before a lower one at origin discards nothing).
- **Depends on:** `BT2`

### BT4.3 — Reconnect backoff schedule & health
- **Done:** exponential backoff + full jitter within `[0, ceiling]`, ceiling grows and
  caps; backoff **resets on demonstrated health** (a successful read OR a min-established
  duration — driven by the injected clock), so an accept-then-instant-drop **flap** widens
  the delay while a healthy-idle reconnect does not; all driven virtually (no real delay).
- **Depends on:** `BT2`

---

## Epic `BT5` — Restart & durability chaos  ·  tags: `INV-RESTART`, `INV-REAL-WIRE`, `INV-GATE-A`, `INV-GATE-B`

Anchors: `startup-reconcile.ts`, `session-registry.ts` (`resolveLive`), `rpc-handlers.ts`, `job-runner.ts`, `state/src/sqlite.ts`.

### BT5.1 — Build-write-restart-read across the wire
- **Done:** dispatch a job to a live (scripted-pi) session, **restart the real daemon
  process** mid-flight (kill fiber + rebind socket + fresh graph on the same file-backed
  db), assert the client **resyncs** (snapshot + offset replay) and the daemon
  **re-dispatches** persisted work; **1 Job = 1 session re-attached by id**; no work lost
  or duplicated. Client-side reconnect (`WorkGraphResync` + the app session-channel
  re-dial) is exercised, not assumed.
- **Depends on:** `BT2`

### BT5.2 — Startup-reconcile settle correctness (Job **and** Session rows)
- **Done:** `startup-reconcile`'s settle/skip path settles the **Session** row to terminal
  alongside the **Job** row for every settle (`succeeded`/`cancelled`/`queued`), so no
  stale non-terminal Session survives a settled Job; assert (a) a stray non-landed
  `running` Job under a **`done`** workstream is NOT resumed; (b) a **`queued`**-orphan
  under a paused (`blocked`) workstream **fails fast** on session-channel resolve (no
  `sessionResolveTimeout` stall — the `resolveLive` Job-status gate holds); (c) the
  register-after-persist window still bridges (a running Job whose handle is not yet
  registered waits, bounded).
- **Depends on:** BT5.1

### BT5.3 — SQLite/WAL durability semantics (deterministic core + runbook)
- **Done:** deterministic: a committed write is visible to a fresh `StateStore` connection
  on the same file (cross-connection durability); WAL mode asserted. The genuinely-real
  **SIGKILL-mid-write → reboot → WAL replay** is a **runbook** step (`INV-HERMETIC-CI`),
  not a CI test, and the test names are honest about the distinction (`INV-HONEST-SCOPE`).
- **Depends on:** BT5.1

---

## Epic `BT6` — Fault injection & graceful-degradation  ·  tags: `INV-REAL-WIRE`, `INV-HONEST-SCOPE`

### BT6.1 — Transient backend/host faults surface as typed errors, never defects/hangs
- **Done:** injected transient failures — a `StateStoreError` on a session-channel resolve
  read; a `Repository` host `404`/`403`/`429` mid reconcile-roll-up; a daemon socket close
  mid in-flight query — each surfaces as the **typed** channel error the consumer handles
  (`SessionNotFound`/`BackendError.connectionClosed`/isolated per-issue reconcile
  continue), never an unrecoverable **defect** that kills a long-lived stream fiber, and
  never a hang. `reconcileWorkstream` is asserted fail-**soft** (one bad issue does not
  abort the roll-up).
- **Depends on:** `BT2`

### BT6.2 — Malformed / adversarial wire input
- **Done:** the daemon and the app each **reject** malformed frames (bad envelope, unknown
  `_tag`, truncated JSON, an empty-string id where `NonEmptyString` is required) with a
  typed error and stay **live** (the connection/handler survives a bad frame where the
  protocol allows, or closes cleanly where it does not) — no crash, no silent accept, no
  unbounded buffering of a never-terminating frame.
- **Depends on:** BT3.1

---

## Epic `BT8` — Cross-language full-stack (gated, non-CI)  ·  tags: `INV-HONEST-SCOPE`, `INV-HERMETIC-CI`

### BT8.1 — Swift-transport → real-daemon e2e (periodic / opt-in)
- **Done:** an **opt-in** (non-CI-gating, env/flag-guarded) integration that drives the
  **real Swift `UnixSocketTransport` + `RpcBackend` + view models** against a spawned real
  Bun daemon (scripted-pi + sandboxed `Repository` leaves), asserting the board/session/
  inspector **view-model** state end-to-end — closing the one gap the TS-side L3 harness
  cannot (the Swift transport/view-model code is not in the L3 loop). If a stable
  cross-language CI harness is infeasible, this lands as a **runbook** + a documented
  manual cadence instead, with `INV-HONEST-SCOPE` noting exactly what CI does and does not
  cover.
- **Depends on:** `BT2`, `BT3`, `BT4`

---

## Provenance — where each epic's scope came from

Every task traces to a boundary hazard or technique exercised (and often a bug caught) during `CVG`.

| Task | Origin |
|------|--------|
| `BT7.1/7.2` | CVG CE2.2 #60 test deadlock (infinite `noDelay` reconnect loop hung the suite) → injected-clock + bounded-await discipline; leak scans from the reap protocol |
| `BT1.*` | FE2.4 freeze/divergence gate + the CE5/CE2.0 re-freeze pattern; CE2.0 R2 omitted-vs-empty `events` payload decode bug |
| `BT2.*` | CE4.1 #70 acceptance harness (`acceptance.test.ts`) — real daemon + real socket + real `RpcClient`, scripted-pi + sandboxed `Repository` leaves |
| `BT3.1` | CE2.1 #62 NDJSON reassembly (frame split across two writes) |
| `BT3.2` | CE2.1 #62 fd-lifetime race (write vs close, read-loop vs close) + SIGPIPE process-death bug |
| `BT3.3` | CE2.2 #63 demand-gated Ack + bounded-buffer overflow→resync + self-cleaning overflow |
| `BT4.1/4.2` | CE2.0/CE2.2 offset cursor + `ContiguousOffsetTracker` loss-freeness under out-of-order live delivery |
| `BT4.3` | CE2.2 #63 `ReconnectBackoff` flap-vs-idle health-reset |
| `BT5.1` | CE4.2 #71 build-write-restart-read |
| `BT5.2` | CE4.1 R3/R4 `resolveLive` Job-status gate + the `startup-reconcile` Session-row settle root fix |
| `BT5.3` | CE4.2 #71 WAL cross-connection visibility vs real crash-recovery (honest-scope) |
| `BT6.1` | CE1.3 reconcile per-issue error isolation + CE4.1 `StateStoreError`→graceful mapping |
| `BT6.2` | CE2.0 unknown-`_tag` decode-failure vector |
| `BT8.1` | CE4.1 harness-approach decision (TS-drives-socket vs Swift-spawns-bun) — the deliberately-uncovered Swift-side loop |
