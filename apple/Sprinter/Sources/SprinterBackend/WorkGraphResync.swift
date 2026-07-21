import SprinterContract

/// The reconnecting, snapshot-consistent work-graph feed (D4 / D17 / INV-REACTIVE)
/// — the reactive spine that keeps the exposed read model current across dropped
/// connections.
///
/// It is expressed purely on the ``Backend`` port (INV-PORT): construction takes a
/// `connect` seam that yields a freshly connected ``Backend`` per attempt, so the
/// engine never names a transport or where the daemon runs.
///
/// **First connect — subscribe-around-snapshot (D4).** The first attempt starts the
/// live `events` reader FIRST (buffering deltas into ``BoundedDeltaQueue``), THEN
/// fetches the `snapshot` baseline, THEN folds the buffered + live deltas onto it with
/// ``SnapshotReconciler``. This is the ordering the daemon's `events` handler requires.
/// Reconcile is idempotent (upsert-by-id), so a buffered delta already reflected in the
/// snapshot is a harmless re-apply. Every published value is a full baseline-consistent
/// ``Snapshot`` (never a bare delta), so a slow consumer can coalesce to the latest.
///
/// **Reconnect — offset-based durable replay (CE2.2).** As deltas apply, the engine
/// tracks the last-applied **contiguous-prefix** offset (``ContiguousOffsetTracker``)
/// and retains the latest published state. On a drop it resumes by handing that offset
/// back as `sinceOffset`, so the daemon replays STRICTLY AFTER it and the engine folds
/// the new deltas onto the RETAINED baseline — an **incremental** resume, NOT a fresh
/// snapshot re-derive. This closes the disconnect gap completely (the daemon replays
/// gaplessly after the cursor) and removes the snapshot-reflicker of the old
/// re-derive-on-every-reconnect path. Only a first connect (or a drop before any
/// baseline was retained) falls back to subscribe-around-snapshot.
///
/// **A DEAD resume point — `ResyncRequired` (the store-generation boundary).** The
/// incremental resume is only valid while the daemon's durable log is the SAME log the
/// cursor came from — which the client asserts EXPLICITLY, by sending the
/// ``Snapshot/generation`` its retained baseline came from alongside the cursor, rather
/// than leaving the daemon to infer staleness from offsets it cannot distinguish. It is
/// not forever: the daemon's store never migrates, so a schema bump DROPS and recreates
/// it, restarting offsets at `1` and destroying every entity the retained baseline
/// describes. The app and the daemon run together locally, so that lands on a live
/// client. The daemon says so explicitly — the `events` request fails with
/// ``ContractError/resyncRequired(sinceOffset:maxOffset:generation:)`` — and this engine
/// answers by discarding BOTH the retained baseline and the cursor and falling back to
/// subscribe-around-snapshot. It has to discard both: deltas are upsert-only (there is
/// no `*Removed`), so no stream of deltas can remove an entity the reset destroyed —
/// only a fresh `snapshot()` can.
///
/// **Contiguous prefix, not max-seen.** The live feed can publish out of durable-offset
/// order under concurrent daemon writers, so the resume cursor is the highest offset
/// whose entire prefix has been applied — never the max — else a reconnect could skip an
/// event that is `≤ max` but was never applied (see ``ContiguousOffsetTracker``).
///
/// **Bounded backlog → resync (the production flow control).** The un-reconciled delta
/// backlog is bounded by ``BoundedDeltaQueue``: if the reconciler falls behind the `events`
/// stream, the overflow forces a reconnect+resync rather than dropping deltas or growing
/// unbounded (the carried #36 F1 recovery). This — NOT daemon-stalling demand-gating — is
/// the real end-to-end flow control on the ``RpcBackend`` path: that adapter drains the
/// per-request ``AckGate`` into an unbounded stream, so the ack does not throttle the
/// daemon; the daemon is instead capped by the ``AckGate`` backlog bound and, downstream,
/// this bounded queue whose overflow triggers the resync. Because the resume cursor tracks
/// only APPLIED offsets, the incremental resume re-sends whatever overflowed — no loss. A
/// bounded resync is the intended, better-for-a-live-UI mechanism (a live view coalesces to
/// the latest baseline; it does not need every intermediate delta stalled into it).
///
/// **Reconnect backoff + jitter (CE2.2).** Reconnect delays follow ``ReconnectBackoff`` —
/// exponential with full jitter — so a persistently-failing daemon is retried with a
/// widening, de-synchronized delay rather than a tight constant loop. The schedule resets on
/// a demonstrably-HEALTHY connection, assessed at each attempt's teardown: healthy means the
/// attempt made a successful read (a snapshot or delta) OR stayed established at least a
/// minimum duration (``minHealthyDuration``, measured on an injected clock). The
/// established-duration signal is what covers a live-but-IDLE connection that delivered no
/// deltas — so a healthy connection that drops during a quiet period reconnects promptly from
/// `base` rather than being mis-scored as a failure and widening the ceiling toward the cap.
/// An accept-then-immediately-drop flap (no read, dropped before the threshold) is NOT
/// healthy, so the backoff widens against it as intended.
public actor WorkGraphResync {
  /// Yields a freshly connected ``Backend`` for one (re)connect attempt.
  public typealias Connect = @Sendable () async throws -> any Backend
  /// Invoked for each published baseline-consistent state before it reaches the
  /// feed — the hook a view model uses to project state onto the main actor.
  public typealias StateObserver = @Sendable (Snapshot) async -> Void

  private let connect: Connect
  private let reconciler: SnapshotReconciler
  private let bufferLimit: Int
  private var backoff: ReconnectBackoff
  private let observer: StateObserver?
  /// The clock the health assessment measures established-duration on (INJECTED so tests
  /// drive the health window deterministically — a manual clock the test advances — with no
  /// wall-clock wait). Only ``runAttempt(_:resume:_:clock:)`` reads ``Clock/now`` from it;
  /// the reconnect delay is scheduled through ``ReconnectBackoff`` (whose jitter tests
  /// neutralize to zero), so no bounded await depends on real time.
  private let clock: any Clock<Duration>
  /// The minimum established-duration that marks a subscription healthy even if it delivered
  /// NO deltas (the idle-connection case). A connection that stayed up at least this long
  /// before dropping is scored healthy → the backoff resets; a connection that drops sooner
  /// WITHOUT any successful read is a flap → the backoff widens.
  private let minHealthyDuration: Duration
  /// The bounded out-of-order gap window handed to each ``ContiguousOffsetTracker``: a gap
  /// that never fills within it forces a resync instead of freezing the resume cursor.
  private let gapLimit: Int

  private var driver: Task<Void, Never>?
  private var started = false

  /// What the next attempt would resume FROM — the retained baseline plus the
  /// last-applied contiguous-prefix cursor (see ``ResumePoint``).
  private var resumePoint = ResumePoint()
  /// Whether the CURRENT attempt has seen a successful read yet (a published snapshot on a
  /// first connect, or an applied delta on a resume). Reset at each attempt's start; part of
  /// the health assessment at teardown (see ``assessHealth(establishedFor:)``).
  private var attemptSawRead = false

  /// Builds the engine over a `connect` seam (the port-only construction path).
  public init(
    connect: @escaping Connect,
    reconciler: SnapshotReconciler = SnapshotReconciler(),
    bufferLimit: Int = 256,
    backoff: ReconnectBackoff = ReconnectBackoff(),
    clock: any Clock<Duration> = ContinuousClock(),
    minHealthyDuration: Duration = .seconds(3),
    gapLimit: Int = 1024,
    observer: StateObserver? = nil
  ) {
    self.connect = connect
    self.reconciler = reconciler
    self.bufferLimit = bufferLimit
    self.backoff = backoff
    self.clock = clock
    self.minHealthyDuration = minHealthyDuration
    self.gapLimit = gapLimit
    self.observer = observer
  }

  /// Convenience: connect through a ``BackendConnector`` to a fixed endpoint. The
  /// engine reconnects by re-resolving the same endpoint (a fresh transport each
  /// time), so local vs. remote stays an adapter choice (INV-PORT).
  public init(
    connector: BackendConnector,
    endpoint: DaemonEndpoint,
    reconciler: SnapshotReconciler = SnapshotReconciler(),
    bufferLimit: Int = 256,
    backoff: ReconnectBackoff = ReconnectBackoff(),
    clock: any Clock<Duration> = ContinuousClock(),
    minHealthyDuration: Duration = .seconds(3),
    gapLimit: Int = 1024,
    observer: StateObserver? = nil
  ) {
    self.init(
      connect: { try await connector.connect(to: endpoint) },
      reconciler: reconciler,
      bufferLimit: bufferLimit,
      backoff: backoff,
      clock: clock,
      minHealthyDuration: minHealthyDuration,
      gapLimit: gapLimit,
      observer: observer)
  }

  /// The live feed of baseline-consistent snapshots. Starts the reconnect loop on
  /// the first call; terminating the returned stream stops the loop. A second call
  /// returns an already-finished stream (single consumer).
  public func states() -> AsyncStream<Snapshot> {
    // `.bufferingNewest(1)` makes "coalesce to latest" real: a slow consumer that
    // reads only via `states()` (no `observer` backpressure) keeps just the newest
    // baseline-consistent snapshot rather than an unbounded backlog of full
    // `Snapshot`s — each published value already supersedes the prior one.
    let (stream, continuation) = AsyncStream<Snapshot>.makeStream(
      bufferingPolicy: .bufferingNewest(1))
    guard !started else {
      continuation.finish()
      return stream
    }
    started = true
    continuation.onTermination = { [weak self] _ in
      Task { await self?.stop() }
    }
    driver = Task { await self.run(continuation) }
    return stream
  }

  /// Stops the reconnect loop; the feed finishes. Idempotent.
  public func stop() {
    driver?.cancel()
    driver = nil
  }

  // MARK: - Reconnect loop

  private func run(_ continuation: AsyncStream<Snapshot>.Continuation) async {
    while !Task.isCancelled {
      do {
        let backend = try await connect()
        // NOTE: the backoff is NOT reset here. A bare `connect()` success is not health — a
        // daemon that accepts the socket then immediately drops the subscription would reset
        // `failures` to 0 every attempt, pinning the delay at `base` and hot-spinning against
        // a flapping daemon. The attempt instead assesses its CONNECTION HEALTH at teardown
        // (see ``runAttempt`` / ``assessHealth(establishedFor:)``) and resets the backoff
        // only when the connection proved healthy — it made a successful read OR stayed
        // established at least ``minHealthyDuration``. An accept-then-immediately-drop flap
        // (no read, dropped before the threshold) is scored a failure and the backoff widens.
        let resume = resumePoint.resumable
        await runAttempt(backend, resume: resume, continuation)
        // Fully drain the OLD transport BEFORE the loop dials the next one (the CE2.1
        // carried teardown constraint): `close()` awaits the socket's read-loop exit and
        // fd release, so no in-flight frame crosses connections.
        await backend.close()
      } catch {
        // `connect` failed: fall through to the (widening) backoff and retry.
      }
      if Task.isCancelled { break }
      try? await Task.sleep(for: backoff.next())
    }
    continuation.finish()
  }

  /// Records the applied state and contiguous-prefix cursor so they survive to the next
  /// attempt. Called BEFORE publishing so a stalled observer cannot lose the cursor.
  private func record(state: Snapshot, contiguous: Int?) {
    resumePoint.record(state: state, contiguous: contiguous)
  }

  /// True when `error` is the daemon telling this client its resume point is dead.
  private func isResyncRequired(_ error: any Error) -> Bool {
    guard case ContractError.resyncRequired = error else { return false }
    return true
  }

  /// Records that the current attempt made a successful read (a published snapshot on a
  /// first connect, or an applied delta on a resume) — one of the two health signals. On
  /// the resume path this is the ONLY read-based signal; the other, established-duration, is
  /// what covers an idle connection that reads nothing. Idempotent (per-delta calls harmless).
  private func noteRead() {
    attemptSawRead = true
  }

  /// Resets the current attempt's health accounting at its start.
  private func beginAttempt() {
    attemptSawRead = false
  }

  /// The connection-health assessment run at an attempt's teardown. The just-ended
  /// subscription is HEALTHY — so the backoff resets, and a later drop reconnects promptly
  /// from `base` — when EITHER it made a successful read (a snapshot/delta proves the
  /// connection was live and functioning) OR it stayed established at least
  /// ``minHealthyDuration`` (proof of a live-but-idle connection that simply had no deltas
  /// to deliver — the quiet-period case). A connection that dropped sooner WITHOUT any read
  /// is an accept-then-flap: NOT healthy, so the backoff is left to widen.
  ///
  /// Assessing at teardown (rather than resetting mid-attempt) is race-free: the reconnect
  /// delay is only ever drawn AFTER the attempt returns, so a teardown reset and a
  /// mid-attempt reset are equivalent for the delay — and the elapsed measurement reads the
  /// clock synchronously here, so an injected manual clock makes the health window exact.
  private func assessHealth(establishedFor elapsed: Duration) {
    if attemptSawRead || elapsed >= minHealthyDuration {
      backoff.reset()
    }
  }

  /// One attempt. Opens the injected existential clock so the health assessment can measure
  /// established-duration with concrete instant arithmetic (an `any Clock<Duration>` erases
  /// its `Instant` type), then runs the attempt against the opened clock.
  private func runAttempt(
    _ backend: any Backend,
    resume: (state: Snapshot, offset: Int)?,
    _ continuation: AsyncStream<Snapshot>.Continuation
  ) async {
    await runAttempt(backend, resume: resume, continuation, clock: clock)
  }

  /// One attempt against a concrete `clock`. With `resume == nil` it subscribes-around-
  /// snapshot (first connect); otherwise it resumes the `events` stream from `resume.offset`
  /// and folds live deltas onto the retained baseline (incremental durable replay). Any
  /// failure (snapshot error, transport drop, bounded-buffer overflow, or a stalled offset
  /// gap) returns so the loop reconnects. On the way out it assesses connection health from
  /// the elapsed established-duration and whether a read occurred, resetting the backoff for
  /// a healthy connection (see ``assessHealth(establishedFor:)``).
  private func runAttempt<C: Clock<Duration>>(
    _ backend: any Backend,
    resume: (state: Snapshot, offset: Int)?,
    _ continuation: AsyncStream<Snapshot>.Continuation,
    clock: C
  ) async {
    beginAttempt()
    let establishedAt = clock.now
    let queue = BoundedDeltaQueue<OffsetEvent>(limit: bufferLimit)
    let gapLimit = gapLimit
    // Establish the subscription BEFORE the snapshot request (on a first connect) — on
    // the connection's serialized send path this issues the `events` Request ahead of
    // `snapshot`. On a reconnect there is no snapshot: the daemon replays strictly after
    // `resume.offset`.
    // The resume sends the cursor WITH the generation it was minted in — the one carried
    // on the retained baseline's ``Snapshot/generation`` — as ONE
    // ``SprinterContract/ResumeContext``. They cannot come apart, and there is no offset
    // value that reads as "origin": the daemon distinguishes a first connect from a
    // resume by the ABSENCE of this value, and compares the generation on every present
    // one. A stale resume is refused, which is exactly what this engine wants when the
    // daemon's store was dropped underneath it.
    let events = backend.events(
      resume: resume.map { ResumeContext(sinceOffset: $0.offset, generation: $0.state.generation) })
    do {
      try await withThrowingTaskGroup(of: Void.self) { group in
        // Reader: buffer live deltas (with their durable offsets) into the bounded queue.
        group.addTask {
          do {
            for try await offsetEvent in events {
              try await queue.enqueue(offsetEvent)
            }
            await queue.finish()
          } catch {
            // A transport drop or a bounded-buffer overflow: unblock the reconciler and
            // surface the failure so the loop reconnects (and resumes from the cursor).
            await queue.finish()
            throw error
          }
        }
        // Folder: publish/retain the baseline, then fold buffered + live deltas, tracking
        // the contiguous-prefix offset for the next resume.
        group.addTask { [self] in
          try await fold(
            from: queue, backend: backend, resume: resume, gapLimit: gapLimit, continuation)
        }
        // DRAIN BOTH halves — never just the first to finish. They are coupled: the reader
        // hands the folder its terminator (`queue.finish()`) BEFORE it rethrows, so a failing
        // reader makes the folder runnable and then races it. Taking only `group.next()` let
        // the folder's normal completion win that race and DISCARD the reader's error — and a
        // swallowed `ResyncRequired` leaves the next attempt re-sending a refused cursor.
        // Every child's outcome is collected, and the first REAL failure is the attempt's.
        // `CancellationError` is `cancelAll()`'s own bookkeeping: a genuine error outranks it,
        // and on its own it is NOT an outcome — the throw below filters it back out.
        var failure: (any Error)?
        var draining = true
        while draining {
          do {
            if try await group.next() == nil { draining = false }
          } catch {
            if failure == nil || failure is CancellationError { failure = error }
          }
          // One half finishing ends the attempt; cancelling the other bounds the drain (both
          // the reader's stream and the folder's queue wait are cancellation-aware).
          group.cancelAll()
        }
        if let failure, !(failure is CancellationError) { throw failure }
      }
    } catch {
      // Reconnect: the next attempt re-dials and resumes incrementally from the cursor —
      // UNLESS the daemon said that cursor is dead. `ResyncRequired` means the store was
      // dropped and recreated (a schema-version bump never migrates), so both the cursor
      // and the retained baseline belong to a generation that no longer exists; drop them
      // so the next attempt subscribes around a FRESH `snapshot()` instead of folding
      // deltas onto state the reset destroyed.
      if isResyncRequired(error) {
        resumePoint.discard()
      }
    }
    // Assess connection health for THIS attempt before the loop draws the next reconnect
    // delay: reset the backoff if the connection made a read or stayed established long
    // enough (healthy), else leave it to widen (a flap). Elapsed is measured on the injected
    // clock, read synchronously here so the window is exact under a manual test clock.
    assessHealth(establishedFor: establishedAt.duration(to: clock.now))
  }

  /// The folder half of an attempt: on a first connect fetch and publish the baseline
  /// (subscribe-around-snapshot); then fold each buffered/live delta onto the retained
  /// baseline, tracking the contiguous-prefix offset for the next resume and recording each
  /// read as a health signal. A permanently-missing offset trips ``ContiguousOffsetTracker``'s
  /// bounded-gap guard (``ContiguousOffsetTracker/StalledGap``), which throws here to collapse
  /// the attempt into a resync from the frozen cursor rather than freezing progress and growing
  /// the ahead-set unbounded (FIX B).
  private func fold(
    from queue: BoundedDeltaQueue<OffsetEvent>,
    backend: any Backend,
    resume: (state: Snapshot, offset: Int)?,
    gapLimit: Int,
    _ continuation: AsyncStream<Snapshot>.Continuation
  ) async throws {
    var state: Snapshot
    var tracker: ContiguousOffsetTracker
    if let resume {
      // Incremental resume: fold onto the retained baseline — no fresh snapshot.
      state = resume.state
      tracker = ContiguousOffsetTracker(resumingAfter: resume.offset, gapLimit: gapLimit)
    } else {
      // First connect: fetch and publish the baseline (subscribe-around-snapshot).
      let base = try await backend.snapshot()
      state = base
      tracker = ContiguousOffsetTracker(gapLimit: gapLimit)
      record(state: base, contiguous: nil)
      await observer?(base)
      continuation.yield(base)
      // A published snapshot IS a successful read — one of the two health signals.
      noteRead()
    }
    while let offsetEvent = await queue.next() {
      state = reconciler.reconcile(state, applying: offsetEvent.event)
      try tracker.observe(offsetEvent.offset)
      record(state: state, contiguous: tracker.contiguous)
      await observer?(state)
      continuation.yield(state)
      // An applied delta IS a successful read (the only read signal on the resume path).
      noteRead()
    }
  }
}
