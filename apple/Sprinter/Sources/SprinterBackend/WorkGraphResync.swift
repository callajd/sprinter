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
/// **Contiguous prefix, not max-seen.** The live feed can publish out of durable-offset
/// order under concurrent daemon writers, so the resume cursor is the highest offset
/// whose entire prefix has been applied — never the max — else a reconnect could skip an
/// event that is `≤ max` but was never applied (see ``ContiguousOffsetTracker``).
///
/// **Bounded backlog → resync.** The un-reconciled delta backlog is bounded by
/// ``BoundedDeltaQueue``: if the reconciler falls behind the demand-gated `events`
/// stream, the overflow forces a reconnect+resync rather than dropping deltas or growing
/// unbounded (the carried #36 F1 recovery). Because the resume cursor tracks only APPLIED
/// offsets, the incremental resume re-sends whatever overflowed — no loss.
///
/// **Reconnect backoff + jitter (CE2.2).** Reconnect delays follow
/// ``ReconnectBackoff`` — exponential with full jitter, reset on a successful connect —
/// so a persistently-failing daemon is retried with a widening, de-synchronized delay
/// rather than a tight constant loop.
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

  private var driver: Task<Void, Never>?
  private var started = false

  /// The latest published baseline-consistent state, retained so a reconnect resumes
  /// incrementally by folding onto it (never re-deriving from a fresh snapshot).
  private var retainedState: Snapshot?
  /// The last-applied contiguous-prefix offset — the `sinceOffset` cursor a reconnect
  /// resumes strictly after.
  private var resumeOffset: Int?

  /// Builds the engine over a `connect` seam (the port-only construction path).
  public init(
    connect: @escaping Connect,
    reconciler: SnapshotReconciler = SnapshotReconciler(),
    bufferLimit: Int = 256,
    backoff: ReconnectBackoff = ReconnectBackoff(),
    observer: StateObserver? = nil
  ) {
    self.connect = connect
    self.reconciler = reconciler
    self.bufferLimit = bufferLimit
    self.backoff = backoff
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
    observer: StateObserver? = nil
  ) {
    self.init(
      connect: { try await connector.connect(to: endpoint) },
      reconciler: reconciler,
      bufferLimit: bufferLimit,
      backoff: backoff,
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
        // A successful connect resets the backoff, so a healthy connection that later
        // drops reconnects promptly from `base` rather than inheriting a widened delay.
        backoff.reset()
        let resume = resumePoint()
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

  /// The incremental-resume point for the next attempt: the retained baseline plus the
  /// last-applied contiguous offset, or `nil` on a first connect (nothing retained yet).
  private func resumePoint() -> (state: Snapshot, offset: Int)? {
    guard let retainedState, let resumeOffset else { return nil }
    return (retainedState, resumeOffset)
  }

  /// Records the applied state and contiguous-prefix cursor so they survive to the next
  /// attempt. Called BEFORE publishing so a stalled observer cannot lose the cursor.
  private func record(state: Snapshot, contiguous: Int?) {
    retainedState = state
    if let contiguous {
      resumeOffset = contiguous
    }
  }

  /// One attempt. With `resume == nil` it subscribes-around-snapshot (first connect);
  /// otherwise it resumes the `events` stream from `resume.offset` and folds live deltas
  /// onto the retained baseline (incremental durable replay). Any failure (snapshot
  /// error, transport drop, or bounded-buffer overflow) returns so the loop reconnects.
  private func runAttempt(
    _ backend: any Backend,
    resume: (state: Snapshot, offset: Int)?,
    _ continuation: AsyncStream<Snapshot>.Continuation
  ) async {
    let queue = BoundedDeltaQueue<OffsetEvent>(limit: bufferLimit)
    // Establish the subscription BEFORE the snapshot request (on a first connect) — on
    // the connection's serialized send path this issues the `events` Request ahead of
    // `snapshot`. On a reconnect there is no snapshot: the daemon replays strictly after
    // `resume.offset`.
    let events = backend.events(sinceOffset: resume?.offset)
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
        group.addTask { [reconciler, observer] in
          var state: Snapshot
          var tracker: ContiguousOffsetTracker
          if let resume {
            // Incremental resume: fold onto the retained baseline — no fresh snapshot.
            state = resume.state
            tracker = ContiguousOffsetTracker(resumingAfter: resume.offset)
          } else {
            // First connect: fetch and publish the baseline (subscribe-around-snapshot).
            let base = try await backend.snapshot()
            state = base
            tracker = ContiguousOffsetTracker()
            await self.record(state: base, contiguous: nil)
            await observer?(base)
            continuation.yield(base)
          }
          while let offsetEvent = await queue.next() {
            state = reconciler.reconcile(state, applying: offsetEvent.event)
            tracker.observe(offsetEvent.offset)
            await self.record(state: state, contiguous: tracker.contiguous)
            await observer?(state)
            continuation.yield(state)
          }
        }
        do {
          try await group.next()
        } catch {
          group.cancelAll()
          throw error
        }
        group.cancelAll()
      }
    } catch {
      // Reconnect: the next attempt re-dials and resumes incrementally from the cursor.
    }
  }
}
