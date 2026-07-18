import SprinterContract

/// The reconnecting, snapshot-consistent work-graph feed (D4 / D17 / INV-REACTIVE)
/// — the reactive spine that keeps the exposed read model current across dropped
/// connections.
///
/// It is expressed purely on the ``Backend`` port (INV-PORT): construction takes a
/// `connect` seam that yields a freshly connected ``Backend`` per attempt, so the
/// engine never names a transport or where the daemon runs. Each attempt
/// **subscribes AROUND the snapshot read** (D4): it starts the live `events` reader
/// FIRST (buffering deltas into ``BoundedDeltaQueue``), THEN fetches the `snapshot`
/// baseline, THEN folds the buffered + live deltas onto it with
/// ``SnapshotReconciler``. This is the ordering the daemon's own `events` handler
/// requires (`changes` subscribes lazily and does not replay pre-subscribe deltas,
/// so a client must subscribe before/around the snapshot). Reconcile is idempotent
/// (upsert-by-id), so a buffered delta already reflected in the snapshot is a
/// harmless re-apply — not a double-count. Every published value is a full
/// baseline-consistent ``Snapshot`` (never a bare delta), so a slow consumer can
/// coalesce to the latest without losing information.
///
/// A dropped connection, a clean end, or a bounded-buffer overflow all re-run
/// subscribe-around-snapshot: a fresh snapshot re-derives the baseline and live
/// events resume.
///
/// **Residual (deferred).** Subscribing around the snapshot closes the gross
/// snapshot-then-subscribe window, but COMPLETE gap-freeness across a long
/// disconnect needs durable **offset-based replay** (the daemon's AE5
/// `EventLogStore.tail`) so the client resumes from its last-seen offset rather
/// than re-deriving from a fresh snapshot. That offset resync is convergence-layer
/// work (the daemon defers it too) — see the workstream ledger.
///
/// The un-reconciled delta backlog is bounded by ``BoundedDeltaQueue``: if the
/// reconciler falls behind BE1.1's ack-on-receipt `events` stream, the overflow
/// forces a snapshot-resync rather than dropping deltas or growing unbounded
/// (the carried #36 F1 recovery).
public actor WorkGraphResync {
  /// Yields a freshly connected ``Backend`` for one (re)connect attempt.
  public typealias Connect = @Sendable () async throws -> any Backend
  /// Invoked for each published baseline-consistent state before it reaches the
  /// feed — the hook a view model uses to project state onto the main actor.
  public typealias StateObserver = @Sendable (Snapshot) async -> Void

  private let connect: Connect
  private let reconciler: SnapshotReconciler
  private let bufferLimit: Int
  private let retryDelay: Duration
  private let observer: StateObserver?

  private var driver: Task<Void, Never>?
  private var started = false

  /// Builds the engine over a `connect` seam (the port-only construction path).
  public init(
    connect: @escaping Connect,
    reconciler: SnapshotReconciler = SnapshotReconciler(),
    bufferLimit: Int = 256,
    retryDelay: Duration = .milliseconds(250),
    observer: StateObserver? = nil
  ) {
    self.connect = connect
    self.reconciler = reconciler
    self.bufferLimit = bufferLimit
    self.retryDelay = retryDelay
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
    retryDelay: Duration = .milliseconds(250),
    observer: StateObserver? = nil
  ) {
    self.init(
      connect: { try await connector.connect(to: endpoint) },
      reconciler: reconciler,
      bufferLimit: bufferLimit,
      retryDelay: retryDelay,
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
        await runAttempt(backend, continuation)
        await backend.close()
      } catch {
        // `connect` failed; fall through to the backoff and retry.
      }
      if Task.isCancelled { break }
      try? await Task.sleep(for: retryDelay)
    }
    continuation.finish()
  }

  /// One subscribe-around-snapshot attempt: start the `events` reader FIRST (so
  /// deltas emitted during the snapshot read are buffered), fetch the `snapshot`
  /// baseline, then fold buffered + live deltas onto it. Any failure (snapshot
  /// error, transport drop, or bounded-buffer overflow) returns so the loop
  /// reconnects.
  private func runAttempt(
    _ backend: any Backend,
    _ continuation: AsyncStream<Snapshot>.Continuation
  ) async {
    let queue = BoundedDeltaQueue<WorkGraphEvent>(limit: bufferLimit)
    // Establish the subscription BEFORE the snapshot request (subscribe-around-
    // snapshot): on the connection's serialized send path this issues the `events`
    // Request ahead of `snapshot`, so the daemon attaches the live subscription
    // before it builds the baseline.
    let events = backend.events()
    do {
      try await withThrowingTaskGroup(of: Void.self) { group in
        // Reader: buffer live deltas into the bounded queue from the moment the
        // subscription is live — including any emitted while the snapshot is read.
        group.addTask {
          do {
            for try await event in events {
              try await queue.enqueue(event)
            }
            await queue.finish()
          } catch {
            // A transport drop or a bounded-buffer overflow: unblock the reconciler
            // (so the group can tear down) and surface the failure to reconnect.
            await queue.finish()
            throw error
          }
        }
        // Folder: fetch the baseline, publish it, then fold buffered + live deltas.
        // Reconcile is idempotent, so a buffered delta already in the snapshot is a
        // harmless re-apply.
        group.addTask { [reconciler, observer] in
          let base = try await backend.snapshot()
          await observer?(base)
          continuation.yield(base)
          var state = base
          while let event = await queue.next() {
            state = reconciler.reconcile(state, applying: event)
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
      // Reconnect: the next attempt re-subscribes and re-fetches a fresh baseline.
    }
  }
}
