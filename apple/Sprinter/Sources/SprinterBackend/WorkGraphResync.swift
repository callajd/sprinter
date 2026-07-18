import SprinterContract

/// The reconnecting, snapshot-consistent work-graph feed (D4 / D17 / INV-REACTIVE)
/// — the reactive spine that keeps the exposed read model current across dropped
/// connections.
///
/// It is expressed purely on the ``Backend`` port (INV-PORT): construction takes a
/// `connect` seam that yields a freshly connected ``Backend`` per attempt, so the
/// engine never names a transport or where the daemon runs. Each attempt runs the
/// **snapshot-then-stream resync**: fetch the `snapshot` baseline FIRST, publish
/// it, THEN subscribe to live `events`, folding each delta onto the baseline with
/// ``SnapshotReconciler`` and publishing the updated state. Because every
/// published value is a full baseline-consistent ``Snapshot`` (never a bare
/// delta), a slow consumer can coalesce to the latest without losing information.
///
/// A dropped connection, a clean end, or a bounded-buffer overflow all re-run
/// snapshot-then-subscribe: a fresh snapshot re-derives the baseline and live
/// events resume, so the exposed state is always snapshot-consistent and never
/// delta-only (which could miss the pre-subscribe gap).
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
    let (stream, continuation) = AsyncStream<Snapshot>.makeStream()
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

  /// One snapshot-then-subscribe attempt. Any failure (snapshot error, transport
  /// drop, or bounded-buffer overflow) returns so the loop reconnects.
  private func runAttempt(
    _ backend: any Backend,
    _ continuation: AsyncStream<Snapshot>.Continuation
  ) async {
    do {
      let base = try await backend.snapshot()
      await publish(base, to: continuation)
      try await consume(backend, base: base, to: continuation)
    } catch {
      // Reconnect: the next attempt re-fetches a fresh baseline.
    }
  }

  /// Publishes the baseline, then folds live deltas onto it through the bounded
  /// buffer. Throws on drop/overflow so the caller reconnects.
  private func consume(
    _ backend: any Backend,
    base: Snapshot,
    to continuation: AsyncStream<Snapshot>.Continuation
  ) async throws {
    let queue = BoundedDeltaQueue<WorkGraphEvent>(limit: bufferLimit)
    try await withThrowingTaskGroup(of: Void.self) { group in
      group.addTask {
        do {
          for try await event in backend.events() {
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
      group.addTask { [reconciler, observer] in
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
  }

  private func publish(
    _ state: Snapshot,
    to continuation: AsyncStream<Snapshot>.Continuation
  ) async {
    await observer?(state)
    continuation.yield(state)
  }
}
