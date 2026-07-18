/// A bounded hand-off buffer between the live `events` feed and the reconciler
/// (BE1.2's recovery for the carried #36 F1 wiring constraint).
///
/// BE1.1's `events` stream acks each batch on RECEIPT (a handshake, not
/// demand-gated backpressure) and buffers unbounded, so a reconciler that falls
/// behind a delta firehose would let that buffer grow without bound. This queue
/// bounds the un-reconciled backlog: while the reconciler keeps up, a delta is
/// handed straight across; once it falls `limit` deltas behind, the next
/// ``enqueue(_:)`` throws ``Overflow`` — the signal the resync loop turns into a
/// snapshot-resync. A fresh snapshot supersedes every buffered delta, so the
/// overflow path drops NOTHING (unlike a keep-newest/keep-oldest policy) and
/// never grows unbounded.
///
/// Single producer (the feed task), single consumer (the reconcile loop); all
/// state is actor-isolated.
actor BoundedDeltaQueue<Element: Sendable> {
  /// Thrown by ``enqueue(_:)`` when the reconciler has fallen `limit` deltas
  /// behind — the trigger for a snapshot-resync.
  struct Overflow: Error, Equatable {}

  private let limit: Int
  private var items: [Element] = []
  private var finished = false
  private var waiter: CheckedContinuation<Element?, Never>?

  init(limit: Int) {
    self.limit = limit
  }

  /// Offers one delta. Hands it straight to a waiting consumer; otherwise buffers
  /// it, throwing ``Overflow`` once the backlog would exceed `limit`.
  func enqueue(_ element: Element) throws {
    if let waiter {
      self.waiter = nil
      waiter.resume(returning: element)
      return
    }
    guard items.count < limit else { throw Overflow() }
    items.append(element)
  }

  /// Marks the feed complete; a waiting/next ``next()`` then returns `nil`.
  func finish() {
    finished = true
    waiter?.resume(returning: nil)
    waiter = nil
  }

  /// Pops the next delta, suspending until one arrives or the feed finishes
  /// (`nil`). One outstanding consumer at a time.
  func next() async -> Element? {
    if !items.isEmpty {
      return items.removeFirst()
    }
    if finished {
      return nil
    }
    return await withCheckedContinuation { continuation in
      waiter = continuation
    }
  }
}
