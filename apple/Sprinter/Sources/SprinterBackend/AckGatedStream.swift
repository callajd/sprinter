import Foundation
import SprinterContract

/// The **demand-gated** delivery buffer for one streaming RPC subscription (CE2.2
/// deliverable — demand-gated backpressure).
///
/// BE1.1 acked each `Chunk` on RECEIPT: the client sent the per-batch `Ack` the instant
/// the frame arrived and buffered the values unbounded, so a slow consumer could not
/// slow the daemon and the backlog grew without bound (the carried #36 F1 constraint).
/// This gate instead defers the `Ack` until the consumer has **drained the batch** —
/// the `Ack` is the "ready for more" signal, so the daemon's own chunk→ack→chunk flow
/// control now gates on downstream demand, not on arrival. Real backpressure.
///
/// The buffer is **bounded**: `push` rejects a batch that would exceed `limit`,
/// surfacing an ``Overflow`` the consumer sees as a stream failure — which
/// ``WorkGraphResync`` turns into a snapshot/offset resync rather than a silent drop or
/// unbounded growth. Under correct demand-gating the daemon holds the next chunk until
/// the ack, so at most one un-acked batch is ever buffered; the bound is the safety net
/// for a single over-large chunk or a daemon that ignores flow control.
///
/// Single producer (the connection's receive loop calls ``push(_:)``/``finish()``/
/// ``fail(_:)``), single consumer (the subscription's iterator calls ``next()``). State
/// is guarded by a lock so the producer never blocks on the consumer (the receive loop
/// must keep draining the transport), and the ack — deferred until drain — is sent off
/// the lock on the consumer's task.
final class AckGate: @unchecked Sendable {
  /// Raised by ``push(_:)`` when a batch would push the un-drained backlog past `limit`
  /// — the trigger the resync loop converts into a snapshot/offset resync.
  struct Overflow: Error, Equatable {}

  private let limit: Int
  /// Sends the per-batch `Ack` once the batch is drained (the demand signal).
  private let ack: @Sendable () async -> Void
  /// Interrupts the request when the consumer abandons the stream early.
  private let cancelHandler: @Sendable () async -> Void

  private let lock = NSLock()
  // Guarded by `lock`:
  /// Full batches received but not yet started (each element is one `Chunk`'s values).
  private var batches: [[JSONValue]] = []
  /// The batch currently being handed out value-by-value.
  private var active: [JSONValue] = []
  private var activeIndex = 0
  /// The active batch, once fully handed out, still owes its `Ack` (sent on the next
  /// pull — the moment the consumer proves it drained the batch and wants more).
  private var activeNeedsAck = false
  /// Count of values buffered but not yet delivered (bounds against `limit`).
  private var buffered = 0
  private var finished = false
  private var failure: (any Error)?
  private var cancelled = false
  private var waiter: CheckedContinuation<Void, Never>?

  init(
    limit: Int,
    ack: @escaping @Sendable () async -> Void,
    cancelHandler: @escaping @Sendable () async -> Void
  ) {
    self.limit = limit
    self.ack = ack
    self.cancelHandler = cancelHandler
  }

  // MARK: - Producer (receive loop)

  /// Offers one `Chunk`'s values. Buffers the batch, or records an ``Overflow`` failure
  /// when it would exceed `limit`. Never blocks the caller (the receive loop).
  func push(_ values: [JSONValue]) {
    lock.lock()
    guard !finished, failure == nil, !cancelled else {
      lock.unlock()
      return
    }
    if buffered + values.count > limit {
      failure = Overflow()
    } else {
      batches.append(values)
      buffered += values.count
    }
    let resumed = takeWaiter()
    lock.unlock()
    resumed?.resume()
  }

  /// Marks the subscription complete (terminal success `Exit`); a pending/next
  /// ``next()`` returns `nil` once the buffer drains.
  func finish() {
    lock.lock()
    finished = true
    let resumed = takeWaiter()
    lock.unlock()
    resumed?.resume()
  }

  /// Fails the subscription (terminal failure `Exit`, transport drop, or connection
  /// teardown); the consumer's ``next()`` throws `error`.
  func fail(_ error: any Error) {
    lock.lock()
    if failure == nil {
      failure = error
    }
    let resumed = takeWaiter()
    lock.unlock()
    resumed?.resume()
  }

  // MARK: - Consumer (subscription iterator)

  /// The outcome of one non-suspending evaluation of the buffer state — computed under
  /// the lock in the sync ``step()`` so the async ``next()`` never touches the lock
  /// directly (`NSLock` is unavailable from async contexts under Swift 6).
  private enum NextStep {
    case value(JSONValue)
    case needsAck
    case finished
    case failed(any Error)
    case cancelled
    case suspend
  }

  /// Pops the next value, sending the deferred `Ack` for a just-drained batch before
  /// loading the next one, and suspending until more arrives or the stream ends.
  func next() async throws -> JSONValue? {
    while true {
      switch step() {
      case .value(let value):
        return value
      case .needsAck:
        // The active batch is fully drained and the consumer wants more: NOW send its
        // deferred ack (off the lock), then re-evaluate.
        await ack()
      case .finished:
        return nil
      case .failed(let error):
        throw error
      case .cancelled:
        throw CancellationError()
      case .suspend:
        await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
          // Re-check under the lock: if the state changed between `step()` and here, do
          // not suspend (resume immediately and re-evaluate) — no lost wakeup.
          if !install(continuation) {
            continuation.resume()
          }
        }
      }
    }
  }

  /// One non-suspending evaluation of the buffer under the lock. Loads the next batch
  /// internally (no await needed), and returns the caller's next action.
  private func step() -> NextStep {
    lock.lock()
    defer { lock.unlock() }
    while true {
      if cancelled {
        return .cancelled
      }
      if activeIndex < active.count {
        let value = active[activeIndex]
        activeIndex += 1
        buffered -= 1
        return .value(value)
      }
      if activeNeedsAck {
        activeNeedsAck = false
        return .needsAck
      }
      if !batches.isEmpty {
        active = batches.removeFirst()
        activeIndex = 0
        activeNeedsAck = true
        continue
      }
      if let failure {
        return .failed(failure)
      }
      if finished {
        return .finished
      }
      return .suspend
    }
  }

  /// Installs the suspending waiter under the lock, or returns `false` if the state has
  /// since advanced (so ``next()`` resumes at once and re-steps rather than sleeping past
  /// a wakeup).
  private func install(_ continuation: CheckedContinuation<Void, Never>) -> Bool {
    lock.lock()
    defer { lock.unlock() }
    let stillWaiting =
      !cancelled && failure == nil && !finished && batches.isEmpty
      && activeIndex >= active.count && !activeNeedsAck
    guard stillWaiting else { return false }
    waiter = continuation
    return true
  }

  /// The waiter to resume and whether an interrupt is owed, computed under the lock by
  /// ``markCancelled()``.
  private struct CancelOutcome {
    let waiter: CheckedContinuation<Void, Never>?
    let shouldInterrupt: Bool
  }

  /// Abandons the stream from the consumer side (task cancelled or iterator dropped):
  /// unblocks a suspended ``next()`` and interrupts the request — but only if the stream
  /// had not already reached a terminal state (a clean finish/failure must not fire a
  /// spurious interrupt). Idempotent.
  func cancelFromConsumer() async {
    guard let outcome = markCancelled() else { return }
    outcome.waiter?.resume()
    if outcome.shouldInterrupt {
      await cancelHandler()
    }
  }

  /// Marks the stream cancelled under the lock; returns the waiter to resume and whether
  /// an interrupt is owed, or `nil` if it was already cancelled.
  private func markCancelled() -> CancelOutcome? {
    lock.lock()
    defer { lock.unlock() }
    guard !cancelled else { return nil }
    cancelled = true
    let shouldInterrupt = !finished && failure == nil
    let resumed = takeWaiter()
    return CancelOutcome(waiter: resumed, shouldInterrupt: shouldInterrupt)
  }

  /// Detaches and returns the suspended waiter, if any. Called under the lock.
  private func takeWaiter() -> CheckedContinuation<Void, Never>? {
    let continuation = waiter
    waiter = nil
    return continuation
  }
}

/// The consumer-facing `AsyncSequence` over an ``AckGate``: iterating it drives the
/// demand-gated ack (each fully-drained batch is acked on the next pull) and, on early
/// abandonment (task cancellation or a dropped iterator), interrupts the request.
struct AckGatedStream: AsyncSequence, Sendable {
  typealias Element = JSONValue

  let gate: AckGate

  func makeAsyncIterator() -> Iterator {
    Iterator(gate: gate)
  }

  /// A class iterator so a `deinit` can interrupt the request when the consumer breaks
  /// the loop without cancelling (the drop path); `withTaskCancellationHandler` covers
  /// the explicit-cancel path. Both funnel through the idempotent
  /// ``AckGate/cancelFromConsumer()``.
  final class Iterator: AsyncIteratorProtocol {
    private let gate: AckGate

    init(gate: AckGate) {
      self.gate = gate
    }

    func next() async throws -> JSONValue? {
      let gate = self.gate
      return try await withTaskCancellationHandler {
        try await gate.next()
      } onCancel: {
        Task { await gate.cancelFromConsumer() }
      }
    }

    deinit {
      let gate = self.gate
      Task { await gate.cancelFromConsumer() }
    }
  }
}
