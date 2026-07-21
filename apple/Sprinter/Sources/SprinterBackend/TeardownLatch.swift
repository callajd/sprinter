import Foundation

/// A one-way latch on a TERMINAL state — raised once, never lowered — that ``UnixSocketTransport``
/// uses to publish "this transport is fully torn down" (see ``UnixSocketTransport/awaitClosed()``).
///
/// Two properties matter, and neither is what a bare `DispatchSemaphore` gives you:
///
/// - **Sticky.** Teardown signals exactly ONCE. A semaphore `wait()` CONSUMES that signal and
///   silently disarms the latch for every later or concurrent waiter, who then blocks forever —
///   the unbounded wait #94 exists to remove. Here ``signal()`` records a raised state, so an
///   observer that arrives afterwards returns immediately, however many came before it.
/// - **Non-blocking.** ``wait()`` is `async` and SUSPENDS its caller; it never parks a thread.
///   A semaphore-backed latch could only be awaited by blocking a real thread, which is the
///   antipattern this PR removes from the transport — and which, in a parallel test suite,
///   pushes toward libdispatch's worker cap. ``signal()`` stays synchronous and lock-only, so
///   the read thread and the write queue can raise it from their non-`async` contexts.
final class TeardownLatch: @unchecked Sendable {
  private let lock = NSLock()
  private var raised = false
  private var waiters: [CheckedContinuation<Void, Never>] = []

  /// Raises the latch and releases every suspended observer. Called once, by whichever
  /// teardown arm arrives second; safe to call from a non-`async`, non-cooperative context
  /// (it takes a lock and resumes continuations — it never blocks on anything).
  func signal() {
    let released: [CheckedContinuation<Void, Never>] = lock.withLock {
      guard !raised else { return [] }
      raised = true
      let pending = waiters
      waiters.removeAll()
      return pending
    }
    for waiter in released { waiter.resume() }
  }

  /// Suspends until the latch is raised, and returns immediately if it already is. Not
  /// cancellation-aware on purpose: this is a TERMINAL state that a teardown arm always
  /// reaches, and callers that need a bound (the regression tests) impose it from outside.
  func wait() async {
    await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
      let alreadyRaised: Bool = lock.withLock {
        guard raised else {
          waiters.append(continuation)
          return false
        }
        return true
      }
      if alreadyRaised { continuation.resume() }
    }
  }
}
