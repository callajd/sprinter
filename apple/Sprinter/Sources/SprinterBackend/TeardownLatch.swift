import Foundation

/// A one-way latch on a TERMINAL state — raised once, never lowered — that ``UnixSocketTransport``
/// uses to publish "this transport is fully torn down" (see ``UnixSocketTransport/awaitClosed()``).
///
/// It wraps a `DispatchSemaphore` rather than exposing one, because a raw semaphore is the wrong
/// shape for a latch and the difference is a deadlock: teardown signals exactly ONCE, so a bare
/// `wait()` CONSUMES that signal and silently disarms the latch for every later or concurrent
/// waiter, who then blocks forever — the unbounded wait #94 exists to remove. Here EVERY wait
/// re-signals on success, so the raised state is sticky and no observer can strand another.
///
/// Both waits BLOCK the calling thread, so callers must be off the cooperative executor — the
/// transport hops onto a dispatch queue for exactly that reason.
final class TeardownLatch: @unchecked Sendable {
  private let semaphore = DispatchSemaphore(value: 0)

  /// Raises the latch. Called once, by whichever teardown arm arrives second.
  func signal() {
    semaphore.signal()
  }

  /// Blocks until the latch is raised, leaving it raised.
  func wait() {
    semaphore.wait()
    semaphore.signal()
  }

  /// Blocks for at most `timeout`, leaving the latch raised if it was. Returns `.timedOut`
  /// instead of blocking indefinitely — the shape a test needs so a stuck teardown fails fast
  /// rather than hanging the run.
  func wait(within timeout: DispatchTimeInterval) -> DispatchTimeoutResult {
    let result = semaphore.wait(timeout: .now() + timeout)
    if result == .success { semaphore.signal() }
    return result
  }
}
