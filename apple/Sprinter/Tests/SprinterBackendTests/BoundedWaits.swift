import Foundation

/// Bounded waiting primitives shared by the #94 regression suites.
///
/// A regression test for a deadlock must never be able to deadlock itself, so every wait in
/// those suites goes through here: a stuck path surfaces as a fast `.timedOut` — turned into a
/// NAMED `#expect` failure by the caller — instead of stalling the run until a suite-level
/// `.timeLimit` fires. The `.timeLimit`s remain the outer backstop; these are the mechanism.

/// Waits on `latch` for at most `seconds`, OFF the cooperative executor (a
/// `DispatchSemaphore.wait` must never park a cooperative thread).
func awaitSignal(
  _ latch: DispatchSemaphore, within seconds: Int = 5
) async -> DispatchTimeoutResult {
  typealias Waiter = CheckedContinuation<DispatchTimeoutResult, Never>
  return await withCheckedContinuation { (waiter: Waiter) in
    DispatchQueue.global().async {
      waiter.resume(returning: latch.wait(timeout: .now() + .seconds(seconds)))
    }
  }
}

/// Runs `operation` under a hard bound. The work is DETACHED rather than a structured child:
/// a child task would be awaited at scope exit, so an `operation` that never returns would
/// hang the test despite the bound — the exact self-defeating shape these suites must avoid.
func runBounded(
  within seconds: Int = 5, _ operation: @escaping @Sendable () async -> Void
) async -> DispatchTimeoutResult {
  let done = DispatchSemaphore(value: 0)
  Task.detached {
    await operation()
    done.signal()
  }
  return await awaitSignal(done, within: seconds)
}

/// The bounded form of `await task.value`: the value, or `nil` if the task did not finish
/// within the bound (so the caller fails fast instead of awaiting it forever).
func boundedValue<Value: Sendable>(
  of task: Task<Value, Never>, within seconds: Int = 5
) async -> Value? {
  let box = ValueBox<Value>()
  let outcome = await runBounded(within: seconds) { await box.set(task.value) }
  guard outcome == .success else { return nil }
  return await box.value
}

/// A one-shot mailbox for ``boundedValue(of:within:)``'s detached hand-back.
private actor ValueBox<Value: Sendable> {
  var value: Value?
  func set(_ value: Value) { self.value = value }
}
