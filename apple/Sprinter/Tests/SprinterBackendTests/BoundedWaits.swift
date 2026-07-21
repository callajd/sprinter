import Foundation

/// Bounded waiting primitives shared by the #94 regression suites.
///
/// A regression test for a deadlock must never be able to deadlock itself, so every wait in
/// those suites goes through here: a stuck path surfaces as a fast `.timedOut` — turned into a
/// NAMED `#expect` failure by the caller — instead of stalling the run until a suite-level
/// `.timeLimit` fires. The `.timeLimit`s remain the outer backstop; these are the mechanism.
///
/// Every wait here is fully ASYNC: it suspends a task and parks no thread. An earlier form
/// raced a `DispatchSemaphore.wait(timeout:)` on `DispatchQueue.global()`, which occupies a
/// libdispatch worker for the whole bound — and swift-testing runs suites in parallel, so a
/// test that fires dozens of those at once (the 32-transport teardown batch) pushes toward
/// libdispatch's 64-thread cap. It self-resolved at the bound rather than hanging, but it is
/// the same block-a-worker antipattern this PR removes from the transport, inside the gate
/// this PR exists to make reliable. No helper here, and no wait in the suites that use them,
/// parks a worker any more — so no concurrency cap is needed. (`RawSocketPeer`'s blocking
/// `read`/`write` on its own private queue is separate: that is real I/O with a peer the test
/// drives, not a wait on a signal that may never come.)

/// Runs `operation` under a hard bound. The work is DETACHED rather than a structured child:
/// a child task would be awaited at scope exit, so an `operation` that never returns would
/// hang the test despite the bound — the exact self-defeating shape these suites must avoid.
/// On `.timedOut` the detached task is simply left suspended (it costs no thread), which is
/// the point: the test reports the stall instead of joining it.
func runBounded(
  within seconds: Int = 5, _ operation: @escaping @Sendable () async -> Void
) async -> DispatchTimeoutResult {
  // An `AsyncStream` rather than `await task.value`: awaiting a `Task<Void, Never>` is NOT
  // cancellable, so the losing racer could not be reaped and the group would never drain.
  // A stream's `next()` returns `nil` on cancellation, so both racers are reapable.
  let (completion, signal) = AsyncStream<Void>.makeStream()
  Task.detached {
    await operation()
    signal.yield(())
    signal.finish()
  }
  return await withTaskGroup(of: DispatchTimeoutResult.self) { group in
    group.addTask {
      var completions = completion.makeAsyncIterator()
      return await completions.next() == nil ? .timedOut : .success
    }
    group.addTask {
      try? await Task.sleep(for: .seconds(seconds))
      return .timedOut
    }
    let first = await group.next() ?? .timedOut
    group.cancelAll()
    return first
  }
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
