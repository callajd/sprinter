import Foundation
import Testing

@testable import SprinterBackend

#if canImport(Darwin)
  import Darwin
#endif

/// Regression cover for #94 — the intermittent teardown deadlock in
/// ``UnixSocketTransport/close()``.
///
/// Every wait in here is BOUNDED and hopped off the cooperative executor: a regression test
/// for a deadlock that can itself deadlock reproduces the very problem it exists to catch,
/// so a stuck teardown must surface as a fast, named FAILURE (`Issue.record` on
/// `.timedOut`), never as a hung test run. The suite-level `.timeLimit` is the outer
/// backstop, not the mechanism.
@Suite("Unix-domain socket transport teardown", .timeLimit(.minutes(1)))
struct UnixSocketTransportTeardownTests {
  /// Waits on a teardown latch for at most `seconds`, off the cooperative executor (a
  /// `DispatchSemaphore.wait` must never park a cooperative thread). Returns `.timedOut`
  /// rather than hanging, so the caller can FAIL instead of wedging the run.
  private func awaitTeardown(
    _ latch: DispatchSemaphore, within seconds: Int = 5
  ) async -> DispatchTimeoutResult {
    typealias Waiter = CheckedContinuation<DispatchTimeoutResult, Never>
    return await withCheckedContinuation { (waiter: Waiter) in
      DispatchQueue.global().async {
        waiter.resume(returning: latch.wait(timeout: .now() + .seconds(seconds)))
      }
    }
  }

  /// Runs `operation` with a bound. The work is DETACHED rather than a structured child: a
  /// child task would be awaited at scope exit, so an `operation` that never returns would
  /// hang the test despite the bound — the exact self-defeating shape this suite must avoid.
  private func awaitBounded(
    within seconds: Int = 5, _ operation: @escaping @Sendable () async -> Void
  ) async -> DispatchTimeoutResult {
    let done = DispatchSemaphore(value: 0)
    Task.detached {
      await operation()
      done.signal()
    }
    return await awaitTeardown(done, within: seconds)
  }

  @Test("teardown completes even when the transport is released the instant close() returns")
  func teardownCompletesWhenTransportReleasedImmediately() async throws {
    // THE #94 INTERLEAVING. `close()` hands the fd's release to a rendezvous between the
    // read thread's exit and the write queue's drain. The read thread is started from `init`
    // and the transport's ONLY strong reference is the caller's — so if the caller closes and
    // releases the transport before the freshly-`start()`ed read thread has been scheduled,
    // the read arm has to still arrive. Batching the create/close/release makes that window
    // wide (N pthreads queued behind a tight N-iteration loop), which is precisely why the
    // hang was load-dependent. Every latch must still be signalled.
    let batch = 32
    var latches: [DispatchSemaphore] = []
    var peers: [RawSocketPeer] = []
    for _ in 0..<batch {
      let (transport, peer) = try RawSocketPeer.pair()
      transport.close()
      latches.append(transport.teardownLatch)
      peers.append(peer)
      // `transport` is released HERE — the read thread may not have run a single instruction.
    }
    defer {
      for peer in peers { peer.close() }
    }

    // Awaited concurrently: a stuck teardown then costs one bound in total, not one per
    // transport, so the failure is reported within seconds even when every one is stuck.
    let stuck = await withTaskGroup(of: Int.self) { group in
      for latch in latches {
        group.addTask { await self.awaitTeardown(latch) == .timedOut ? 1 : 0 }
      }
      return await group.reduce(0, +)
    }
    #expect(
      stuck == 0,
      """
      \(stuck)/\(batch) transports never completed teardown: close() is waiting on a signal \
      that the read thread will never send (#94).
      """)
  }

  @Test("teardown completes when close() races a still-active read loop")
  func teardownCompletesWhileReadLoopIsActive() async throws {
    // The other half of the rendezvous: the read thread is provably RUNNING (it has pumped a
    // chunk) and parked in `read(2)` when `close()` lands. `shutdown(2)` must unblock it, and
    // the fd release must not be gated on any wait a caller has to sit on.
    for _ in 0..<16 {
      let (transport, peer) = try RawSocketPeer.pair()
      let latch = transport.teardownLatch
      try await peer.write(Data("{\"k\":\"v\"}\n".utf8))
      transport.close()
      transport.close()  // idempotent — a second close must not consume or re-arm the latch.
      #expect(await awaitTeardown(latch) == .success, "teardown did not complete (#94)")
      peer.close()
    }
  }

  @Test("awaitClosed() returns for repeated and concurrent callers")
  func awaitClosedIsRepeatableAndConcurrent() async throws {
    // `closeCompleted` is a LATCH, not a one-shot ticket: a second `awaitClosed()` (a retry, a
    // second teardown caller, an `RpcBackend.close()` called twice) must not consume the
    // signal and leave the next caller waiting forever. Every teardown path is bounded, not
    // just the one that first surfaced.
    let (transport, peer) = try RawSocketPeer.pair()
    defer { peer.close() }
    transport.close()
    let concurrent = await awaitBounded {
      await withTaskGroup(of: Void.self) { group in
        for _ in 0..<8 { group.addTask { await transport.awaitClosed() } }
      }
    }
    #expect(concurrent == .success, "a concurrent awaitClosed() never returned (#94)")
    let repeated = await awaitBounded {
      await transport.awaitClosed()
      await transport.awaitClosed()
    }
    #expect(repeated == .success, "a repeated awaitClosed() never returned (#94)")
  }

  @Test("awaitClosed() before any close() is a no-op rather than an unbounded wait")
  func awaitClosedWithoutCloseReturns() async throws {
    let (transport, peer) = try RawSocketPeer.pair()
    defer { peer.close() }
    // Never closed: nothing to drain, so this must return rather than wait on a latch that
    // nothing will ever signal.
    let beforeClose = await awaitBounded { await transport.awaitClosed() }
    #expect(beforeClose == .success, "awaitClosed() blocked with no close() initiated (#94)")
    transport.close()
    let afterClose = await awaitBounded { await transport.awaitClosed() }
    #expect(afterClose == .success, "awaitClosed() never returned after close() (#94)")
  }

  @Test("a transport closed before its first read completes teardown and rejects sends")
  func closeBeforeAnyIoCompletesTeardown() async throws {
    let (transport, peer) = try RawSocketPeer.pair()
    defer { peer.close() }
    let latch = transport.teardownLatch
    transport.close()  // close-before-any-I/O: nothing was ever written or read.
    #expect(await awaitTeardown(latch) == .success, "teardown did not complete (#94)")
    await #expect(throws: BackendError.connectionClosed) {
      try await transport.send(Data("{}\n".utf8))
    }
  }
}
