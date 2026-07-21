import Foundation
import Testing

@testable import SprinterBackend

#if canImport(Darwin)
  import Darwin
#endif

/// Regression cover for #94 — the intermittent teardown deadlock in
/// ``UnixSocketTransport/close()``.
///
/// Every wait in here is BOUNDED (see `BoundedWaits.swift`) and hopped off the cooperative
/// executor: a regression test for a deadlock that can itself deadlock reproduces the very
/// problem it exists to catch, so a stuck teardown must surface as a fast, named FAILURE (a
/// `#expect` on the `.timedOut` result), never as a hung test run. The suite-level
/// `.timeLimit` is the outer backstop, not the mechanism.
@Suite("Unix-domain socket transport teardown", .timeLimit(.minutes(1)))
struct UnixSocketTransportTeardownTests {
  /// Waits on a teardown latch under a bound. Fully async — it suspends a task and parks no
  /// thread, so firing dozens concurrently (the batch below) costs no dispatch workers.
  /// ``TeardownLatch`` is sticky, so waiting here can never disarm a later wait or an
  /// ``UnixSocketTransport/awaitClosed()``.
  private func awaitTeardown(
    _ latch: TeardownLatch, within seconds: Int = 5
  ) async -> DispatchTimeoutResult {
    await runBounded(within: seconds) { await latch.wait() }
  }

  @Test("teardown completes even when the transport is released the instant close() returns")
  func teardownCompletesWhenTransportReleasedImmediately() async throws {
    // THE #94 INTERLEAVING. `close()` hands the fd's release to a rendezvous between the
    // read thread's exit and the write queue's drain. The read thread is started from `init`,
    // so if the caller closes and releases the transport before that freshly-`start()`ed
    // thread has been scheduled, the read arm has to still arrive. Batching the
    // create/close/release makes that window wide (N pthreads queued behind a tight
    // N-iteration loop), which is precisely why the hang was load-dependent. Every latch must
    // still be raised.
    //
    // The test holds the DETACHED latch, not the transport, on purpose: dropping the only
    // caller-side reference is what makes the window real, and it is what makes reverting the
    // read thread's `[self]` capture to `[weak self]` fail here instead of passing.
    let batch = 32
    var latches: [TeardownLatch] = []
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
    // The waits are async (no thread each), so 32 of them alongside a parallel suite run
    // cannot approach libdispatch's worker cap — no concurrency cap is needed here.
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
    // chunk) and waiting in `poll(2)` when `close()` lands — so BOTH arms are live and must
    // still meet, and the fd release must not be gated on any wait a caller has to sit on.
    // (What ends the wait is the wake pipe, not `shutdown(2)`; that is pinned separately, and
    // deterministically, by `teardownCompletesWhenShutdownWakesNobody` below.)
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

  @Test("teardown completes PROMPTLY when close()'s shutdown wakes no one")
  func teardownCompletesWhenShutdownWakesNobody() async throws {
    // #107, DETERMINISTICALLY. The flake this pins was a ~1-in-3000 kernel-level race:
    // `close()`'s `shutdown(2)` landing in the window where the read loop is ENTERING its
    // socket wait rather than already parked in it. The wakeup went to nobody, the wait that
    // started microseconds later had no remaining wakeup source, `.readLoopExit` never
    // arrived, and every `awaitClosed()` suspended forever. Reproducing that timing is
    // hopeless in a test; reproducing its RESULT is trivial and total — suppress the
    // `shutdown(2)` entirely and keep the peer's end open, so the socket offers the read loop
    // nothing at all. Teardown must still complete, which it can only do if the wake is
    // carried by durable state (the wake pipe) rather than by that one edge.
    //
    // The bound is DELIBERATELY TIGHT (1s vs the wake wait's own 2s tick). Since #108's review
    // the wake is the fast path, not the only path — the loop's `poll(2)` timeout is finite, so
    // a teardown with NO wake at all still completes one tick late. A 5s bound would therefore
    // pass with the raise deleted, silently disarming this guard; 1s cannot. Falsification:
    // remove `wake.raise()` from `close()` (or the wake fd from the read loop's `poll`) and
    // this fails on every run, because teardown then takes a full ~2s tick.
    //
    // Two iterations, breaking on the first failure: the failure is DETERMINISTIC, so more
    // buys nothing and only spends the suite's `.timeLimit` — which, if it fired first, would
    // replace this named diagnostic with a generic timeout.
    for _ in 0..<2 {
      let (transport, peer) = try RawSocketPeer.pair(raisesShutdownOnClose: false)
      let latch = transport.teardownLatch
      // Park the read loop for real, and PROVE it: feed a chunk and consume it off
      // `receive()`, so the loop has demonstrably pumped and gone back round to its wait. A
      // close landing before that would be caught by the loop's own `isClosed` re-check —
      // which is not the state under test, and would pass with or without the fix.
      let received = Task { () -> Bool in
        var chunks = transport.receive().makeAsyncIterator()
        do { return try await chunks.next() != nil } catch { return false }
      }
      try await peer.write(Data("{\"k\":\"v\"}\n".utf8))
      #expect(await boundedValue(of: received) == true, "the read loop never pumped the chunk")
      try await Task.sleep(for: .milliseconds(50))  // ≫ the microseconds to re-enter the wait.

      transport.close()
      let torndown = await awaitTeardown(latch, within: 1)
      // The peer is closed only AFTER the wait: an early close would supply the very EOF this
      // test exists to withhold.
      peer.close()
      #expect(
        torndown == .success,
        """
        teardown did not complete within 1s with shutdown(2) suppressed: the wake is not \
        ending the read loop's wait, and only the fallback tick is (#107).
        """)
      guard torndown == .success else { break }
    }
  }

  @Test("teardown completes even when the wake pipe could not be allocated")
  func teardownCompletesWithAnUnusableWake() async throws {
    // The FALLBACK the argued bound degrades to, exercised rather than assumed. `pipe(2)`
    // failing (fd exhaustion) yields an unusable wake, so `close()`'s raise signals nothing at
    // all — exactly as a failed `raise()` would. With `shutdown(2)` also suppressed and the
    // peer still open, NOTHING is readable: the only thing that can end the read loop's wait
    // is its bounded poll timeout, followed by the loop's own `isClosed` re-check. If that
    // timeout were ever infinite this hangs — which is #107 all over again, one layer down.
    let (transport, peer) = try RawSocketPeer.pair(
      raisesShutdownOnClose: false, wake: .unusable())
    let latch = transport.teardownLatch
    let received = Task { () -> Bool in
      var chunks = transport.receive().makeAsyncIterator()
      do { return try await chunks.next() != nil } catch { return false }
    }
    try await peer.write(Data("{\"k\":\"v\"}\n".utf8))
    #expect(await boundedValue(of: received) == true, "the read loop never pumped the chunk")
    try await Task.sleep(for: .milliseconds(50))

    transport.close()
    let torndown = await awaitTeardown(latch, within: 1)
    peer.close()
    #expect(
      torndown == .success,
      "teardown never completed with an unusable wake: the fallback poll is not bounded (#107)")
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
    let concurrent = await runBounded {
      await withTaskGroup(of: Void.self) { group in
        for _ in 0..<8 { group.addTask { await transport.awaitClosed() } }
      }
    }
    #expect(concurrent == .success, "a concurrent awaitClosed() never returned (#94)")
    let repeated = await runBounded {
      await transport.awaitClosed()
      await transport.awaitClosed()
    }
    #expect(repeated == .success, "a repeated awaitClosed() never returned (#94)")
  }

  @Test("waiting on the teardown latch leaves it raised for a later awaitClosed()")
  func teardownLatchStaysRaised() async throws {
    let (transport, peer) = try RawSocketPeer.pair()
    defer { peer.close() }
    transport.close()
    #expect(await awaitTeardown(transport.teardownLatch) == .success, "no teardown (#94)")
    // The latch must RE-ARM. A bare semaphore `wait()` would consume the one signal teardown
    // ever sends and park this `awaitClosed()` forever — reintroducing exactly the unbounded
    // wait #94 is about, from the observation side.
    let after = await runBounded { await transport.awaitClosed() }
    #expect(after == .success, "a teardown probe disarmed the latch for awaitClosed() (#94)")
  }

  @Test("awaitClosed() before any close() is a no-op rather than an unbounded wait")
  func awaitClosedWithoutCloseReturns() async throws {
    let (transport, peer) = try RawSocketPeer.pair()
    defer { peer.close() }
    // Never closed: nothing to drain, so this must return rather than wait on a latch that
    // nothing will ever signal.
    let beforeClose = await runBounded { await transport.awaitClosed() }
    #expect(beforeClose == .success, "awaitClosed() blocked with no close() initiated (#94)")
    transport.close()
    let afterClose = await runBounded { await transport.awaitClosed() }
    #expect(afterClose == .success, "awaitClosed() never returned after close() (#94)")
  }

  @Test("a transport dropped after peer EOF releases its descriptor rather than leaking it")
  func droppedTransportReleasesItsDescriptor() async throws {
    // `close()` is the contract, but it is not the only way a transport ENDS: a peer EOF ends
    // the read loop on its own, and a caller that then just drops the transport never reaches
    // the rendezvous — which is the only place the socket and the wake pipe are released. Since
    // #107 that is THREE fds per dropped connection, not one, and a reconnect flap loop
    // multiplies it. `deinit` is the net (the wake releases itself; the socket is released
    // here). Falsification: delete the transport's `deinit` and this fails.
    var descriptors: [Int32] = [-1, -1]
    guard socketpair(AF_UNIX, SOCK_STREAM, 0, &descriptors) == 0 else {
      throw LoopbackError.setupFailed("socketpair errno \(errno)")
    }
    var transport: UnixSocketTransport? = UnixSocketTransport(connectedDescriptor: descriptors[0])
    // The STREAM is what the observer holds, not the transport: an inbound stream does not
    // retain its producer, so the `nil` below really is the last reference (the same reason
    // the #94 tests observe teardown through the detached latch).
    let inbound = transport?.receive()
    let finished = Task { () -> Bool in
      guard let inbound else { return false }
      var chunks = inbound.makeAsyncIterator()
      do {
        while try await chunks.next() != nil {}
        return true
      } catch { return false }
    }
    _ = Darwin.close(descriptors[1])  // peer EOF: the read loop ends WITHOUT any close().
    #expect(await boundedValue(of: finished) == true, "the read loop never saw the peer's EOF")
    // Let the read thread finish releasing its strong reference, so the drop below is the last.
    try await Task.sleep(for: .milliseconds(200))

    transport = nil  // dropped without `close()` — `deinit` must not leak the socket.
    #expect(
      fcntl(descriptors[0], F_GETFD) == -1,
      "a transport dropped after peer EOF leaked its socket descriptor")
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
