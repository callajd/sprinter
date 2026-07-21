import Foundation
import Testing

@testable import SprinterBackend

#if canImport(Darwin)
  import Darwin
#endif

/// Direct cover for ``ReadLoopWake`` — the mechanism #107's teardown fix rests on.
///
/// It is tested HERE, as a unit, rather than only through ``UnixSocketTransport``, because its
/// interesting paths are all LIVENESS paths: the durable-state wake, and the two degradations
/// (an unusable wake, a lost raise) that the transport's bound falls back on. #94 and #107 both
/// happened because a liveness path was never exercised — a fallback nobody runs is a fallback
/// nobody knows works.
@Suite("Read loop wake", .timeLimit(.minutes(1)))
struct ReadLoopWakeTests {
  /// A connected `socketpair(2)`; `local` is what the wake waits on, `peer` is written to.
  private static func socketPair() throws -> (local: Int32, peer: Int32) {
    var descriptors: [Int32] = [-1, -1]
    guard socketpair(AF_UNIX, SOCK_STREAM, 0, &descriptors) == 0 else {
      throw LoopbackError.setupFailed("socketpair errno \(errno)")
    }
    return (descriptors[0], descriptors[1])
  }

  /// Runs the BLOCKING ``ReadLoopWake/waitForData(socket:)`` on a dedicated thread — never on
  /// the cooperative executor — and returns its result under a bound, so a wait that never ends
  /// fails the test fast instead of hanging it (`nil`).
  private func wakeup(
    _ wake: ReadLoopWake, socket: Int32, within seconds: Int = 5
  ) async -> ReadLoopWake.Wakeup? {
    let task = Task { () -> ReadLoopWake.Wakeup in
      await withCheckedContinuation { (resume: CheckedContinuation<ReadLoopWake.Wakeup, Never>) in
        let thread = Thread { resume.resume(returning: wake.waitForData(socket: socket)) }
        thread.name = "sprinter.test.wake"
        thread.start()
      }
    }
    return await boundedValue(of: task, within: seconds)
  }

  @Test("a raise BEFORE the wait is still observed — durable state, not an edge")
  func raiseBeforeWaitIsObserved() async throws {
    // THE #107 PROPERTY. The `shutdown(2)` this replaced was an edge: one delivered before the
    // reader parked was lost forever. The byte in the pipe is still there when the wait begins.
    let (local, peer) = try Self.socketPair()
    defer {
      _ = Darwin.close(local)
      _ = Darwin.close(peer)
    }
    let wake = ReadLoopWake.make()
    #expect(wake.isUsable, "pipe(2) failed; the durable-state path cannot be tested")
    wake.raise()
    #expect(await wakeup(wake, socket: local) == .woken)
    wake.release()
  }

  @Test("a raise DURING the wait ends it")
  func raiseDuringWaitEndsIt() async throws {
    let (local, peer) = try Self.socketPair()
    defer {
      _ = Darwin.close(local)
      _ = Darwin.close(peer)
    }
    let wake = ReadLoopWake.make()
    let waiting = Task { await self.wakeup(wake, socket: local) }
    try await Task.sleep(for: .milliseconds(50))  // ≫ the microseconds to enter the poll.
    wake.raise()
    #expect(await waiting.value == .woken)
    wake.release()
  }

  @Test("the wake wins over pending socket bytes")
  func wakeTakesPrecedenceOverSocketData() async throws {
    // Why the drop of un-pumped inbound bytes on `close()` is DETERMINISTIC rather than racy:
    // with both fds readable, the wake is reported. See `UnixSocketTransport.close()`.
    let (local, peer) = try Self.socketPair()
    defer {
      _ = Darwin.close(local)
      _ = Darwin.close(peer)
    }
    var byte: UInt8 = 0x7B
    #expect(withUnsafeBytes(of: &byte) { Darwin.write(peer, $0.baseAddress, 1) } == 1)
    let wake = ReadLoopWake.make()
    wake.raise()
    #expect(await wakeup(wake, socket: local) == .woken)
    wake.release()
  }

  @Test("socket bytes end the wait as readable")
  func socketDataEndsTheWait() async throws {
    let (local, peer) = try Self.socketPair()
    defer {
      _ = Darwin.close(local)
      _ = Darwin.close(peer)
    }
    let wake = ReadLoopWake.make()
    let waiting = Task { await self.wakeup(wake, socket: local) }
    try await Task.sleep(for: .milliseconds(50))
    var byte: UInt8 = 0x7B
    #expect(withUnsafeBytes(of: &byte) { Darwin.write(peer, $0.baseAddress, 1) } == 1)
    #expect(await waiting.value == .socketReadable)
    wake.release()
  }

  @Test("an UNUSABLE wake still returns from its wait, bounded")
  func unusableWakeStillReturnsBounded() async throws {
    // The `pipe(2)`-failed shape. Nothing will ever be readable here — no data, no wake — so
    // the ONLY thing that can end this wait is the bounded poll timeout. If that bound were
    // ever `-1` (or the fallback removed), this hangs: it is the liveness path the transport's
    // teardown degrades to, asserted rather than assumed.
    let (local, peer) = try Self.socketPair()
    defer {
      _ = Darwin.close(local)
      _ = Darwin.close(peer)
    }
    let wake = ReadLoopWake.unusable()
    #expect(!wake.isUsable)
    wake.raise()  // no-op, and must not touch fd -1.
    #expect(await wakeup(wake, socket: local, within: 2) == .retry)
    wake.release()  // no-op, and must not close fd -1.
  }

  @Test("a USABLE wake's wait is bounded too, so a lost raise cannot strand it")
  func usableWakeWaitIsBounded() async throws {
    // FINDING 1 of #108's review, pinned. `raise()` has no failure channel: a `write(2)` that
    // fails for anything but `EINTR` signals nothing. If the wait were infinite the reader
    // would then park forever — the exact failure class #107 exists to remove. So the wait must
    // expire on its own with nothing readable and NO raise, which is what this asserts.
    let (local, peer) = try Self.socketPair()
    defer {
      _ = Darwin.close(local)
      _ = Darwin.close(peer)
    }
    let wake = ReadLoopWake.make()
    #expect(wake.isUsable)
    let started = DispatchTime.now()
    #expect(await wakeup(wake, socket: local, within: 10) == .retry)
    let elapsed = Double(DispatchTime.now().uptimeNanoseconds - started.uptimeNanoseconds) / 1e9
    #expect(elapsed < 5, "the usable-wake wait is not bounded (took \(elapsed)s)")
    wake.release()
  }

  @Test("raise() on a FULL pipe returns instead of blocking inside close()")
  func raiseOnAFullPipeDoesNotBlock() async throws {
    // FINDING 2 of #108's review. A plain `pipe(2)` write end is BLOCKING, so "a full pipe
    // needs no retry" would have been true only by accident: the raise would not fail, it
    // would PARK — inside `UnixSocketTransport.close()`, contradicting the invariant that no
    // teardown path parks its caller. `make()` sets `O_NONBLOCK` on the write end, so a full
    // pipe returns `EAGAIN`, which is already success (the bytes in it carry the same signal).
    //
    // Falsification: drop the `fcntl` from `make()` and this test hangs to its bound and fails.
    // Darwin's pipe buffer maxes out well below this many single-byte writes.
    let (local, peer) = try Self.socketPair()
    defer {
      _ = Darwin.close(local)
      _ = Darwin.close(peer)
    }
    let wake = ReadLoopWake.make()
    #expect(wake.isUsable)
    let raised = await runBounded(within: 10) {
      for _ in 0..<200_000 { wake.raise() }
    }
    #expect(raised == .success, "raise() blocked on a full pipe: the write end is not O_NONBLOCK")
    // And the signal is intact: a full pipe is a readable pipe.
    #expect(await wakeup(wake, socket: local) == .woken)
    wake.release()
  }

  @Test("release() is one-shot, so a recycled descriptor is never closed twice")
  func releaseIsOneShot() throws {
    // A second `close(2)` of a number the kernel has since handed to someone else closes THEIR
    // fd — a corruption far worse than a leak. `release()` runs from the teardown rendezvous
    // AND from `deinit`, so being one-shot is load-bearing, not decorative.
    let wake = ReadLoopWake.make()
    wake.release()
    let recycled = socket(AF_UNIX, SOCK_STREAM, 0)  // very likely one of the two just freed.
    #expect(recycled >= 0)
    wake.release()
    #expect(fcntl(recycled, F_GETFD) != -1, "a second release() closed a recycled descriptor")
    _ = Darwin.close(recycled)
  }
}
