import Foundation

#if canImport(Darwin)
  import Darwin
#endif

/// The read loop's wakeup channel — the mechanism that makes ``UnixSocketTransport``'s
/// teardown independent of TIMING (#107).
///
/// #94 left ``UnixSocketTransport/close()``'s `shutdown(2)` as the only thing that ended the
/// read loop's blocking `read(2)`, and a `shutdown(2)` only unblocks a reader that is ALREADY
/// parked. One landing in the microseconds while the reader is *entering* the syscall wakes
/// nobody; the read that lands just after has no remaining wakeup source and blocks forever,
/// the `.readLoopExit` teardown arm never arrives, and every `awaitClosed()` suspends forever.
/// Measured on Darwin with a `socketpair(2)` reader entering `read(2)` as `shutdown(2)`
/// returns: ~1 in 3000 such races never wakes.
///
/// The cure is to stop signalling with an EDGE and signal with DURABLE STATE instead. A
/// `pipe(2)` is exactly that: ``raise()`` leaves a byte in it, and a `poll(2)` that begins
/// *after* the byte was written still reports the read end readable. There is no window in
/// which the signal can be delivered to nobody, so no interleaving of `close()` against the
/// read loop can strand teardown.
///
/// The wake is the FAST path, not the only path: every wait here is bounded (see
/// ``wakeTimeoutMilliseconds``), so neither a failed `pipe(2)` nor a failed `raise()` can
/// turn a lost signal into a lost thread. Teardown's bound holds on the mechanism failing.
///
/// It also carries the socket wait itself (``waitForData(socket:)``), because the two fds must
/// be waited on TOGETHER — a wake that a reader parked on the socket alone could not see would
/// be no better than the `shutdown(2)` it replaces.
///
/// A reference type on purpose: it OWNS two descriptors, so RAII is the only way to make
/// "released exactly once, and never leaked" a property of the value rather than a rule the
/// holder has to follow (see ``release()``).
final class ReadLoopWake: @unchecked Sendable {
  /// How long ``waitForData(socket:)`` blocks per call when this wake is UNUSABLE (`pipe(2)`
  /// failed). Bounded rather than indefinite so the read loop still re-checks its `isClosed`
  /// flag promptly and teardown degrades to "slightly late" rather than "never".
  private static let wakelessTimeoutMilliseconds: Int32 = 100

  /// How long ``waitForData(socket:)`` blocks per call when the wake IS usable. **Finite on
  /// purpose.** An infinite (`-1`) wait would make the whole teardown bound rest on
  /// ``raise()`` having actually delivered its byte — and `raise()` has no failure channel, so
  /// a `write(2)` failing for any reason other than `EINTR` would return having signalled
  /// NOTHING while the reader stayed parked forever. That is the exact failure class #107 is
  /// about, and arguing "this `write(2)` cannot fail" is what made #107's `shutdown(2)`
  /// premise unsound in the first place. With a finite tick the loop re-checks `isClosed`
  /// unconditionally, so a lost wake costs LATENCY (one tick) and never LIVENESS. Long enough
  /// that an idle connection's read thread wakes rarely; short enough to bound teardown well
  /// inside any caller's patience.
  private static let wakeTimeoutMilliseconds: Int32 = 2000

  /// What ended the wait.
  enum Wakeup {
    /// The socket has bytes, EOF or an error pending — read it.
    case socketReadable
    /// ``raise()`` was called: the transport is closing. Leave the loop; touch nothing.
    case woken
    /// Interrupted, or the bounded wait expired. The caller re-checks its state and re-waits.
    case retry
  }

  /// `-1` on both when `pipe(2)` failed; ``isUsable`` is then `false` and every operation
  /// degrades safely rather than acting on a bogus descriptor.
  private let readEnd: Int32
  private let writeEnd: Int32
  /// Guards ``released`` so ``release()`` is one-shot across the rendezvous and `deinit`.
  private let releaseLock = NSLock()
  private var released = false

  var isUsable: Bool { readEnd >= 0 && writeEnd >= 0 }

  private init(readEnd: Int32, writeEnd: Int32) {
    self.readEnd = readEnd
    self.writeEnd = writeEnd
  }

  /// An UNUSABLE wake — the shape a failed `pipe(2)` yields. Every operation degrades safely
  /// and ``waitForData(socket:)`` falls back to its bounded poll, which is a LIVENESS path, so
  /// tests construct this directly rather than waiting for fd exhaustion to construct it.
  static func unusable() -> ReadLoopWake {
    ReadLoopWake(readEnd: -1, writeEnd: -1)
  }

  /// Allocates the pipe. A failed `pipe(2)` yields an unusable wake rather than throwing:
  /// losing the wake degrades teardown latency (see ``wakelessTimeoutMilliseconds``), and is
  /// not worth failing a connection that is otherwise perfectly good.
  ///
  /// The WRITE end is made non-blocking here, before the value exists, so ``raise()`` can
  /// never block: a plain `pipe(2)` write end is BLOCKING, so a full pipe would park the
  /// caller *inside* ``UnixSocketTransport/close()`` — contradicting that type's headline
  /// invariant that no teardown path parks its caller. A wake that could do that is therefore
  /// never constructed (an `fcntl` failure yields the unusable wake, whose `raise()` writes
  /// nothing at all), rather than being argued unreachable.
  static func make() -> ReadLoopWake {
    var ends: [Int32] = [-1, -1]
    guard pipe(&ends) == 0, ends.count == 2 else {
      return .unusable()
    }
    let flags = fcntl(ends[1], F_GETFL)
    guard flags >= 0, fcntl(ends[1], F_SETFL, flags | O_NONBLOCK) == 0 else {
      UnixSocketPosix.closeDescriptor(ends[0])
      UnixSocketPosix.closeDescriptor(ends[1])
      return .unusable()
    }
    return ReadLoopWake(readEnd: ends[0], writeEnd: ends[1])
  }

  /// Puts one byte in the pipe, which is what a subsequent (or already-parked)
  /// ``waitForData(socket:)`` observes. Retries `EINTR`; anything else returns.
  ///
  /// A FULL pipe needs no retry, and cannot block: the write end is `O_NONBLOCK` (see
  /// ``make()``), so a full pipe returns `EAGAIN` — which is already success here, because the
  /// bytes in it carry the identical "readable" signal. State is what matters, not the count.
  /// Any OTHER error returns having signalled nothing; that is survivable only because
  /// ``waitForData(socket:)``'s wait is bounded even when the wake is usable (see
  /// ``wakeTimeoutMilliseconds``), so a lost raise is late teardown, never stuck teardown.
  func raise() {
    guard isUsable else { return }
    var token: UInt8 = 1
    while true {
      let written = withUnsafeBytes(of: &token) { raw in write(writeEnd, raw.baseAddress, 1) }
      if written >= 0 || errno != EINTR { return }
    }
  }

  /// Blocks — for a BOUNDED time, always (see ``wakeTimeoutMilliseconds``) — until `socket`
  /// has data (or an error/EOF), or the wake is raised, whichever comes first; the bound
  /// expiring is reported as ``Wakeup/retry``.
  ///
  /// The wake is checked FIRST so a close in flight always wins over draining more inbound
  /// bytes. That makes the drop of any bytes that arrived but were not yet pumped
  /// DETERMINISTIC on close rather than racy — see ``UnixSocketTransport/close()``.
  func waitForData(socket: Int32) -> Wakeup {
    var descriptors = [
      pollfd(fd: socket, events: Int16(POLLIN), revents: 0),
      pollfd(fd: readEnd, events: Int16(POLLIN), revents: 0)
    ]
    let count: nfds_t = isUsable ? 2 : 1
    let timeout: Int32 = isUsable ? Self.wakeTimeoutMilliseconds : Self.wakelessTimeoutMilliseconds
    let ready = poll(&descriptors, count, timeout)
    guard ready > 0 else {
      // `poll` itself failed for a non-retryable reason: report the socket readable so the
      // caller's `recv` surfaces the real error and exits, rather than spinning here.
      return ready < 0 && errno != EINTR ? .socketReadable : .retry
    }
    if isUsable, descriptors[1].revents != 0 { return .woken }
    return descriptors[0].revents != 0 ? .socketReadable : .retry
  }

  /// Closes both ends, AT MOST ONCE however many callers ask (a second `close(2)` on a
  /// number the kernel has since recycled would shut an unrelated fd). The caller must have
  /// established that neither end is in use — for the transport that is the teardown
  /// rendezvous: the read loop has exited (so nothing polls the read end) and `close()` raised
  /// the wake strictly before enqueuing the arm that completes the rendezvous (so nothing
  /// writes the write end).
  func release() {
    guard isUsable else { return }
    let shouldClose: Bool = releaseLock.withLock {
      guard !released else { return false }
      released = true
      return true
    }
    guard shouldClose else { return }
    UnixSocketPosix.closeDescriptor(readEnd)
    UnixSocketPosix.closeDescriptor(writeEnd)
  }

  /// The safety net that makes the two fds impossible to LEAK rather than merely unlikely to:
  /// a transport whose read loop ended on peer EOF and which is then dropped without
  /// `close()` never reaches the rendezvous, so nothing would release them. `deinit` runs only
  /// once nothing references the wake — the read loop holds the transport, which holds this —
  /// so no end can be in use here.
  deinit {
    release()
  }
}
