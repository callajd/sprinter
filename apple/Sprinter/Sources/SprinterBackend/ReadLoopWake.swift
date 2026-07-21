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
/// It also carries the socket wait itself (``waitForData(socket:)``), because the two fds must
/// be waited on TOGETHER — a wake that a reader parked on the socket alone could not see would
/// be no better than the `shutdown(2)` it replaces.
struct ReadLoopWake: Sendable {
  /// How long ``waitForData(socket:)`` blocks per call when this wake is UNUSABLE (`pipe(2)`
  /// failed). Bounded rather than indefinite so the read loop still re-checks its `isClosed`
  /// flag promptly and teardown degrades to "slightly late" rather than "never".
  private static let wakelessTimeoutMilliseconds: Int32 = 100

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

  var isUsable: Bool { readEnd >= 0 && writeEnd >= 0 }

  private init(readEnd: Int32, writeEnd: Int32) {
    self.readEnd = readEnd
    self.writeEnd = writeEnd
  }

  /// Allocates the pipe. A failed `pipe(2)` yields an unusable wake rather than throwing:
  /// losing the wake degrades teardown latency (see ``wakelessTimeoutMilliseconds``), and is
  /// not worth failing a connection that is otherwise perfectly good.
  static func make() -> ReadLoopWake {
    var ends: [Int32] = [-1, -1]
    guard pipe(&ends) == 0, ends.count == 2 else {
      return ReadLoopWake(readEnd: -1, writeEnd: -1)
    }
    return ReadLoopWake(readEnd: ends[0], writeEnd: ends[1])
  }

  /// Puts one byte in the pipe, which is what a subsequent (or already-parked)
  /// ``waitForData(socket:)`` observes. Retries `EINTR`. A full pipe needs no retry: the bytes
  /// already in it carry the identical "readable" signal — the state is what matters, not the
  /// count.
  func raise() {
    guard isUsable else { return }
    var token: UInt8 = 1
    while true {
      let written = withUnsafeBytes(of: &token) { raw in write(writeEnd, raw.baseAddress, 1) }
      if written >= 0 || errno != EINTR { return }
    }
  }

  /// Blocks until `socket` has data (or an error/EOF), or the wake is raised. The wake is
  /// checked FIRST so a close in flight always wins over draining more inbound bytes.
  func waitForData(socket: Int32) -> Wakeup {
    var descriptors = [
      pollfd(fd: socket, events: Int16(POLLIN), revents: 0),
      pollfd(fd: readEnd, events: Int16(POLLIN), revents: 0)
    ]
    let count: nfds_t = isUsable ? 2 : 1
    let timeout: Int32 = isUsable ? -1 : Self.wakelessTimeoutMilliseconds
    let ready = poll(&descriptors, count, timeout)
    guard ready > 0 else {
      // `poll` itself failed for a non-retryable reason: report the socket readable so the
      // caller's `recv` surfaces the real error and exits, rather than spinning here.
      return ready < 0 && errno != EINTR ? .socketReadable : .retry
    }
    if isUsable, descriptors[1].revents != 0 { return .woken }
    return descriptors[0].revents != 0 ? .socketReadable : .retry
  }

  /// Closes both ends. The caller must have established that NEITHER is in use — for the
  /// transport that is the teardown rendezvous: the read loop has exited (so nothing polls
  /// the read end) and `close()` raised the wake strictly before enqueuing the arm that
  /// completes the rendezvous (so nothing writes the write end).
  func release() {
    guard isUsable else { return }
    UnixSocketPosix.closeDescriptor(readEnd)
    UnixSocketPosix.closeDescriptor(writeEnd)
  }
}
