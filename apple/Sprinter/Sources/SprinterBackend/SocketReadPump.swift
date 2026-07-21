import Foundation

#if canImport(Darwin)
  import Darwin
#endif

/// One turn of ``UnixSocketTransport``'s inbound read loop: a single non-blocking `recv(2)`
/// into a reused buffer, and the `yield` of whatever it produced.
///
/// Split from the transport because it needs none of that type's state — only the descriptor
/// and the stream continuation — and because it carries a policy of its own: what to do when a
/// `poll(2)` readiness does NOT survive to the read.
///
/// Reads are `MSG_DONTWAIT` (per-call, so the descriptor's blocking mode is left alone and
/// ``UnixSocketPosix/writeAll(_:_:)`` keeps its simple blocking form): a readiness that lapses
/// must not turn into an unbounded park inside `recv`, so it comes back `EAGAIN` and the loop
/// simply waits again.
struct SocketReadPump {
  /// What one turn decided.
  enum Step {
    /// Keep pumping (bytes were yielded, or the read lapsed and should be re-waited).
    case again
    /// Stop, and `finish()` the inbound stream (EOF, a close, or a read error).
    case endOfStream
    /// Stop; the stream has ALREADY been finished (with an overflow error, or by its consumer).
    case streamAlreadyFinished
  }

  /// How many consecutive fruitless turns (`poll(2)` said readable, the `recv` found nothing)
  /// are taken before pausing for ``lapsePauseMicroseconds``. Zero is expected — a single
  /// reader on a `SOCK_STREAM` socket owns the readiness `poll` reported — but "expected" is
  /// not "enforced", and the read loop runs on a REAL OS thread, so an unyielding `poll`/`recv`
  /// pair would burn a whole core. This guard is free on the live path (any yielded chunk
  /// resets it) and turns a hypothetical spin into a rate limit. The pause is orders of
  /// magnitude shorter than the wait's own timeout, so teardown latency is unaffected.
  private static let maxConsecutiveLapses = 16
  private static let lapsePauseMicroseconds: UInt32 = 1000

  /// Reused across turns so the loop allocates no per-read buffer.
  private var buffer = [UInt8](repeating: 0, count: 64 * 1024)
  private var consecutiveLapses = 0

  /// One `recv` + `yield` against `descriptor`, handing bytes to `continuation`.
  mutating func pumpOnce(
    descriptor: Int32, yieldingTo continuation: AsyncThrowingStream<Data, any Error>.Continuation
  ) -> Step {
    let count = buffer.withUnsafeMutableBytes { raw in
      UnixSocketPosix.receiveWithoutBlocking(descriptor, into: raw)
    }
    guard count > 0 else {
      if count == 0 { return .endOfStream }  // EOF: the daemon closed the connection.
      // Interrupted, or readiness did not survive to the read — wait again, under the spin
      // guard above. Any other errno (incl. the descriptor closed under us) ends the stream.
      guard errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK else { return .endOfStream }
      consecutiveLapses += 1
      if consecutiveLapses >= Self.maxConsecutiveLapses {
        consecutiveLapses = 0
        usleep(Self.lapsePauseMicroseconds)
      }
      return .again
    }
    consecutiveLapses = 0
    // Bounded inbound (the CE2.1 carried constraint): the stream keeps at most
    // `receiveBufferLimit` un-consumed chunks. A yield the bound would overflow comes back
    // `.dropped`; for a BYTE stream any drop corrupts framing, so surface it as a hard
    // overflow error (→ resync upstream) instead of silently losing bytes.
    switch continuation.yield(Data(buffer[0..<count])) {
    case .enqueued:
      return .again
    case .dropped:
      continuation.finish(throwing: UnixSocketTransportError.receiveBufferOverflow)
      return .streamAlreadyFinished
    case .terminated:
      return .streamAlreadyFinished
    @unknown default:
      return .streamAlreadyFinished
    }
  }
}
