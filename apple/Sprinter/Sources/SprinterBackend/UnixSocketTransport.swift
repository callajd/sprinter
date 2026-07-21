import Foundation

#if canImport(Darwin)
  import Darwin
#endif

/// The concrete ``RpcTransport`` that dials the daemon's **Unix-domain socket**
/// (CE2.1 / CE1.2's served wire).
///
/// This is the ONE new adapter behind the ``Backend`` port (INV-PORT): a live
/// byte duplex over an `AF_UNIX` `SOCK_STREAM` socket. It is *only* a byte pipe —
/// the `effect/unstable/rpc` **envelope** encode/decode (``Envelope``) and the
/// NDJSON line framing (``NdjsonFraming``) live one layer up in ``RpcConnection``
/// and are reused UNCHANGED. `send` writes an already-NDJSON-framed frame;
/// `receive()` yields raw inbound chunks (split arbitrarily relative to line
/// boundaries) that the connection reassembles. So a local-socket and any future
/// remote connection differ only by which ``RpcTransport`` is supplied — nothing
/// above the port learns about sockets or localness.
///
/// Concurrency: inbound bytes are pumped on a dedicated blocking read thread that
/// feeds the `receive()` stream; outbound writes are serialized on a private
/// `writeQueue` so a blocking `write(2)` never occupies a cooperative executor
/// thread; the blocking dial (`socket(2)`/`connect(2)`) is likewise hopped off the
/// cooperative executor by ``connect(toUnixSocketPath:)``'s `async` form.
///
/// SIGPIPE self-safety: every descriptor this transport writes to has `SO_NOSIGPIPE`
/// set (``setNoSigPipe(_:)``) the moment it is created — the dial path in
/// ``connect(toUnixSocketPath:)`` and the `socketpair(2)` test fixture alike — so a
/// `write(2)` racing an expected daemon drop returns `EPIPE` (``UnixSocketTransportError/writeFailed``)
/// instead of raising a process-terminating `SIGPIPE`. The transport never depends on a
/// process-wide `signal(SIGPIPE, SIG_IGN)`.
///
/// It is the fd's *lifetime* — not just the `descriptor` variable — that makes a
/// `close()` racing an in-flight `send` OR the parked read loop safe, on BOTH the write
/// and read paths. The lock alone does NOT protect the fd across a blocking
/// `write(2)`/`read(2)`: were `close()` to `close(2)` the descriptor while a queued write
/// is mid-flight OR the read loop is parked in `read(2)`, that fd number could be reused
/// by a concurrent dial (a reconnect, or a second backend) and the write would land RPC
/// bytes on — or the read loop would drain bytes off, then silently drop them from — an
/// unrelated connection. So the real `close(2)` is deferred until BOTH the write queue has
/// drained past the close (its arm is enqueued behind the last queued write on the SAME
/// `writeQueue`, and `send` re-checks `isClosed` there) and the read thread has provably
/// left `read(2)` (its arm runs as the read loop returns).
///
/// Those two arms meet in a RENDEZVOUS (``arriveAtTeardown(_:)``), not a wait: each arm
/// records its arrival under the lock and whichever arrives SECOND performs the `close(2)`
/// and signals ``closeCompleted``. No teardown path parks a thread on the other arm — this
/// is the fix for #94, where the drain arm instead *waited* on a read-loop-exit semaphore and
/// could park a dispatch worker forever if that signal never came. `close()` still
/// `shutdown(2)`s the fd first, which unblocks the parked `read(2)` (EOF) so the read arm
/// arrives promptly rather than at the peer's leisure. The lock guards the
/// `descriptor`/`isClosed` *variables*; the `writeQueue` ordering plus the rendezvous guard
/// the fd's *lifetime* (`@unchecked Sendable` is discharged by all three).
///
/// Both arms are UNCONDITIONAL, which is what makes ``closeCompleted`` — and therefore
/// ``awaitClosed()`` — impossible to strand: the drain arm is a `writeQueue.async` block
/// (dispatch always runs it) and the read arm is the read thread's `defer`, whose closure
/// captures `self` STRONGLY. A `weak` capture there was the #94 root cause: a transport
/// released before its freshly-`start()`ed read thread had loaded the reference simply never
/// ran its read loop, so the arm never arrived. ``awaitClosed()`` is the one wait left in the
/// type, and its bound is ARGUED from exactly these two unconditional arms — the argument is
/// stated on that method, where a reader of it will find it.
///
/// `close()` is LOAD-BEARING for this conformer: it owns a real OS thread (the read
/// loop, parked in `read(2)`) and a file descriptor. The read thread retains `self`, so
/// `deinit` can never fire while it runs — skipping `close()` leaks BOTH the thread and
/// the fd. The connection's teardown must call it.
public final class UnixSocketTransport: RpcTransport, @unchecked Sendable {
  private let inbound: AsyncThrowingStream<Data, any Error>
  private let continuation: AsyncThrowingStream<Data, any Error>.Continuation
  private let writeQueue = DispatchQueue(label: "sprinter.UnixSocketTransport.write")

  /// Off-executor home for the blocking dial (`socket(2)`/`connect(2)`): a full listen
  /// backlog can park `connect(2)` for a while, so it must never run on a cooperative
  /// executor thread. Concurrent so independent dials don't serialize behind each other.
  private static let dialQueue = DispatchQueue(
    label: "sprinter.UnixSocketTransport.dial", attributes: .concurrent)

  /// Guards ``descriptor`` and ``isClosed`` across the read thread, the write
  /// queue, and a caller's `close()`.
  private let lock = NSLock()
  /// The connected socket file descriptor; `-1` once closed.
  private var descriptor: Int32
  private var isClosed = false

  /// The fd whose `close(2)` is pending the teardown rendezvous; `-1` when there is none
  /// (before ``close()``, and once the fd has been released). Guarded by ``lock``.
  private var pendingCloseDescriptor: Int32 = -1
  /// The two teardown arms' arrivals (guarded by ``lock``): the read thread has left
  /// `read(2)`, and the write queue has drained past the close. The SECOND arrival releases
  /// the fd — see ``arriveAtTeardown(_:)``. Neither arm ever waits on the other.
  private var readLoopDidExit = false
  private var writeQueueDidDrain = false

  /// Raised once the fd has been released — i.e. the read loop has exited AND the write
  /// queue has drained AND `close(2)` has run. ``awaitClosed()`` waits on it so a reconnect
  /// can gate the new dial on the OLD transport being FULLY torn down (the CE2.1 carried
  /// teardown constraint). A ``TeardownLatch``, not a bare semaphore: teardown signals ONCE,
  /// and the raised state is STICKY, so repeated or concurrent observers all proceed instead
  /// of the first one consuming the signal and stranding the rest. Its wait is `async` and
  /// parks no thread.
  private let closeCompleted = TeardownLatch()
  /// `true` once ``close()`` has actually initiated teardown (guards ``awaitClosed()``
  /// from waiting on a `closeCompleted` that will never be signalled).
  private var closeInitiated = false

  /// **TEST SEAM — not part of the transport contract.** No production code reads this; the
  /// public way to observe teardown is ``awaitClosed()``. It is `internal` (invisible to the
  /// app) and exists for exactly one reason, below. Do not build on it.
  ///
  /// The teardown latch itself — an observation handle DETACHED from the transport, so a
  /// holder need not keep `self` alive to wait on it. That is what lets the #94 regression
  /// tests take the handle and then drop their only reference to the transport, which is
  /// exactly the interleaving #94 died on and the reason reverting the read thread's
  /// `[self]` capture to `[weak self]` still fails those tests fast. ``awaitClosed()`` could
  /// not stand in: calling it requires holding the transport, which closes the window.
  ///
  /// This is NOT a claim that the transport deallocates while teardown is pending: the read
  /// thread's strong capture owns it until the read loop returns. It is only that observing
  /// teardown does not ITSELF have to hold a reference. ``TeardownLatch`` is sticky, so
  /// taking the handle and waiting on it can never disarm ``awaitClosed()``.
  var teardownLatch: TeardownLatch { closeCompleted }

  /// Wraps an already-connected socket descriptor and starts pumping inbound bytes.
  /// Internal so tests can drive the framing seam over a `socketpair(2)` peer without
  /// a real `connect` — production goes through ``connect(toUnixSocketPath:)``.
  ///
  /// `receiveBufferLimit` BOUNDS the inbound stream (the CE2.1 carried constraint): the
  /// read loop pumps from here while the connection's `receive()` consumer is lazy, so an
  /// unbounded `AsyncThrowingStream` would let bytes accumulate without limit. The stream
  /// keeps at most `receiveBufferLimit` un-consumed chunks; a chunk that would overflow
  /// that bound ends the stream with ``UnixSocketTransportError/receiveBufferOverflow``
  /// (→ a resync upstream) rather than being silently dropped or growing unbounded.
  init(connectedDescriptor descriptor: Int32, receiveBufferLimit: Int = 1024) {
    self.descriptor = descriptor
    (inbound, continuation) = AsyncThrowingStream<Data, any Error>.makeStream(
      bufferingPolicy: .bufferingNewest(receiveBufferLimit))
    startReadLoop()
  }

  /// Dials the daemon listening on `path` (CE1.2's `SPRINTER_SOCKET`) and returns a
  /// connected transport. Throws a typed ``UnixSocketTransportError`` on a failed
  /// `socket(2)`/`connect(2)`, a failed `SO_NOSIGPIPE`, or an over-long path — never a
  /// force-unwrap.
  ///
  /// `internal` (NB3): the blocking dial must never run on a cooperative executor, so
  /// the public dial is the `async` overload below, which hops this onto ``dialQueue``.
  /// This synchronous form only backs that overload (and the tests reach it through it).
  static func connect(toUnixSocketPath path: String) throws -> UnixSocketTransport {
    let descriptor = socket(AF_UNIX, SOCK_STREAM, 0)
    guard descriptor >= 0 else {
      throw UnixSocketTransportError.socketCreationFailed(errno: errno)
    }
    // Disable SIGPIPE per-socket BEFORE any write can race a daemon drop: on Darwin a
    // `write(2)` to a socket whose peer closed its read end otherwise raises SIGPIPE,
    // whose default disposition TERMINATES the process. The daemon closing/restarting is
    // EXPECTED (WorkGraphResync reconnects on drops), so the broken-pipe write must
    // return -1/EPIPE (→ ``UnixSocketTransportError/writeFailed``), never a signal.
    let noSigPipe = UnixSocketPosix.setNoSigPipe(descriptor)
    guard noSigPipe == 0 else {
      UnixSocketPosix.closeDescriptor(descriptor)
      throw UnixSocketTransportError.socketOptionFailed(errno: noSigPipe)
    }
    let connectResult = UnixSocketPosix.withAddress(path: path) { addr, length in
      UnixSocketPosix.connectSocket(descriptor, addr, length)
    }
    guard let outcome = connectResult else {
      UnixSocketPosix.closeDescriptor(descriptor)
      throw UnixSocketTransportError.socketPathTooLong(
        maxBytes: UnixSocketPosix.pathCapacity - 1)
    }
    guard outcome == 0 else {
      let failure = errno
      UnixSocketPosix.closeDescriptor(descriptor)
      throw UnixSocketTransportError.connectionFailed(errno: failure)
    }
    return UnixSocketTransport(connectedDescriptor: descriptor)
  }

  /// `async` dial that runs the blocking `socket(2)`/`connect(2)` on a dedicated
  /// off-executor thread (never a cooperative one), so a full listen backlog cannot
  /// stall the shared cooperative pool. Throws the same typed
  /// ``UnixSocketTransportError`` as the synchronous form.
  ///
  /// **NOT CANCELLATION-AWARE — tracked as issue #101.** Cancelling the caller does not
  /// abort the in-flight `connect(2)`; this continuation still resumes with a LIVE transport
  /// (or the dial's error) once the kernel answers. Making it cancellable means closing the
  /// fd from the cancel handler, which races the dial itself — real design work, deliberately
  /// out of #94's scope. Both call sites are covered instead: ``WorkGraphResync`` closes the
  /// backend on every loop exit, and `AppModel`'s connect loop closes a dial that resolves
  /// after `stop()` rather than abandoning it (each pinned by a regression test). A NEW call
  /// site must do the same, or wait for #101.
  public static func connect(toUnixSocketPath path: String) async throws -> UnixSocketTransport {
    try await withCheckedThrowingContinuation { resume in
      dialQueue.async {
        do {
          resume.resume(returning: try connect(toUnixSocketPath: path))
        } catch {
          resume.resume(throwing: error)
        }
      }
    }
  }

  /// Writes one already-NDJSON-framed frame to the socket. A closed connection surfaces
  /// as ``BackendError/connectionClosed`` (the fd is gone — nothing is written); a
  /// mid-write `write(2)` failure surfaces as ``UnixSocketTransportError/writeFailed``.
  public func send(_ bytes: Data) async throws {
    try await withCheckedThrowingContinuation { (resume: CheckedContinuation<Void, any Error>) in
      writeQueue.async {
        // Read the descriptor ON the write queue — the same serial queue onto which
        // `close()` defers the real `close(2)`. So between this check and `write(2)`
        // the fd cannot be released (and its number reused by a concurrent dial): a
        // closed connection is skipped here, never written to a freed/reused fd.
        let descriptor: Int32 = self.lock.withLock { self.isClosed ? -1 : self.descriptor }
        guard descriptor >= 0 else {
          resume.resume(throwing: BackendError.connectionClosed)
          return
        }
        do {
          try UnixSocketPosix.writeAll(descriptor, bytes)
          resume.resume()
        } catch {
          resume.resume(throwing: error)
        }
      }
    }
  }

  public func receive() -> AsyncThrowingStream<Data, any Error> {
    inbound
  }

  /// Suspends until an initiated ``close()`` has fully drained (read loop exited, write
  /// queue flushed, fd released). A no-op if ``close()`` was never initiated — there is
  /// nothing to drain, and ``closeCompleted`` would never be raised.
  ///
  /// **The bound is ARGUED, not mechanical.** ``TeardownLatch/wait()`` carries no timeout,
  /// so this suspension ends only when ``arriveAtTeardown(_:)`` raises the latch. What makes
  /// that certain is that BOTH arms are unconditional once `closeInitiated` is `true`: the
  /// drain arm is a `writeQueue.async` block enqueued by ``close()`` on the same statement
  /// that sets the flag (dispatch always runs an enqueued block), and the read arm is the
  /// read thread's `defer`, whose closure captures `self` STRONGLY so the loop always runs
  /// and always returns — `close()` `shutdown(2)`s the fd first, which returns EOF from a
  /// parked `read(2)` rather than waiting on the peer. Neither arm waits on the other
  /// (``arriveAtTeardown(_:)`` is a rendezvous), so neither can be starved by the other. #94
  /// permits an argued bound; this is it, and the ledger says "argued", not "mechanical".
  ///
  /// **PRECONDITION: sequence `close()` BEFORE `awaitClosed()`.** The `closeInitiated` read is
  /// a plain guard, not a rendezvous with `close()`: a caller that invokes this CONCURRENTLY
  /// with another thread's `close()` can observe `false` and return while teardown is in
  /// flight, reporting "fully torn down" when it is not. Every caller today sequences the two
  /// (``RpcConnection/close()`` and the tests), which is what makes the guard sound; it is a
  /// precondition of the API, not a property of it. Repeated and concurrent calls AFTER
  /// `close()` are fine — that is what makes ``closeCompleted`` a latch.
  public func awaitClosed() async {
    let initiated = lock.withLock { closeInitiated }
    guard initiated else { return }
    // Suspends the caller; parks no thread. ``TeardownLatch`` is sticky, so a second, later
    // or concurrent observer returns rather than blocking on a signal already consumed.
    await closeCompleted.wait()
  }

  public func close() {
    let descriptor: Int32 = lock.withLock {
      guard !isClosed else { return -1 }
      let current = self.descriptor
      // NEITHER flag is flipped except on the branch that actually arms the rendezvous below.
      // With no descriptor to release there is nothing to signal `closeCompleted`, so
      // claiming teardown was initiated would send ``awaitClosed()`` past its guard and onto
      // a latch nothing will ever raise — and flipping `isClosed` would mark the transport
      // closed while NOTHING is armed, the inverse of the invariant. Both stay false, and the
      // call is the idempotent no-op below.
      guard current >= 0 else { return -1 }
      isClosed = true
      closeInitiated = true
      self.descriptor = -1
      // Hand the fd to the rendezvous: from here it is released by whichever teardown arm
      // arrives second, never by this caller.
      pendingCloseDescriptor = current
      return current
    }
    guard descriptor >= 0 else { return }  // already closed — idempotent no-op.

    // `shutdown` immediately unblocks a read parked in the read loop (it returns EOF), so
    // the read arm arrives promptly instead of at the peer's leisure. The real `close(2)`
    // is deferred to the teardown rendezvous, whose write-queue arm is enqueued HERE — so
    // it runs strictly after any write already queued on this fd, and `send` re-checks
    // `isClosed` on the same serial queue, so a write enqueued after this point is skipped
    // rather than landing on the closed fd. The block captures `self` strongly on purpose:
    // dispatch will always run it, so the arm can never go missing (#94).
    UnixSocketPosix.shutdownDescriptor(descriptor)
    writeQueue.async {
      self.arriveAtTeardown(.writeQueueDrain)
    }
    continuation.finish()
  }

  // MARK: - Teardown rendezvous

  /// The two parties whose departure from the fd `close(2)` must wait for.
  private enum TeardownArm {
    /// The read thread has returned from ``readLoop()`` — it will never `read(2)` again.
    case readLoopExit
    /// The write queue has drained past ``close()`` — no `write(2)` is in flight, and any
    /// later `send` is skipped by the `isClosed` re-check on that same serial queue.
    case writeQueueDrain
  }

  /// Records one arm's arrival and, if it is the SECOND, releases the fd and raises
  /// ``closeCompleted``.
  ///
  /// This is a rendezvous rather than a wait, which is the whole point: an arm that arrives
  /// first RETURNS instead of parking on the other, so no teardown path can block — not the
  /// caller of ``close()``, not the write queue's dispatch worker, not the read thread. The
  /// fd is `close(2)`d exactly once (the `pendingCloseDescriptor` handoff is a
  /// compare-and-take under ``lock``), and only after both arms have provably stopped
  /// touching it, so its number can never be reused while a write or read might still land
  /// on it. An arm that arrives before ``close()`` was ever called (an EOF-terminated read
  /// loop, say) simply records itself; `close()` supplying the fd later completes the pair.
  private func arriveAtTeardown(_ arm: TeardownArm) {
    let releasable: Int32 = lock.withLock {
      switch arm {
      case .readLoopExit: readLoopDidExit = true
      case .writeQueueDrain: writeQueueDidDrain = true
      }
      guard readLoopDidExit, writeQueueDidDrain, pendingCloseDescriptor >= 0 else { return -1 }
      let descriptor = pendingCloseDescriptor
      pendingCloseDescriptor = -1
      return descriptor
    }
    guard releasable >= 0 else { return }
    UnixSocketPosix.closeDescriptor(releasable)
    // The fd is now released and neither a queued write nor the read thread can touch it:
    // a reconnect awaiting `awaitClosed()` may safely dial the new socket.
    closeCompleted.signal()
  }

  // MARK: - Inbound pump

  private func startReadLoop() {
    // STRONG capture, deliberately (#94). This thread is the ONLY producer of the
    // `.readLoopExit` arm, and `Thread.start()` is asynchronous: under load the thread can
    // be scheduled well after the caller has closed and released the transport. A `weak`
    // capture therefore left a window in which the reference was already nil, the read loop
    // never ran at all, and the arm never arrived — stranding teardown forever. Owning
    // `self` for the thread's lifetime makes that window unconstructible; the loop always
    // runs, and always arrives. It also matches the type's documented contract that `deinit`
    // cannot fire while the read thread lives.
    let thread = Thread { [self] in
      readLoop()
    }
    thread.name = "sprinter.UnixSocketTransport.read"
    thread.start()
  }

  private func readLoop() {
    // Arrive the instant the read thread stops touching the fd (however it exits), so the
    // deferred `close(2)` can only run once we are provably out of `read(2)`. The fd number
    // is thus never released while this thread might still `read(2)` from it.
    defer { arriveAtTeardown(.readLoopExit) }
    let bufferSize = 64 * 1024
    var buffer = [UInt8](repeating: 0, count: bufferSize)
    while true {
      lock.lock()
      let descriptor = self.descriptor
      let closed = isClosed
      lock.unlock()
      guard !closed, descriptor >= 0 else { break }

      let count = buffer.withUnsafeMutableBytes { raw in
        read(descriptor, raw.baseAddress, bufferSize)
      }
      if count > 0 {
        // Bounded inbound (the CE2.1 carried constraint): the stream keeps at most
        // `receiveBufferLimit` un-consumed chunks. A yield the bound would overflow comes
        // back `.dropped`; for a BYTE stream any drop corrupts framing, so surface it as
        // a hard overflow error (→ resync upstream) instead of silently losing bytes.
        switch continuation.yield(Data(buffer[0..<count])) {
        case .enqueued:
          continue
        case .dropped:
          continuation.finish(throwing: UnixSocketTransportError.receiveBufferOverflow)
          return
        case .terminated:
          return
        @unknown default:
          return
        }
      } else if count == 0 {
        break  // EOF: the daemon closed the connection.
      } else if errno == EINTR {
        continue  // interrupted syscall — retry.
      } else {
        break  // read error (incl. the descriptor closed under us) — end the stream.
      }
    }
    continuation.finish()
  }
}
