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
/// SIGPIPE self-safety: every descriptor this transport writes to has `SO_NOSIGPIPE` set
/// (``setNoSigPipe(_:)``) the moment it is created — the dial path and the `socketpair(2)` test
/// fixture alike — so a `write(2)` racing an expected daemon drop returns `EPIPE`
/// (``UnixSocketTransportError/writeFailed``) instead of raising a process-terminating
/// `SIGPIPE`. It never depends on a process-wide `signal(SIGPIPE, SIG_IGN)`.
///
/// It is the fd's *lifetime* — not just the `descriptor` variable — that makes a `close()`
/// racing an in-flight `send` OR the parked read loop safe. The lock alone does NOT protect
/// the fd across a blocking `write(2)`/`read(2)`: `close(2)`ing it mid-flight would free a
/// number a concurrent dial can reuse, so the write would land RPC bytes on — or the read loop
/// would drain bytes off — an unrelated connection. So the real `close(2)` is deferred until
/// BOTH the write queue has drained past the close (its arm is enqueued behind the last queued
/// write on the SAME `writeQueue`, and `send` re-checks `isClosed` there) and the read thread
/// has provably left `read(2)` (its arm runs as the read loop returns).
///
/// Those two arms meet in a RENDEZVOUS (``arriveAtTeardown(_:)``), not a wait: each arm
/// records its arrival under the lock and whichever arrives SECOND performs the `close(2)`
/// and signals ``closeCompleted``. No teardown path parks a thread on the other arm — this
/// is the fix for #94, where the drain arm instead *waited* on a read-loop-exit semaphore and
/// could park a dispatch worker forever if that signal never came. The lock guards the
/// `descriptor`/`isClosed` *variables*; the `writeQueue` ordering plus the rendezvous guard
/// the fd's *lifetime* (`@unchecked Sendable` is discharged by all three).
///
/// **The read arm is woken by DURABLE STATE, not by an edge (#107).** #94 left that arm's
/// liveness resting on `close()`'s `shutdown(2)`, which only unblocks a reader ALREADY parked;
/// one landing in the read loop's syscall-entry window woke nobody and stranded teardown
/// forever (~1 in 3000 such races). The read loop now waits on the socket and on a private
/// wake pipe together, and `close()` raises that wake — see ``ReadLoopWake``, which carries
/// the full argument. `close()` still `shutdown(2)`s the fd so the peer sees EOF promptly; it
/// is simply no longer load-bearing for teardown.
///
/// Both arms are UNCONDITIONAL (a `writeQueue.async` block dispatch always runs; the read
/// thread's `defer`, whose closure captures `self` STRONGLY — a `weak` capture there was the
/// #94 root cause, see ``startReadLoop()``), which is what keeps ``closeCompleted`` from being
/// stranded. ``awaitClosed()`` is the one wait left in the type; its bound is ARGUED from
/// those two arms, and the argument — with its premises — is stated on that method.
///
/// `close()` is LOAD-BEARING for this conformer: it owns a real OS thread (the read loop,
/// parked in `read(2)`) and a file descriptor. The read thread retains `self`, so `deinit` can
/// never fire while it runs — skipping `close()` leaks BOTH. Teardown must call it.
public final class UnixSocketTransport: RpcTransport, @unchecked Sendable {
  private let inbound: AsyncThrowingStream<Data, any Error>
  private let continuation: AsyncThrowingStream<Data, any Error>.Continuation
  private let writeQueue = DispatchQueue(label: "sprinter.UnixSocketTransport.write")

  /// Guards ``descriptor`` and ``isClosed`` across the read thread, the write queue, and a
  /// caller's `close()`.
  private let lock = NSLock()
  /// The connected socket fd (`-1` once closed), and whether ``close()`` has taken it.
  private var descriptor: Int32
  private var isClosed = false
  /// The fd whose `close(2)` is pending the teardown rendezvous; `-1` when there is none —
  /// before ``close()``, and once released. Guarded by ``lock``.
  private var pendingCloseDescriptor: Int32 = -1
  /// The two teardown arms' arrivals (guarded by ``lock``): the read thread has left
  /// `read(2)`, and the write queue has drained past the close. The SECOND arrival releases
  /// the fd — see ``arriveAtTeardown(_:)``. Neither arm ever waits on the other.
  private var readLoopDidExit = false
  private var writeQueueDidDrain = false

  /// The read loop's EDGE-FREE wakeup channel (#107) — see ``ReadLoopWake``. ``close()``
  /// raises it strictly before enqueuing the drain arm, and only the read thread waits on it,
  /// so the rendezvous releases it alongside the socket once both arms have provably stopped
  /// touching either.
  private let wake = ReadLoopWake.make()

  /// TEST SEAM — see ``init(connectedDescriptor:receiveBufferLimit:raisesShutdownOnClose:)``.
  private let raisesShutdownOnClose: Bool

  /// Raised once the fd has been released — i.e. the read loop has exited AND the write
  /// queue has drained AND `close(2)` has run. ``awaitClosed()`` waits on it so a reconnect
  /// can gate the new dial on the OLD transport being FULLY torn down (the CE2.1 carried
  /// teardown constraint). A ``TeardownLatch``, not a bare semaphore: teardown signals ONCE and
  /// the raised state is STICKY, so repeated or concurrent observers all proceed instead of the
  /// first consuming the signal and stranding the rest. Its wait is `async`, parking no thread.
  private let closeCompleted = TeardownLatch()
  /// `true` once ``close()`` has actually initiated teardown (guards ``awaitClosed()`` from
  /// waiting on a `closeCompleted` that will never be signalled).
  private var closeInitiated = false

  /// **TEST SEAM — not part of the transport contract.** No production code reads this; the
  /// public way to observe teardown is ``awaitClosed()``. It is `internal` (invisible to the
  /// app) and exists for exactly one reason. Do not build on it. It is the teardown latch
  /// itself — an observation handle DETACHED from the transport, so a holder need not keep
  /// `self` alive to wait on it. That is what lets the #94 regression tests take the handle
  /// and then drop their only reference to the transport — exactly the interleaving #94 died
  /// on, and the reason reverting `[self]` to `[weak self]` still fails those tests fast.
  /// ``awaitClosed()`` cannot stand in: calling it requires holding the transport, which
  /// closes the window. This is NOT a claim that the transport deallocates while teardown is
  /// pending (the read thread's strong capture owns it until the loop returns) — only that
  /// observing teardown need not itself hold a reference. The latch is sticky, so taking the
  /// handle can never disarm ``awaitClosed()``.
  var teardownLatch: TeardownLatch { closeCompleted }

  /// Wraps an already-connected socket descriptor and starts pumping inbound bytes. Internal
  /// so tests can drive the framing seam over a `socketpair(2)` peer without a real `connect`
  /// — production goes through ``connect(toUnixSocketPath:)``.
  ///
  /// `receiveBufferLimit` BOUNDS the inbound stream (the CE2.1 carried constraint): the read
  /// loop pumps from here while the connection's `receive()` consumer is lazy, so an unbounded
  /// `AsyncThrowingStream` would let bytes accumulate without limit. The stream keeps at most
  /// `receiveBufferLimit` un-consumed chunks; a chunk that would overflow that bound ends the
  /// stream with ``UnixSocketTransportError/receiveBufferOverflow`` (→ a resync upstream)
  /// rather than being silently dropped or growing unbounded.
  ///
  /// **PRECONDITION: `descriptor` is a CONNECTED SOCKET, and ownership passes to the
  /// transport** (only ``close()`` may `close(2)` it). Neither half is checkable here. An fd
  /// closed by anyone else can be reused by a concurrent dial while the read loop is still
  /// polling the number — the very race the teardown rendezvous exists to prevent.
  /// ``connect(toUnixSocketPath:)`` satisfies both by construction. A negative descriptor is
  /// rejected outright rather than tolerated: its ``close()`` would be a total no-op that
  /// never even `finish()`es the inbound stream. (Teardown no longer *depends* on the
  /// descriptor being a socket: since #107 the read arm is woken by the wake pipe, so a
  /// `shutdown(2)` that fails `ENOTSOCK` — or that fires into the read loop's syscall-entry
  /// window — no longer strands ``awaitClosed()``.)
  ///
  /// `raisesShutdownOnClose` is a **TEST SEAM** and nothing else: production always leaves it
  /// `true`. Setting it `false` makes ``close()`` skip the `shutdown(2)`, which is exactly the
  /// observable shape of #107's lost wakeup — the read loop is parked with the peer still
  /// holding its end open and nothing but the wake pipe can end it. It exists so that race is
  /// pinned by a DETERMINISTIC regression test rather than a ~1-in-3000 one.
  init(
    connectedDescriptor descriptor: Int32, receiveBufferLimit: Int = 1024,
    raisesShutdownOnClose: Bool = true
  ) {
    precondition(descriptor >= 0, "UnixSocketTransport requires a connected socket descriptor")
    self.descriptor = descriptor
    self.raisesShutdownOnClose = raisesShutdownOnClose
    (inbound, continuation) = AsyncThrowingStream<Data, any Error>.makeStream(
      bufferingPolicy: .bufferingNewest(receiveBufferLimit))
    startReadLoop()
  }

  /// Writes one already-NDJSON-framed frame to the socket. A closed connection surfaces
  /// as ``BackendError/connectionClosed`` (the fd is gone — nothing is written); a
  /// mid-write `write(2)` failure surfaces as ``UnixSocketTransportError/writeFailed``.
  public func send(_ bytes: Data) async throws {
    try await withCheckedThrowingContinuation { (resume: CheckedContinuation<Void, any Error>) in
      writeQueue.async {
        // Read the descriptor ON the write queue — the same serial queue onto which `close()`
        // defers the real `close(2)`. So between this check and `write(2)` the fd cannot be
        // released (nor its number reused by a concurrent dial).
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
  /// **The bound is ARGUED, not mechanical.** ``TeardownLatch/wait()`` carries no timeout, so
  /// this suspension ends only when ``arriveAtTeardown(_:)`` raises the latch. What makes that
  /// certain is that BOTH arms are unconditional once `closeInitiated` is `true`: the drain arm
  /// is a `writeQueue.async` block enqueued by ``close()`` on the same statement that sets the
  /// flag (dispatch always runs an enqueued block), and the read arm is the read thread's
  /// `defer`, whose closure captures `self` STRONGLY so the loop always runs and always
  /// returns — every step of that loop is bounded, because it waits in `poll(2)` on the wake
  /// pipe as well as the socket and reads with `MSG_DONTWAIT`, and `close()` puts a byte in
  /// that pipe BEFORE enqueuing the drain arm. Neither arm waits on the other
  /// (``arriveAtTeardown(_:)`` is a rendezvous), so neither can starve the other. #94 permits
  /// an argued bound; this is it.
  ///
  /// **#107 removed the premise that used to make this argument unsound.** It previously read
  /// "`close()` `shutdown(2)`s the fd, which returns EOF from a parked `read(2)`" — true only
  /// if the reader is ALREADY parked. A `shutdown(2)` landing in the reader's syscall-entry
  /// window woke nobody and the ensuing `read(2)` blocked forever (~1 in 3000 such races).
  /// The wake pipe replaces that edge with durable state, so the read arm's arrival no longer
  /// depends on any interleaving — nor on the descriptor being a socket at all.
  ///
  /// **One premise remains ENVIRONMENTAL** and is named rather than assumed:
  /// `Foundation.Thread.start()` has no failure channel, so under thread exhaustion the read
  /// loop never runs, `.readLoopExit` never arrives, and this suspension never ends.
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
      // Reaching here, `descriptor` is necessarily valid: `init` preconditions it non-negative
      // and it is only ever set to -1 inside THIS critical section, on the same statement that
      // sets `isClosed` — which the guard above already turned away. So the flags below are
      // flipped only where the rendezvous is genuinely armed, never on a path with no
      // descriptor to release (which would send ``awaitClosed()`` past its guard onto a latch
      // nothing will ever raise).
      let current = self.descriptor
      isClosed = true
      closeInitiated = true
      self.descriptor = -1
      // Hand the fd to the rendezvous: from here it is released by whichever teardown arm
      // arrives second, never by this caller.
      pendingCloseDescriptor = current
      return current
    }
    guard descriptor >= 0 else { return }  // already closed — idempotent no-op.

    // `shutdown` tells the PEER we are done (it sees EOF promptly). It is deliberately NOT
    // what the read arm's arrival depends on — one landing in the reader's syscall-entry
    // window wakes nobody, which is how #107's teardown hung. The wake below is the dependable
    // signal, and it is raised BEFORE the drain arm is enqueued, so the rendezvous can never
    // release the wake out from under this call.
    if raisesShutdownOnClose { UnixSocketPosix.shutdownDescriptor(descriptor) }
    wake.raise()
    // The real `close(2)` is deferred to the teardown rendezvous, whose write-queue arm is
    // enqueued HERE — so it runs strictly after any write already queued on this fd, and
    // `send` re-checks `isClosed` on the same serial queue, so a write enqueued after this
    // point is skipped rather than landing on the closed fd. The block captures `self`
    // strongly on purpose: dispatch will always run it, so the arm can never go missing (#94).
    writeQueue.async {
      self.arriveAtTeardown(.writeQueueDrain)
    }
    continuation.finish()
  }

  // MARK: - Teardown rendezvous

  /// The two parties whose departure from the fd `close(2)` must wait for.
  private enum TeardownArm {
    /// The read thread has returned from ``readLoop()`` — it will never touch the socket or
    /// the wake pipe again.
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
  /// fd is `close(2)`d exactly once (the `pendingCloseDescriptor` handoff is a compare-and-take
  /// under ``lock``), and only after both arms have provably stopped touching it. An arm that
  /// arrives before ``close()`` was ever called simply records itself; `close()` supplying the
  /// fd later completes the pair.
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
    // The wake goes with it, and only here: its read end belongs to the read loop (which has
    // provably exited — that is `.readLoopExit`) and its write end is only ever touched by
    // `close()`, strictly before the drain arm it enqueues. So neither end can be in use.
    wake.release()
    // The fd is now released and neither a queued write nor the read thread can touch it:
    // a reconnect awaiting `awaitClosed()` may safely dial the new socket.
    closeCompleted.signal()
  }

  // MARK: - Inbound pump

  private func startReadLoop() {
    // STRONG capture, deliberately (#94). This thread is the ONLY producer of the
    // `.readLoopExit` arm, and `Thread.start()` is asynchronous: under load the thread can be
    // scheduled well after the caller has closed and released the transport. A `weak` capture
    // therefore left a window in which the reference was already nil, the read loop never ran
    // at all, and the arm never arrived — stranding teardown forever. Owning `self` for the
    // thread's lifetime makes that window unconstructible, and matches the type's contract
    // that `deinit` cannot fire while the read thread lives. (`start()` itself succeeding is
    // an unavoidable premise — see ``awaitClosed()``.)
    let thread = Thread { [self] in
      readLoop()
    }
    thread.name = "sprinter.UnixSocketTransport.read"
    thread.start()
  }

  /// What one turn of the read loop decided.
  private enum PumpStep {
    /// Keep pumping.
    case again
    /// Stop, and `finish()` the inbound stream (EOF, a close, or a read error).
    case endOfStream
    /// Stop; the stream has ALREADY been finished (with an overflow error, or by its consumer).
    case streamAlreadyFinished
  }

  private func readLoop() {
    // Arrive the instant the read thread stops touching the fd (however it exits), so the
    // deferred `close(2)` can only run once we are provably out of the socket. The fd number
    // is thus never released while this thread might still be polling or reading it.
    defer { arriveAtTeardown(.readLoopExit) }
    let bufferSize = 64 * 1024
    var buffer = [UInt8](repeating: 0, count: bufferSize)
    readPump: while true {
      lock.lock()
      let descriptor = self.descriptor
      let closed = isClosed
      lock.unlock()
      guard !closed, descriptor >= 0 else { break }

      // Wait on the socket AND the wake together (#107). This — not the socket read — is where
      // the loop spends its time, and it ends on durable state on either fd, so no `close()`
      // can slip through a syscall-entry window and leave the loop parked. See ``ReadLoopWake``.
      switch wake.waitForData(socket: descriptor) {
      case .woken: break readPump  // closing: leave without touching the socket.
      case .retry: continue
      case .socketReadable: break
      }
      switch pumpOnce(descriptor: descriptor, into: &buffer) {
      case .again: continue
      case .endOfStream: break readPump
      case .streamAlreadyFinished: return
      }
    }
    continuation.finish()
  }

  /// One `recv` + `yield`. Reads NON-blocking (`MSG_DONTWAIT`): a `poll(2)` readiness that does
  /// not survive to the read must not turn into an unbounded park — `EAGAIN` just sends the
  /// loop back to the wait.
  private func pumpOnce(descriptor: Int32, into buffer: inout [UInt8]) -> PumpStep {
    let count = buffer.withUnsafeMutableBytes { raw in
      UnixSocketPosix.receiveWithoutBlocking(descriptor, into: raw)
    }
    guard count > 0 else {
      if count == 0 { return .endOfStream }  // EOF: the daemon closed the connection.
      // Interrupted, or readiness did not survive to the read — wait again. Any other errno
      // (incl. the descriptor closed under us) ends the stream.
      let retryable = errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK
      return retryable ? .again : .endOfStream
    }
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
