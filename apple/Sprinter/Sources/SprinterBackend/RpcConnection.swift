import Foundation
import SprinterContract

/// The RPC client connection: an actor that speaks the NDJSON-framed
/// `effect/unstable/rpc` envelope over an injected ``RpcTransport`` (BE1.1).
///
/// It owns all mutable protocol state — the request-id counter, the NDJSON
/// reassembler, and the map of in-flight requests — under actor isolation, so
/// request/response correlation and stream fan-out are data-race free (Swift 6
/// complete strict concurrency). Every outbound `Request` carries a fresh id;
/// each inbound `Chunk`/`Exit` is routed to its originating request by that id.
///
/// This is the transport-generic core. The typed procedure surface (snapshot,
/// commands, the `events` stream) lives in ``RpcBackend`` on top of it.
actor RpcConnection {
  private let transport: any RpcTransport
  private let streamBufferLimit: Int
  private var reassembler = NdjsonReassembler()
  private var pending: [RequestId: PendingEntry] = [:]
  private var nextRequestId = 0
  private var receiveTask: Task<Void, Never>?
  private var closed = false

  /// `streamBufferLimit` bounds each subscription's un-drained backlog in the ``AckGate``.
  /// It is the hard memory bound per stream: a fast daemon can push at most this many
  /// values ahead of the consumer before ``AckGate/Overflow`` trips and the resync loop
  /// recovers — the safety net for an over-large chunk, a daemon that ignores flow control,
  /// or the production ``RpcBackend/events(resume:)`` path (which drains into an
  /// unbounded stream, so the ack is not a live throttle and this bound is what caps it).
  init(transport: any RpcTransport, streamBufferLimit: Int = 1024) {
    self.transport = transport
    self.streamBufferLimit = streamBufferLimit
  }

  /// Tears the connection down: cancels the receive loop, fails every in-flight request
  /// with ``BackendError/connectionClosed``, closes the transport, and — for the live
  /// socket — awaits its FULL drain before returning (so a reconnect dials the new socket
  /// only after the old fd is released; the CE2.1 carried teardown constraint).
  /// Idempotent — a second call is a no-op.
  func close() async {
    guard !closed else { return }
    closed = true
    receiveTask?.cancel()
    receiveTask = nil
    failAll(with: BackendError.connectionClosed)
    transport.close()
    await transport.awaitClosed()
  }

  /// One in-flight request, resolved off its correlated terminal `Exit`.
  private enum PendingEntry {
    case query(QueryEntry)
    case stream(AckGate)
  }

  /// One in-flight request/response entry: the suspended caller, plus the ORDERING state
  /// that keeps a cancellation's `Interrupt` from overtaking its own `Request` on the wire.
  ///
  /// Both the transmit and the cancel run as unstructured `Task`s hopping onto this actor,
  /// and their relative order is NOT a language guarantee (priority escalation can reorder
  /// them). So the ordering is enforced by state rather than by scheduling: the `Interrupt`
  /// is only ever sent once ``transmitted`` is `true`, and a cancellation that arrives first
  /// merely sets ``cancelDeferred``, which the transmit path honours the moment the `Request`
  /// is out. A daemon can therefore never see an `Interrupt` for an id it has not been asked
  /// about — which it would ignore, then run the request to completion with no client-side
  /// consumer left to receive its `Exit`.
  private struct QueryEntry {
    let continuation: CheckedContinuation<JSONValue?, any Error>
    /// `true` when a caller's cancellation may also INTERRUPT the daemon-side work — i.e.
    /// this request's tag is in ``daemonInterruptibleTags``. `false` for every mutating
    /// procedure: the caller is released, the daemon runs the mutation to completion.
    let interruptsDaemon: Bool
    /// `true` once this request's `Request` frame has been handed to the transport.
    var transmitted = false
    /// `true` when the caller cancelled BEFORE the `Request` went out; the transmit path
    /// retires the entry itself, immediately after sending it.
    var cancelDeferred = false
  }

  /// The request/response tags whose daemon-side handler is READ-ONLY, and whose in-flight
  /// work may therefore be aborted by an `Interrupt` when the caller cancels.
  ///
  /// This is an ALLOWLIST, deliberately: `effect/unstable/rpc`'s server interrupts the
  /// handler's fiber the instant it sees an `Interrupt` (`RpcServer`'s `Interrupt` case calls
  /// `fiber.interruptUnsafe`), and the daemon marks no handler `Rpc.uninterruptible`. So an
  /// `Interrupt` for a MUTATING procedure aborts a partially-applied write sequence.
  /// `createWorkstreamFromPlan` is the concrete case: it commits `putRepository` and
  /// `putWorkstream` as two INDEPENDENT SQLite transactions with a host call and a duplicate
  /// check in between, and no `withTransaction` wraps the pair — an interrupt landing between
  /// them leaves a persisted repository row (and its journalled `RepositoryChanged` delta)
  /// with no workstream, and the client cannot tell whether the create landed. `control` is
  /// worse: it writes the workstream, then dispatches jobs on the LAYER scope, so an interrupt
  /// can leave a workstream marked active whose jobs were never dispatched.
  ///
  /// The client-side wait is still bounded for those tags — the caller is resumed with a
  /// `CancellationError` either way, which is #94's actual bar. What is withheld is only the
  /// wire-level `Interrupt`, restoring the pre-#94 daemon semantics: the mutation runs to
  /// completion and its `Exit` is discarded by ``handleExit(_:_:)``'s missing-entry guard.
  /// Bounding a client wait must not silently change what a cancelled mutation does to the
  /// daemon's store.
  ///
  /// Default-DENY: a tag not named here is treated as mutating, so a new procedure is safe
  /// until someone establishes it is read-only. Only `snapshot` qualifies today; the two
  /// streaming tags (`events`, `executionEvents`) are read-only too, but their abandonment path
  /// is ``interruptIfPending(_:)``, not this one.
  private static let daemonInterruptibleTags: Set<String> = ["snapshot"]

  // MARK: - Issuing requests

  /// Sends a request/response `Request` and awaits the value (or error) carried by
  /// its correlated `Exit`. A void-success RPC resolves to `nil`.
  ///
  /// CANCELLATION-AWARE (#94's general bar: no wait may be unbounded). Without a handler
  /// this suspension is released ONLY by a correlated `Exit` or by ``failAll(with:)`` — so a
  /// cancelled caller whose daemon never answers, and whose connection is never closed, waits
  /// forever. On cancellation the pending entry is ALWAYS resolved with a `CancellationError`,
  /// whatever the tag — that is what bounds the wait. Whether the daemon is ALSO told to stop
  /// working on it (`Interrupt`) depends on the tag: only ``daemonInterruptibleTags`` — the
  /// read-only procedures — are interrupted, because the daemon interrupts the handler fiber
  /// outright and a mutating handler is not atomic. Registration happens synchronously on the
  /// actor before this method can suspend, so the handler's hop can never run ahead of the
  /// entry it retires — and ``QueryEntry/transmitted`` keeps the `Interrupt` behind its own
  /// `Request` on the wire.
  func request(tag: String, payload: JSONValue?) async throws -> JSONValue? {
    guard !closed else { throw BackendError.connectionClosed }
    startReceiving()
    let id = allocateId()
    let interruptsDaemon = Self.daemonInterruptibleTags.contains(tag)
    return try await withTaskCancellationHandler {
      try await withCheckedThrowingContinuation { continuation in
        pending[id] = .query(
          QueryEntry(continuation: continuation, interruptsDaemon: interruptsDaemon))
        Task { await self.transmitRequest(id: id, tag: tag, payload: payload) }
      }
    } onCancel: {
      Task { await self.cancelPendingRequest(id) }
    }
  }

  /// Sends a request/response `Request` and records that it went out, then honours a
  /// cancellation that arrived while it was in flight. Marking `transmitted` on the actor
  /// AFTER the send is what makes the `Request`-before-`Interrupt` ordering a state
  /// invariant rather than a bet on which unstructured task the runtime schedules first.
  private func transmitRequest(id: RequestId, tag: String, payload: JSONValue?) async {
    await transmit(.request(id: id, tag: tag, payload: payload))
    // The entry is gone if the `Exit` already landed, or if `transmit` failed everything.
    guard case .query(var entry)? = pending[id] else { return }
    entry.transmitted = true
    pending[id] = .query(entry)
    guard entry.cancelDeferred else { return }
    await retireCancelledRequest(id)
  }

  /// Handles a caller's cancellation. If the `Request` is not yet on the wire the retirement
  /// is DEFERRED to ``transmitRequest(id:tag:payload:)`` — never dropped, and never allowed
  /// to emit its `Interrupt` first. A no-op if the `Exit` already landed (or the connection
  /// already failed it), so the continuation resumes exactly once; a streaming entry with
  /// the same id is left to ``interruptIfPending(_:)``.
  private func cancelPendingRequest(_ id: RequestId) async {
    guard case .query(var entry)? = pending[id] else { return }
    guard entry.transmitted else {
      entry.cancelDeferred = true
      pending[id] = .query(entry)
      return
    }
    await retireCancelledRequest(id)
  }

  /// Retires a cancelled request/response entry whose `Request` is already on the wire:
  /// resumes its continuation with a `CancellationError` and — for a READ-ONLY tag only —
  /// interrupts the daemon-side work.
  ///
  /// The continuation is resumed unconditionally: that is the bounded wait #94 asks for. The
  /// `Interrupt` is withheld for every mutating tag (see ``daemonInterruptibleTags``), so a
  /// cancelled submit can never abort a half-applied write sequence daemon-side; that
  /// request's later `Exit` lands on no entry and is dropped by ``handleExit(_:_:)``.
  private func retireCancelledRequest(_ id: RequestId) async {
    guard case .query(let entry)? = pending[id] else { return }
    pending.removeValue(forKey: id)
    entry.continuation.resume(throwing: CancellationError())
    guard entry.interruptsDaemon else { return }
    await transmit(.interrupt(requestId: id))
  }

  /// Opens a streaming subscription: sends a `Request` and returns an ``AckGatedStream``
  /// fed by correlated `Chunk` frames until the terminal `Exit`. The per-batch `Ack` is
  /// deferred until the consumer drains that batch (demand-gating for a consumer that
  /// paces itself; on the unbounded-interposed ``RpcBackend/events(resume:)`` path the
  /// drain is instant, so the bound below — not the ack — is the flow control), and the
  /// backlog is bounded (overflow → the consumer sees a failure → resync). Early consumer
  /// termination (task cancel, a dropped iterator, or an ``AckGate/Overflow``) sends an
  /// `Interrupt` for the request.
  func stream(tag: String, payload: JSONValue?) async -> AckGatedStream {
    guard !closed else {
      let gate = AckGate(limit: streamBufferLimit, ack: {}, cancelHandler: {})
      gate.fail(BackendError.connectionClosed)
      return AckGatedStream(gate: gate)
    }
    startReceiving()
    let id = allocateId()
    let gate = AckGate(
      limit: streamBufferLimit,
      ack: { [weak self] in await self?.sendAck(id) },
      cancelHandler: { [weak self] in await self?.interruptIfPending(id) })
    pending[id] = .stream(gate)
    await transmit(.request(id: id, tag: tag, payload: payload))
    return AckGatedStream(gate: gate)
  }

  /// Sends the deferred per-batch `Ack` (the demand signal) once the consumer has drained
  /// a batch — the ``AckGate``'s callback into the connection.
  private func sendAck(_ id: RequestId) async {
    await transmit(.ack(requestId: id))
  }

  /// Sends a liveness `Ping` keepalive frame.
  func ping() async {
    await transmit(.ping)
  }

  /// Signals end-of-input for the connection with an `Eof` frame.
  func eof() async {
    await transmit(.eof)
  }

  // MARK: - Outbound

  private func allocateId() -> RequestId {
    defer { nextRequestId += 1 }
    return RequestId(String(nextRequestId))
  }

  private func transmit(_ frame: ClientFrame) async {
    let bytes: Data
    do {
      bytes = try encodeFrame(frame)
    } catch {
      // Encoding an owned ``ClientFrame`` should never fail; surface it as-is if it does.
      failAll(with: error)
      return
    }
    do {
      try await transport.send(bytes)
    } catch let error as BackendError {
      // Already a terminal Backend failure (e.g. `connectionClosed` from a closed fd).
      failAll(with: error)
    } catch let error as UnixSocketTransportError {
      // ASSUMPTION: the real transport's `send` only ever throws
      // ``UnixSocketTransportError/writeFailed`` (its only fallible op here is the write; the
      // typed dial errors stay on the DIAL path in ``DaemonTransports``). A failed `write(2)`
      // on a stream socket means a dead connection, so map it to
      // ``BackendError/connectionClosed`` — like every other terminal Backend failure — so
      // feature view models switching on ``BackendError`` see it, not a transport-specific
      // error. Any OTHER transport case is unexpected on the write path; surface it as-is
      // rather than silently collapsing it into `connectionClosed`.
      if case .writeFailed = error {
        failAll(with: BackendError.connectionClosed)
      } else {
        failAll(with: error)
      }
    } catch {
      // An injected/unexpected transport threw a type we do not model on the write path. Do
      // NOT masquerade it as `connectionClosed` — surface the original so the real failure
      // is never silently lost.
      failAll(with: error)
    }
  }

  private func interruptIfPending(_ id: RequestId) async {
    guard pending.removeValue(forKey: id) != nil else { return }
    await transmit(.interrupt(requestId: id))
  }

  // MARK: - Inbound

  private func startReceiving() {
    guard receiveTask == nil else { return }
    receiveTask = Task { await self.receiveLoop() }
  }

  private func receiveLoop() async {
    do {
      for try await chunk in transport.receive() {
        for frame in try reassembler.push(chunk) {
          await handle(frame)
        }
      }
      failAll(with: BackendError.connectionClosed)
    } catch {
      failAll(with: error)
    }
  }

  private func handle(_ frame: ServerFrame) async {
    switch frame {
    case .chunk(let requestId, let values):
      handleChunk(requestId, values)
    case .exit(let requestId, let exit):
      handleExit(requestId, exit)
    case .defect:
      failAll(with: BackendError.daemonDefect)
    case .pong:
      break
    case .clientProtocolError:
      failAll(with: BackendError.protocolError)
    }
  }

  private func handleChunk(_ id: RequestId, _ values: [JSONValue]) {
    guard case .stream(let gate)? = pending[id] else { return }
    // Hand the batch to the bounded gate WITHOUT acking: the `Ack` is deferred until the
    // consumer drains this batch (``AckGate/next()`` sends it then). For a self-paced
    // consumer that gates the daemon's chunk→ack→chunk flow on downstream demand; on the
    // production `events` path the drain is instant, so the gate's bound (overflow →
    // resync) is what keeps the backlog bounded. Either way the receive loop never blocks
    // on the consumer here.
    gate.push(values)
  }

  private func handleExit(_ id: RequestId, _ exit: ExitFrame) {
    guard let entry = pending.removeValue(forKey: id) else { return }
    switch entry {
    case .query(let query):
      switch exit {
      case .success(let value):
        query.continuation.resume(returning: value)
      case .failure(let cause):
        query.continuation.resume(throwing: causeError(cause))
      }
    case .stream(let gate):
      switch exit {
      case .success:
        gate.finish()
      case .failure(let cause):
        gate.fail(causeError(cause))
      }
    }
  }

  private func failAll(with error: any Error) {
    let entries = pending
    pending.removeAll()
    for entry in entries.values {
      switch entry {
      case .query(let query):
        query.continuation.resume(throwing: error)
      case .stream(let gate):
        gate.fail(error)
      }
    }
  }

  /// Maps a failure `cause` to a thrown error, preferring the typed
  /// ``ContractError`` carried by a `Fail` entry.
  private func causeError(_ cause: [CauseEntry]) -> any Error {
    for entry in cause {
      guard case .fail(let error) = entry else { continue }
      if let contractError = try? fromJSONValue(ContractError.self, error) {
        return contractError
      }
      return BackendError.malformedResponse
    }
    for entry in cause {
      switch entry {
      case .die:
        return BackendError.daemonDefect
      case .interrupt:
        return BackendError.interrupted
      case .fail:
        continue
      }
    }
    return BackendError.malformedResponse
  }
}
