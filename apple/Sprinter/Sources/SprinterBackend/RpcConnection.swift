import Foundation
import SprinterContract

/// A stream of raw envelope values (chunk payloads), decoded by the typed surface.
typealias JSONStream = AsyncThrowingStream<JSONValue, any Error>

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
  private var reassembler = NdjsonReassembler()
  private var pending: [RequestId: PendingEntry] = [:]
  private var nextRequestId = 0
  private var receiveTask: Task<Void, Never>?
  private var closed = false

  init(transport: any RpcTransport) {
    self.transport = transport
  }

  /// Tears the connection down: cancels the receive loop, closes the transport, and
  /// fails every in-flight request with ``BackendError/connectionClosed``. Idempotent
  /// — a second call is a no-op — so the port's `close()` is safe to call repeatedly.
  func close() {
    guard !closed else { return }
    closed = true
    receiveTask?.cancel()
    receiveTask = nil
    transport.close()
    failAll(with: BackendError.connectionClosed)
  }

  /// One in-flight request, resolved off its correlated terminal `Exit`.
  private enum PendingEntry {
    case query(CheckedContinuation<JSONValue?, any Error>)
    case stream(JSONStream.Continuation)
  }

  // MARK: - Issuing requests

  /// Sends a request/response `Request` and awaits the value (or error) carried by
  /// its correlated `Exit`. A void-success RPC resolves to `nil`.
  func request(tag: String, payload: JSONValue?) async throws -> JSONValue? {
    guard !closed else { throw BackendError.connectionClosed }
    startReceiving()
    let id = allocateId()
    return try await withCheckedThrowingContinuation { continuation in
      pending[id] = .query(continuation)
      Task { await self.transmit(.request(id: id, tag: tag, payload: payload)) }
    }
  }

  /// Opens a streaming subscription: sends a `Request` and returns a stream fed by
  /// correlated `Chunk` frames until the terminal `Exit`. Early consumer
  /// termination sends an `Interrupt` for the request.
  func stream(tag: String, payload: JSONValue?) async -> JSONStream {
    let (stream, continuation) = JSONStream.makeStream()
    guard !closed else {
      continuation.finish(throwing: BackendError.connectionClosed)
      return stream
    }
    startReceiving()
    let id = allocateId()
    continuation.onTermination = { [weak self] _ in
      guard let self else { return }
      Task { await self.interruptIfPending(id) }
    }
    pending[id] = .stream(continuation)
    await transmit(.request(id: id, tag: tag, payload: payload))
    return stream
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
      await handleChunk(requestId, values)
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

  private func handleChunk(_ id: RequestId, _ values: [JSONValue]) async {
    guard case .stream(let continuation)? = pending[id] else { return }
    for value in values {
      continuation.yield(value)
    }
    // Send the per-batch `Ack` flow-control frame the envelope defines. NOTE: this
    // acks on RECEIPT, not on consumer DEMAND — it does not yet gate the server on
    // downstream consumption, so it is the handshake, not true backpressure. Demand-
    // gated acking (defer the `Ack` until the consumer drains) + a bounded buffer
    // with overflow→resync belong with the live streaming transport and BE1.2's
    // snapshot-resync (which owns the recovery); see the workstream ledger.
    await transmit(.ack(requestId: id))
  }

  private func handleExit(_ id: RequestId, _ exit: ExitFrame) {
    guard let entry = pending.removeValue(forKey: id) else { return }
    switch entry {
    case .query(let continuation):
      switch exit {
      case .success(let value):
        continuation.resume(returning: value)
      case .failure(let cause):
        continuation.resume(throwing: causeError(cause))
      }
    case .stream(let continuation):
      switch exit {
      case .success:
        continuation.finish()
      case .failure(let cause):
        continuation.finish(throwing: causeError(cause))
      }
    }
  }

  private func failAll(with error: any Error) {
    let entries = pending
    pending.removeAll()
    for entry in entries.values {
      switch entry {
      case .query(let continuation):
        continuation.resume(throwing: error)
      case .stream(let continuation):
        continuation.finish(throwing: error)
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
