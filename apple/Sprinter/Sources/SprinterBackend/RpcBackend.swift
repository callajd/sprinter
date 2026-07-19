import Foundation
import SprinterContract

/// The RPC-over-transport adapter for the ``Backend`` port (BE1.1).
///
/// It maps each port method onto the transport-generic ``RpcConnection``: a query
/// awaits the correlated `Exit` and decodes its success value (or throws the
/// mirrored ``ContractError``); `events` wraps the raw chunk stream, decoding each
/// value into an ``OffsetEvent`` envelope and yielding its ``WorkGraphEvent`` — an
/// unknown inner `_tag` surfaces as a decode failure, never a silent drop. All
/// request bodies reuse the frozen
/// `SprinterContract` payload DTOs (INV-CONTRACT); this type adds no new wire
/// shapes beyond the envelope.
///
/// The same adapter serves a local-daemon and a remote-daemon connection — the
/// only difference is which ``RpcTransport`` it is built over (INV-PORT).
public struct RpcBackend: Backend {
  private let connection: RpcConnection

  /// Builds an adapter over an injected transport (any local/remote conformer).
  public init(transport: any RpcTransport) {
    self.connection = RpcConnection(transport: transport)
  }

  public func snapshot() async throws -> Snapshot {
    let value = try await connection.request(tag: "snapshot", payload: nil)
    return try requireValue(value)
  }

  public func createWorkstreamFromPlan(_ plan: WorkstreamPlan) async throws -> WorkstreamId {
    let payload = try toJSONValue(CreateWorkstreamFromPlanPayload(plan: plan))
    let value = try await connection.request(tag: "createWorkstreamFromPlan", payload: payload)
    return try requireValue(value)
  }

  public func control(workstreamId: WorkstreamId, action: ControlAction) async throws {
    let payload = try toJSONValue(ControlPayload(workstreamId: workstreamId, action: action))
    _ = try await connection.request(tag: "control", payload: payload)
  }

  public func retryIssue(issueId: IssueId) async throws {
    let payload = try toJSONValue(RetryIssuePayload(issueId: issueId))
    _ = try await connection.request(tag: "retryIssue", payload: payload)
  }

  public func events() -> AsyncThrowingStream<WorkGraphEvent, any Error> {
    // The port's bare feed is the origin-replay offset feed with the offset dropped.
    let offsetEvents = events(sinceOffset: nil)
    return AsyncThrowingStream { continuation in
      let task = Task {
        do {
          for try await offsetEvent in offsetEvents {
            continuation.yield(offsetEvent.event)
          }
          continuation.finish()
        } catch {
          continuation.finish(throwing: error)
        }
      }
      continuation.onTermination = { _ in task.cancel() }
    }
  }

  public func events(sinceOffset: Int?) -> AsyncThrowingStream<OffsetEvent, any Error> {
    // FLOW CONTROL NOTE: this interposes an UNBOUNDED ``AsyncThrowingStream`` over the
    // bounded ``AckGatedStream`` and drains it as fast as it can, so the gate's deferred
    // ack fires on arrival — it is NOT a live demand signal that throttles the daemon on
    // this path. Memory is still bounded, by two per-request bounds upstream of any
    // unbounded growth: the ``AckGate`` backlog cap (``RpcConnection``'s streamBufferLimit)
    // caps how far a fast daemon can run ahead, and ``WorkGraphResync``'s bounded
    // reconciler queue collapses the attempt into a snapshot/offset resync the moment the
    // reconciler falls behind — loss-free via the offset cursor. See ``AckGate``.
    AsyncThrowingStream { continuation in
      let task = Task {
        do {
          // The wire carries the ``OffsetEvent`` envelope (contract v3 / CE2.0): each
          // item pairs the delta with its DURABLE offset, which ``WorkGraphResync`` tracks
          // to resume STRICTLY AFTER the last-applied offset on reconnect. An unknown
          // inner `_tag` still surfaces as a decode failure, never a silent drop.
          //
          // Send a PRESENT ``EventsPayload`` (the `sinceOffset` KEY is omitted when `nil`
          // → origin replay; present → incremental resume) so the request encodes a
          // payload object, matching the canonical Effect client (INV-CONTRACT). Under v3
          // the payload schema is a `Struct`, so an OMITTED payload key would decode to
          // `undefined` and fail — a present object decodes correctly.
          let payload = try toJSONValue(EventsPayload(sinceOffset: sinceOffset))
          for try await value in await connection.stream(tag: "events", payload: payload) {
            continuation.yield(try fromJSONValue(OffsetEvent.self, value))
          }
          continuation.finish()
        } catch {
          continuation.finish(throwing: error)
        }
      }
      continuation.onTermination = { _ in task.cancel() }
    }
  }

  // MARK: - Session channel (BE1.2)

  public func sessionEvents(sessionId: SessionId) -> AsyncThrowingStream<SessionEvent, any Error> {
    AsyncThrowingStream { continuation in
      let task = Task {
        do {
          // The wire carries the ``OffsetSessionEvent`` envelope (contract v4) — ONE channel
          // serving BOTH modalities: DURABLE transcript-grade events carry a per-session
          // `offset`, EPHEMERAL live deltas ride offset-less. Send a PRESENT
          // ``SessionEventsPayload`` with NO `sinceOffset` key (→ origin replay of the durable
          // transcript, then live tail), then UNWRAP `.event` and yield EVERY event — durable
          // and ephemeral alike — to the existing ``SessionEvent`` fold (unchanged), which folds
          // durable entries into the transcript and applies ephemeral deltas live. Tracking the
          // offset for a reconnect resume is deferred — ``InteractiveSession`` is a single,
          // non-reconnecting subscription — so only the wire is made offset-aware here (the
          // optional `offset` is unwrapped away); resume via `sinceOffset` layers on later,
          // exactly as ``WorkGraphResync`` did for the `events` feed.
          let payload = try toJSONValue(SessionEventsPayload(sessionId: sessionId))
          for try await value in await connection.stream(tag: "sessionEvents", payload: payload) {
            continuation.yield(try fromJSONValue(OffsetSessionEvent.self, value).event)
          }
          continuation.finish()
        } catch {
          continuation.finish(throwing: error)
        }
      }
      continuation.onTermination = { _ in task.cancel() }
    }
  }

  public func sessionSend(sessionId: SessionId, input: SessionInput) async throws {
    let payload = try toJSONValue(SessionSendPayload(sessionId: sessionId, input: input))
    _ = try await connection.request(tag: "sessionSend", payload: payload)
  }

  public func interrupt(sessionId: SessionId) async throws {
    let payload = try toJSONValue(InterruptPayload(sessionId: sessionId))
    _ = try await connection.request(tag: "interrupt", payload: payload)
  }

  public func answerUiRequest(sessionId: SessionId, response: UiResponse) async throws {
    let payload = try toJSONValue(AnswerUiRequestPayload(sessionId: sessionId, response: response))
    _ = try await connection.request(tag: "answerUiRequest", payload: payload)
  }

  public func close() async {
    await connection.close()
  }

  /// Decodes a required (non-void) success value, or throws
  /// ``BackendError/malformedResponse`` when the `Exit` carried no value.
  private func requireValue<Value: Decodable>(_ value: JSONValue?) throws -> Value {
    guard let value else {
      throw BackendError.malformedResponse
    }
    return try fromJSONValue(Value.self, value)
  }
}
