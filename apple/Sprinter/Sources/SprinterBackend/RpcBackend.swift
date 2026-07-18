import Foundation
import SprinterContract

/// The RPC-over-transport adapter for the ``Backend`` port (BE1.1).
///
/// It maps each port method onto the transport-generic ``RpcConnection``: a query
/// awaits the correlated `Exit` and decodes its success value (or throws the
/// mirrored ``ContractError``); `events` wraps the raw chunk stream, decoding each
/// value into a ``WorkGraphEvent`` — an unknown `_tag` surfaces as a decode
/// failure, never a silent drop. All request bodies reuse the frozen
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
    AsyncThrowingStream { continuation in
      let task = Task {
        do {
          for try await value in await connection.stream(tag: "events", payload: nil) {
            continuation.yield(try fromJSONValue(WorkGraphEvent.self, value))
          }
          continuation.finish()
        } catch {
          continuation.finish(throwing: error)
        }
      }
      continuation.onTermination = { _ in task.cancel() }
    }
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
