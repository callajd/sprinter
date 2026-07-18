import Foundation
import SprinterContract

/// The **`Backend` port** (INV-PORT / D14) — the seam feature code depends on to
/// reach the daemon.
///
/// It speaks only owned domain types (``Snapshot``, ``WorkstreamId``,
/// ``WorkGraphEvent``) and the neutral ``ContractError`` channel. Nothing here
/// references *where* the daemon runs or *which* transport carries the traffic:
/// local-daemon vs. remote-daemon is an adapter choice made when the connection's
/// transport is selected (see ``BackendConnector``), never a distinction the
/// client surface can observe.
public protocol Backend: Sendable {
  /// Hydrates the full owned read model (snapshot-on-connect, D4).
  func snapshot() async throws -> Snapshot

  /// Submits a plan; yields the new ``WorkstreamId`` or throws
  /// ``ContractError/planRejected(reason:)``.
  func createWorkstreamFromPlan(_ plan: WorkstreamPlan) async throws -> WorkstreamId

  /// Applies a lifecycle action; throws ``ContractError/workstreamNotFound(id:)``
  /// for an unknown workstream.
  func control(workstreamId: WorkstreamId, action: ControlAction) async throws

  /// Requeues an issue; throws ``ContractError/issueNotFound(id:)`` for an unknown
  /// issue.
  func retryIssue(issueId: IssueId) async throws

  /// The live work-graph delta subscription (INV-REACTIVE): a stream of
  /// ``WorkGraphEvent`` fed until the daemon ends the subscription.
  func events() -> AsyncThrowingStream<WorkGraphEvent, any Error>
}

/// Transport / protocol-level failures surfaced by the client, distinct from the
/// daemon's owned ``ContractError`` channel. A `Fail` cause decodes to a
/// ``ContractError``; the cases here cover the envelope's non-`Fail` outcomes and
/// broken-connection conditions.
public enum BackendError: Error, Equatable, Sendable {
  /// The transport ended before an in-flight request reached its terminal `Exit`.
  case connectionClosed
  /// The daemon reported an unrecoverable defect (a `Die` cause or `Defect` frame).
  case daemonDefect
  /// The in-flight request was interrupted (an `Interrupt` cause).
  case interrupted
  /// The server reported a client protocol error affecting the connection.
  case protocolError
  /// A response frame could not be decoded into the expected owned type.
  case malformedResponse
}
