import Foundation
import SprinterBackend
import SprinterContract

/// A deterministic, offline fake ``Backend`` for the composition tests: it serves a
/// fixed baseline ``Snapshot`` and keeps its `events` subscription open so a real
/// ``WorkGraphResync`` (and the ``AppModel`` board it feeds) can be driven end to end
/// without a daemon or network. The write/session surface is unused here and throws.
final class FakeBackend: Backend, @unchecked Sendable {
  private let baseline: Snapshot
  private let eventStream: AsyncThrowingStream<WorkGraphEvent, any Error>
  private let continuation: AsyncThrowingStream<WorkGraphEvent, any Error>.Continuation
  private let closedState = ClosedFlag()

  init(snapshot: Snapshot) {
    self.baseline = snapshot
    (self.eventStream, self.continuation) = AsyncThrowingStream.makeStream()
  }

  var wasClosed: Bool { closedState.value }

  func snapshot() async throws -> Snapshot { baseline }

  func events() -> AsyncThrowingStream<WorkGraphEvent, any Error> { eventStream }

  func close() async {
    closedState.value = true
    continuation.finish()
  }

  // MARK: - Unused write / session surface

  func createWorkstreamFromPlan(_ plan: WorkstreamPlan) async throws -> WorkstreamId {
    throw ContractError.planRejected(reason: "unsupported in FakeBackend")
  }

  func control(workstreamId: WorkstreamId, action: ControlAction) async throws {
    throw ContractError.workstreamNotFound(id: workstreamId)
  }

  func retryIssue(issueId: IssueId) async throws {
    throw ContractError.issueNotFound(id: issueId)
  }

  func sessionEvents(sessionId: SessionId) -> AsyncThrowingStream<SessionEvent, any Error> {
    AsyncThrowingStream { $0.finish() }
  }

  func sessionSend(sessionId: SessionId, input: SessionInput) async throws {
    throw ContractError.sessionNotFound(id: sessionId)
  }

  func interrupt(sessionId: SessionId) async throws {
    throw ContractError.sessionNotFound(id: sessionId)
  }

  func answerUiRequest(sessionId: SessionId, response: UiResponse) async throws {
    throw ContractError.sessionNotFound(id: sessionId)
  }
}

/// Polls `predicate` on the main actor until it holds, yielding between checks so the
/// feed-consumption / connect tasks can run. Returns `false` if the bound is exhausted.
@MainActor
func waitUntil(_ predicate: @MainActor () -> Bool) async -> Bool {
  for _ in 0..<100_000 {
    if predicate() { return true }
    await Task.yield()
  }
  return false
}

/// A tiny lock-guarded flag so `close()` can record teardown across the actor hops the
/// fake is driven from, without a data race.
private final class ClosedFlag: @unchecked Sendable {
  private let lock = NSLock()
  private var closed = false
  var value: Bool {
    get { lock.withLock { closed } }
    set { lock.withLock { closed = newValue } }
  }
}

/// A fake ``DaemonTransportProvider`` for exercising ``DaemonConnection``'s LIVE
/// initializer offline: it records the endpoint it was asked to build and returns a
/// trivial in-memory transport, so `connect()` assembles a real ``RpcBackend`` without a
/// socket. No RPC traffic is driven — the test asserts only that the wiring resolves.
final class FakeTransportProvider: DaemonTransportProvider, @unchecked Sendable {
  private let recorded = RecordedEndpoint()

  var requestedEndpoint: DaemonEndpoint? { recorded.value }

  func makeTransport(for endpoint: DaemonEndpoint) async throws -> any RpcTransport {
    recorded.value = endpoint
    return NullTransport()
  }
}

private final class RecordedEndpoint: @unchecked Sendable {
  private let lock = NSLock()
  private var endpoint: DaemonEndpoint?
  var value: DaemonEndpoint? {
    get { lock.withLock { endpoint } }
    set { lock.withLock { endpoint = newValue } }
  }
}

/// A no-op ``RpcTransport``: it holds no OS resource, sends nowhere, and yields an
/// immediately-finished inbound stream. Enough for ``DaemonConnection`` to build an
/// ``RpcBackend`` without a live socket.
private final class NullTransport: RpcTransport {
  func send(_ bytes: Data) async throws {}
  func receive() -> AsyncThrowingStream<Data, any Error> {
    AsyncThrowingStream { $0.finish() }
  }
  func close() {}
}

/// A one-node baseline snapshot: an active workstream ⊃ epic ⊃ in-progress issue, so the
/// board projects a non-empty tree the ``AppModel`` test can wait on.
enum AppSupportFixtures {
  static let snapshot = Snapshot(
    workstreams: [
      Workstream(
        id: WorkstreamId(rawValue: "ws-1"),
        name: "Convergence",
        repo: "callajd/sprinter",
        status: .active,
        epics: [EpicId(rawValue: "ep-1")])
    ],
    epics: [
      Epic(
        id: EpicId(rawValue: "ep-1"),
        workstreamId: WorkstreamId(rawValue: "ws-1"),
        name: "CE3",
        status: .active,
        issues: [IssueId(rawValue: "is-1")])
    ],
    issues: [
      Issue(
        id: IssueId(rawValue: "is-1"),
        epicId: EpicId(rawValue: "ep-1"),
        number: 64,
        title: "App target + feature Views",
        status: .inProgress,
        dependsOn: [],
        pullRequest: nil)
    ],
    jobs: [],
    sessions: [])
}
