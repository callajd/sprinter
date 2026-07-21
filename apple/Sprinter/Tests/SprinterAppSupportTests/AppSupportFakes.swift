import Foundation
import SprinterBackend
import SprinterContract

/// A deterministic, offline fake ``Backend`` for the composition tests: it serves a
/// fixed baseline ``Snapshot`` and keeps its `events` subscription open so a real
/// ``WorkGraphResync`` (and the ``AppModel`` board it feeds) can be driven end to end
/// without a daemon or network. The write/session surface is unused here and throws.
///
/// It supports MULTIPLE concurrent `events()` consumers (each a fresh stream), because a
/// single connection is now consumed by BOTH the board feed and the ``AppModel`` reconnect
/// loop's liveness watch. ``simulateDrop()`` finishes every open feed to model the daemon
/// going away (a restart) so the loop's liveness watch observes the drop and re-dials.
final class FakeBackend: Backend, @unchecked Sendable {
  private let baseline: Snapshot
  private let feeds = EventFeeds()
  private let closedState = ClosedFlag()

  init(snapshot: Snapshot) {
    self.baseline = snapshot
  }

  var wasClosed: Bool { closedState.value }

  func snapshot() async throws -> Snapshot { baseline }

  func events() -> AsyncThrowingStream<WorkGraphEvent, any Error> {
    feeds.makeStream()
  }

  /// Models the daemon going away mid-connection (a restart): finishes every open `events`
  /// feed so a liveness watch sees the drop. Distinct from ``close()`` (the app tearing the
  /// connection down); a dropped backend is not marked `wasClosed` by this call.
  func simulateDrop() {
    feeds.finishAll()
  }

  func close() async {
    closedState.value = true
    feeds.finishAll()
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

/// A lock-guarded registry of open `events` continuations, so one ``FakeBackend`` can serve
/// several concurrent `events()` consumers (the board feed AND the ``AppModel`` liveness
/// watch) and finish them all together on a drop/close.
private final class EventFeeds: @unchecked Sendable {
  private typealias Feed = AsyncThrowingStream<WorkGraphEvent, any Error>.Continuation
  private let lock = NSLock()
  private var continuations: [UUID: Feed] = [:]

  func makeStream() -> AsyncThrowingStream<WorkGraphEvent, any Error> {
    let id = UUID()
    return AsyncThrowingStream { continuation in
      lock.withLock { continuations[id] = continuation }
      continuation.onTermination = { [weak self] _ in
        self?.lock.withLock { _ = self?.continuations.removeValue(forKey: id) }
      }
    }
  }

  func finishAll() {
    let open = lock.withLock {
      let values = Array(continuations.values)
      continuations.removeAll()
      return values
    }
    for continuation in open { continuation.finish() }
  }
}

/// A connect seam a test can HOLD OPEN: the dial reports that it started, then parks until
/// the test releases it. That makes the "dial resolves after `stop()`" window — which is
/// otherwise a race — deterministic.
///
/// It is a COUNTING gate, exactly like the `DispatchSemaphore` it replaces: ``AppModel/start()``
/// drives TWO consumers of the same connect seam (the board's ``WorkGraphResync`` feed and the
/// connect loop), so both park here, and one ``release()`` must let exactly ONE through. A
/// sticky "released" flag would let the board's dial resolve too, close the fake behind the
/// connect loop's back, and make the late-dial regression test pass vacuously.
///
/// The park is a SUSPENSION, not a `DispatchSemaphore.wait` hopped onto a global queue: the
/// latter occupies a libdispatch worker for as long as the dial is held — and the dial that is
/// never released holds one for the rest of the process. That is the block-a-worker
/// antipattern this PR removes from the transport, and it should not live inside the gate.
/// ``releaseAll()`` drains the rest at the end of a test so no continuation is left dangling.
final class HeldDial: @unchecked Sendable {
  private let lock = NSLock()
  private var started = false
  private var resumed = false
  private var permits = 0
  private var waiters: [CheckedContinuation<Void, Never>] = []

  var didStart: Bool { lock.withLock { started } }
  /// `true` once a parked ``park()`` has actually RESUMED — the point after which the connect
  /// seam's own outcome (a live connection, or a throw) has reached the connect loop.
  var didResume: Bool { lock.withLock { resumed } }

  /// Called from inside the connect seam: mark the dial in flight, then park.
  func park() async {
    lock.withLock { started = true }
    defer { lock.withLock { resumed = true } }
    await withCheckedContinuation { (resume: CheckedContinuation<Void, Never>) in
      let granted: Bool = lock.withLock {
        guard permits > 0 else {
          waiters.append(resume)
          return false
        }
        permits -= 1
        return true
      }
      if granted { resume.resume() }
    }
  }

  /// Lets exactly ONE parked dial resolve (or banks a permit for the next one).
  func release() {
    let next: CheckedContinuation<Void, Never>? = lock.withLock {
      guard !waiters.isEmpty else {
        permits += 1
        return nil
      }
      return waiters.removeFirst()
    }
    next?.resume()
  }

  /// Drains every remaining parked dial — test cleanup, so nothing is left suspended on a
  /// continuation that will never be resumed.
  func releaseAll() {
    let parked: [CheckedContinuation<Void, Never>] = lock.withLock {
      let pending = waiters
      waiters.removeAll()
      return pending
    }
    for waiter in parked { waiter.resume() }
  }
}

/// A lock-guarded counter of connect-seam calls, so a test can fail the first N dials and
/// succeed afterward — exercising the ``AppModel`` reconnect loop's retry-with-backoff.
final class DialCounter: @unchecked Sendable {
  private let lock = NSLock()
  private var failuresRemaining: Int

  init(failFirst: Int) {
    self.failuresRemaining = failFirst
  }

  /// Returns `true` (and consumes one) while failures remain, else `false`.
  func shouldFail() -> Bool {
    lock.withLock {
      guard failuresRemaining > 0 else { return false }
      failuresRemaining -= 1
      return true
    }
  }
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
  /// The repository the workstream REFERENCES — a snapshot must carry it, or the
  /// board could not resolve the reference into a name.
  static let repository = Repository(
    id: RepositoryId(rawValue: "repo:github:1296269"),
    host: .github,
    owner: "callajd",
    name: "sprinter",
    refs: [
      RepositoryRef(
        name: BranchName(rawValue: "main"),
        sha: CommitSha(rawValue: "0123456789abcdef0123456789abcdef01234567"))
    ],
    observedAt: "2026-07-20T12:00:00.000Z")

  static let snapshot = Snapshot(
    repositories: [repository],
    workstreams: [
      Workstream(
        id: WorkstreamId(rawValue: "ws-1"),
        name: "Convergence",
        repositoryId: RepositoryId(rawValue: "repo:github:1296269"),
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
    sessions: [],
    agents: [],
    generation: StoreGenerationId(rawValue: "gen-test"))
}
