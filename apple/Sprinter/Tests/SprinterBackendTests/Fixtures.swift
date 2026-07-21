import Foundation
import SprinterContract

@testable import SprinterBackend

/// Representative owned-DTO values used to drive the fake-transport tests.
enum Fixtures {
  /// The repository the workstream fixtures REFERENCE. Every Snapshot carries it,
  /// because Workstream.repositoryId is a reference a client must be able to resolve.
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

  static let workstream = Workstream(
    id: WorkstreamId(rawValue: "ws-1"),
    name: "Foundation",
    repositoryId: RepositoryId(rawValue: "repo:github:1296269"),
    status: .active,
    epics: [EpicId(rawValue: "ep-1")])

  static let issue = Issue(
    id: IssueId(rawValue: "iss-1"),
    epicId: EpicId(rawValue: "ep-1"),
    number: 34,
    title: "Swift RPC client",
    status: .inProgress,
    dependsOn: [],
    pullRequest: nil)

  /// The store generation the FIRST connect hydrates in — the context every cursor this
  /// baseline produces is a coordinate in.
  static let generation = StoreGenerationId(rawValue: "gen-1")
  /// The generation of a daemon that has been DROPPED AND RECREATED under the client (a
  /// schema-version bump). Distinct from ``generation`` by construction: that difference
  /// is the whole detection mechanism, so the fixtures must not share one.
  static let generationAfterReset = StoreGenerationId(rawValue: "gen-2")

  static let snapshot = Snapshot(
    repositories: [repository],
    workstreams: [workstream],
    epics: [
      Epic(
        id: EpicId(rawValue: "ep-1"),
        workstreamId: WorkstreamId(rawValue: "ws-1"),
        name: "BE1",
        status: .active,
        issues: [IssueId(rawValue: "iss-1")])
    ],
    issues: [issue],
    jobs: [],
    sessions: [],
    agents: [],
    generation: generation)

  /// The wire payload of a RESUME `events` request: the cursor PLUS the generation the
  /// retained baseline was hydrated in, nested in the single `resume` object the contract
  /// defines. A bare offset has no wire form at all, so the tests cannot accidentally
  /// assert one.
  static func resumePayload(_ sinceOffset: Int) throws -> JSONValue {
    try toJSONValue(
      EventsPayload(resume: ResumeContext(sinceOffset: sinceOffset, generation: generation)))
  }

  /// The daemon's refusal AFTER a drop-and-recreate. Note the cursor is WITHIN the new
  /// log's extent, so nothing about the offsets is suspicious — only the generation
  /// identity distinguishes this from a perfectly ordinary resume.
  static func resyncRefusal(sinceOffset: Int) -> ContractError {
    .resyncRequired(sinceOffset: sinceOffset, maxOffset: 2, generation: generationAfterReset)
  }

  static let plan = WorkstreamPlan(
    name: "Foundation",
    repository: RepositoryKey(host: .github, owner: "callajd", name: "sprinter"),
    spec: "build the thing")

  static let issueEvent = WorkGraphEvent.issueChanged(issue)
  static let workstreamEvent = WorkGraphEvent.workstreamChanged(workstream)

  /// Wrap a delta in the streamed ``OffsetEvent`` envelope (CE2.0) —
  /// what the daemon puts on the `events` wire; ``RpcBackend`` unwraps to `.event`.
  static func offsetEvent(_ event: WorkGraphEvent, at offset: Int) -> OffsetEvent {
    OffsetEvent(offset: offset, event: event)
  }

  // ── Reconnect / resync ──────────────────────────────────────────────────────

  /// The same issue, advanced to `in_review` — an UPSERT delta over `snapshot`.
  static let issueInReview = Issue(
    id: IssueId(rawValue: "iss-1"),
    epicId: EpicId(rawValue: "ep-1"),
    number: 34,
    title: "Swift RPC client",
    status: .inReview,
    dependsOn: [],
    pullRequest: nil)

  /// A second baseline the daemon serves AFTER a reconnect — distinct from
  /// `snapshot` so a test can prove the reconnect re-fetched a fresh snapshot
  /// (never delta-only).
  static let snapshotAfterReconnect = Snapshot(
    repositories: [repository],
    workstreams: [
      Workstream(
        id: WorkstreamId(rawValue: "ws-1"),
        name: "Foundation",
        repositoryId: RepositoryId(rawValue: "repo:github:1296269"),
        status: .done,
        epics: [EpicId(rawValue: "ep-1")])
    ],
    epics: [
      Epic(
        id: EpicId(rawValue: "ep-1"),
        workstreamId: WorkstreamId(rawValue: "ws-1"),
        name: "BE1",
        status: .done,
        issues: [IssueId(rawValue: "iss-1")])
    ],
    issues: [issueInReview],
    jobs: [],
    sessions: [],
    agents: [],
    generation: generationAfterReset)

  // ── Session channel ─────────────────────────────────────────────────────────

  static let sessionId = SessionId(rawValue: "sess-1")

  static let sessionInput = SessionInput(text: "ship it", images: nil, mode: .prompt)

  static let uiRequestEvent = SessionEvent.uiRequestRaised(
    id: "req-1", kind: .confirm, prompt: "Merge the PR?", options: nil)
}
