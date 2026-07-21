import Foundation
import SprinterContract

@testable import SprinterBackend

/// Representative owned-DTO values used to drive the fake-transport tests.
enum Fixtures {
  static let workstream = Workstream(
    id: WorkstreamId(rawValue: "ws-1"),
    name: "Foundation",
    repo: "callajd/sprinter",
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
  /// retained baseline was hydrated in. They are one resume context — the daemon refuses a
  /// cursor sent without its generation — so the tests assert the pair, never a bare offset.
  static func resumePayload(_ sinceOffset: Int) throws -> JSONValue {
    try toJSONValue(EventsPayload(sinceOffset: sinceOffset, generation: generation))
  }

  /// The daemon's refusal AFTER a drop-and-recreate. Note the cursor is WITHIN the new
  /// log's extent, so nothing about the offsets is suspicious — only the generation
  /// identity distinguishes this from a perfectly ordinary resume.
  static func resyncRefusal(sinceOffset: Int) -> ContractError {
    .resyncRequired(sinceOffset: sinceOffset, maxOffset: 2, generation: generationAfterReset)
  }

  static let plan = WorkstreamPlan(
    name: "Foundation", repo: "callajd/sprinter", spec: "build the thing")

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
    workstreams: [
      Workstream(
        id: WorkstreamId(rawValue: "ws-1"),
        name: "Foundation",
        repo: "callajd/sprinter",
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
