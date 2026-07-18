import Foundation
import SprinterContract

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
    sessions: [])

  static let plan = WorkstreamPlan(
    name: "Foundation", repo: "callajd/sprinter", spec: "build the thing")

  static let issueEvent = WorkGraphEvent.issueChanged(issue)
  static let workstreamEvent = WorkGraphEvent.workstreamChanged(workstream)

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
    sessions: [])

  // ── Session channel ─────────────────────────────────────────────────────────

  static let sessionId = SessionId(rawValue: "sess-1")

  static let sessionInput = SessionInput(text: "ship it", images: nil, mode: .prompt)

  static let uiRequestEvent = SessionEvent.uiRequestRaised(
    id: "req-1", kind: .confirm, prompt: "Merge the PR?", options: nil)
}
