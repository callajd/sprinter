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
}
