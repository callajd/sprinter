import Foundation
import Testing

@testable import SprinterContract

/// One committed golden paired with the mirror type it represents — the table the
/// ENCODE-AGREEMENT harness walks (issue #89).
///
/// The pairing has to be written down somewhere: the golden files carry no type tag, and
/// `Golden.decode` is told the type at every call site. This makes that knowledge ONE
/// list instead of scattering it, so "is every mirrored type checked in the encode
/// direction?" is a question with an answer — and `EncodeAgreementTests` turns it into an
/// assertion by requiring this table's names to be EXACTLY the goldens in the bundle.
///
/// The closure is type-erased so heterogeneous mirror types live in one array; the
/// concrete type is captured as a metatype and applied inside. Capturing it in a
/// `@Sendable` closure needs `T: Sendable` — which every mirrored DTO is, by design — so
/// the constraint costs nothing and would only bite on a type that had no business here.
struct GoldenCase: Sendable, CustomTestStringConvertible {
  /// The golden's file name, without `.json`.
  let name: String
  /// Decode-then-re-encode `data` as this case's mirror type and report the first
  /// divergence from the golden it came from — see ``Golden/encodeDivergence(_:in:)``.
  let divergence: @Sendable (Data) throws -> String?

  var testDescription: String { name }

  init<T: Codable & Sendable>(_ name: String, _ type: T.Type) {
    self.name = name
    self.divergence = { data in try Golden.encodeDivergence(type, in: data) }
  }
}

extension GoldenCase {
  /// Every committed golden, with its mirror type.
  ///
  /// **D3 — scope.** The decisive coverage is every mirrored type carrying at least one
  /// `Schema.optionalKey` field, because that is the only place omission-vs-`null` can
  /// diverge. Those are, with the golden(s) that pin BOTH the present and the absent form:
  ///
  /// - `Issue` (`pr`) — `issue-with-pr`, `issue-no-pr`
  /// - `Job` (`sessionId`, `transcriptRef`, `pr`) — `job-full`, `job-minimal`
  /// - `Agent` (`supersedes`, `retiredAt`) — `agent-original`, `agent-revised`,
  ///   `agent-retired`
  /// - `Usage` (`cacheReadTokens`, `cacheWriteTokens`) — `usages`
  /// - `SessionInput` (`images`) — `session-inputs`
  /// - `SessionEvent` (`usage`, `text`, `reasoning`, `options`, a `Notice`'s `id`) —
  ///   `session-events`
  /// - `TranscriptEntry` (`reasoning`) — `transcript-entries`
  /// - `OffsetSessionEvent` (`offset`) — `offset-session-events`
  /// - `EventsPayload` (`resume`) — `payload-events`, `payload-events-no-offset`
  /// - `SessionEventsPayload` (`resume`) — `payload-session-events`,
  ///   `payload-session-events-no-offset`
  ///
  /// The table below is not restricted to those: every golden is checked, including the
  /// types with no optional field (welcome but not required by D3) and the decode-only
  /// value types (`Timestamp`-shaped strings, `CommitSha`, `BranchName` — in scope for
  /// ENCODE SHAPE, out of scope for validation), because the containers that embed the
  /// optional-bearing types are where a nesting mistake would actually show up.
  static let all: [GoldenCase] = readModel + registry + sessionModel + commands

  /// The owned read model and the STATE layer, plus the hydration snapshot.
  private static let readModel: [GoldenCase] = [
    GoldenCase("snapshot", Snapshot.self),
    GoldenCase("repository", Repository.self),
    GoldenCase("repository-no-refs", Repository.self),
    GoldenCase("workstream", Workstream.self),
    GoldenCase("workstream-cancelled", Workstream.self),
    GoldenCase("epic", Epic.self),
    GoldenCase("epic-cancelled", Epic.self),
    GoldenCase("issue-with-pr", Issue.self),
    GoldenCase("issue-no-pr", Issue.self),
    GoldenCase("job-full", Job.self),
    GoldenCase("job-minimal", Job.self),
    GoldenCase("session", Session.self),
    GoldenCase("pull-request-ref", PullRequestRef.self),
    GoldenCase("work-graph-events", [WorkGraphEvent].self),
    GoldenCase("offset-events", [OffsetEvent].self)
  ]

  /// The append-only registry layer.
  private static let registry: [GoldenCase] = [
    GoldenCase("agent-original", Agent.self),
    GoldenCase("agent-revised", Agent.self),
    GoldenCase("agent-retired", Agent.self)
  ]

  /// The session channel: its events, transcript records and neutral I/O types.
  private static let sessionModel: [GoldenCase] = [
    GoldenCase("session-events", [SessionEvent].self),
    GoldenCase("offset-session-events", [OffsetSessionEvent].self),
    GoldenCase("transcript-entries", [TranscriptEntry].self),
    GoldenCase("usages", [Usage].self),
    GoldenCase("session-inputs", [SessionInput].self),
    GoldenCase("ui-responses", [UiResponse].self),
    GoldenCase("json-values", [JSONValue].self)
  ]

  /// The command payloads, the one command response, and the contract's error channel.
  private static let commands: [GoldenCase] = [
    GoldenCase("workstream-plan", WorkstreamPlan.self),
    GoldenCase("control-actions", [ControlAction].self),
    GoldenCase("payload-events", EventsPayload.self),
    GoldenCase("payload-events-no-offset", EventsPayload.self),
    GoldenCase("payload-session-events", SessionEventsPayload.self),
    GoldenCase("payload-session-events-no-offset", SessionEventsPayload.self),
    GoldenCase(
      "payload-create-workstream-from-plan", CreateWorkstreamFromPlanPayload.self),
    GoldenCase("payload-control", ControlPayload.self),
    GoldenCase("payload-retry-issue", RetryIssuePayload.self),
    GoldenCase("payload-session-send", SessionSendPayload.self),
    GoldenCase("payload-interrupt", InterruptPayload.self),
    GoldenCase("payload-answer-ui-request", AnswerUiRequestPayload.self),
    GoldenCase("response-workstream-id", WorkstreamId.self),
    GoldenCase("contract-errors", [ContractError].self)
  ]
}
