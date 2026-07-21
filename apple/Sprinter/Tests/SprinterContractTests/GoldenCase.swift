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
/// The list is the SHARED one, not a second opinion: `Golden.decode` checks the type each
/// decode-side call site asks for against this table
/// (``GoldenCase/requireDeclaredType(_:for:sourceLocation:)``), so the two directions
/// cannot drift apart one entry at a time.
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
  /// The paired mirror type's identity, kept alongside the erased closure so the pairing
  /// is CHECKABLE from outside — see ``requireDeclaredType(_:for:)``. Stored as an
  /// `ObjectIdentifier` (and a name for the message) rather than a metatype, so the case
  /// stays `Sendable` without depending on a metatype's conformance.
  let typeIdentity: ObjectIdentifier
  /// The paired type, spelled for a failure message.
  let typeName: String

  var testDescription: String { name }

  init<T: Codable & Sendable>(_ name: String, _ type: T.Type) {
    self.name = name
    self.divergence = { data in try Golden.encodeDivergence(type, in: data) }
    self.typeIdentity = ObjectIdentifier(type)
    self.typeName = String(describing: type)
  }
}

extension GoldenCase {
  /// The table, keyed by golden name — the ONE place a golden's mirror type is written
  /// down, shared by both directions of the harness.
  ///
  /// Duplicate names keep the first entry rather than trapping: a duplicate is a real
  /// defect, but it is ``EncodeAgreementTests/everyGoldenIsCovered()``'s to REPORT (it
  /// counts the table against the bundle), and a `Dictionary(uniqueKeysWithValues:)` here
  /// would instead crash every test in the target before that count ever ran.
  private static let byName: [String: GoldenCase] = Dictionary(
    all.map { ($0.name, $0) }, uniquingKeysWith: { first, _ in first })

  /// Requires that a DECODE-side call site reads `name` as the same type this table pairs
  /// it with — the check that stops the pairing from being a per-golden escape hatch.
  ///
  /// `T.Type` is otherwise unconstrained: `GoldenCase("snapshot", JSONValue.self)` decodes
  /// any object, re-encodes to itself by construction, and passes the encode-agreement
  /// test vacuously while `everyGoldenIsCovered` still reports full coverage — so one
  /// inconvenient failure could be silenced without any test going red. Routing every
  /// `Golden.decode` through here makes the table the SINGLE declaration of a golden's
  /// type and asserts it from the other side: retyping a case to something the mirror
  /// tests do not decode it as fails those tests immediately, by name.
  ///
  /// A `name` absent from the table is not this check's business — it means a golden with
  /// no encode-agreement case at all, which `everyGoldenIsCovered` reports on its own.
  static func requireDeclaredType<T>(
    _ type: T.Type,
    for name: String,
    sourceLocation: SourceLocation = #_sourceLocation
  ) {
    guard let declared = byName[name] else { return }
    #expect(
      declared.typeIdentity == ObjectIdentifier(type),
      """
      \(name).json: GoldenCase pairs it with \(declared.typeName), but this test decodes \
      it as \(String(describing: type)). The encode-agreement harness checks the type the \
      TABLE names — if that is not the mirror type this golden represents, the encode \
      check for \(name) is vacuous.
      """,
      sourceLocation: sourceLocation
    )
  }

  /// Every committed golden, with its mirror type.
  ///
  /// **D3 — scope.** The decisive coverage is every mirrored type carrying at least one
  /// `Schema.optionalKey` field, because that is the only place omission-vs-`null` can
  /// diverge — and it is decisive only while some golden actually OMITS each such field.
  ///
  /// That property is NOT asserted here, and deliberately not written down here either: a
  /// hand-kept list of "the optional fields and the fixtures covering them" is prose, and
  /// prose does not fail a build when DE2 adds a mirrored type whose fixture happens to
  /// populate its new optional field. It is enforced at FIELD granularity by
  /// `scripts/golden-coverage.ts`, which reads the optional keys and tagged-union cases
  /// off the TypeScript schemas themselves and fails `bun run check:goldens` unless each
  /// optional key has one golden that carries it and one that omits it, and each union
  /// case appears somewhere. This table's job is the complementary one: that every golden
  /// so produced is actually CHECKED in the encode direction, which
  /// ``EncodeAgreementTests/everyGoldenIsCovered()`` asserts.
  ///
  /// The table is not restricted to optional-bearing types: every golden is checked,
  /// including the types with no optional field and the decode-only
  /// value types (`Timestamp`-shaped strings, `CommitSha`, `BranchName` — in scope for
  /// ENCODE SHAPE, out of scope for validation), because the containers that embed the
  /// optional-bearing types are where a nesting mistake would actually show up.
  static let all: [GoldenCase] = readModel + registry + executionModel + commands

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
    GoldenCase("execution", Execution.self),
    GoldenCase("execution-child", Execution.self),
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

  /// The execution channel: its events, transcript records and neutral I/O types.
  private static let executionModel: [GoldenCase] = [
    GoldenCase("execution-events", [ExecutionEvent].self),
    GoldenCase("offset-execution-events", [OffsetExecutionEvent].self),
    GoldenCase("transcript-entries", [TranscriptEntry].self),
    GoldenCase("usages", [Usage].self),
    GoldenCase("execution-inputs", [ExecutionInput].self),
    GoldenCase("ui-responses", [UiResponse].self),
    GoldenCase("json-values", [JSONValue].self)
  ]

  /// The command payloads, the one command response, and the contract's error channel.
  private static let commands: [GoldenCase] = [
    GoldenCase("workstream-plan", WorkstreamPlan.self),
    GoldenCase("control-actions", [ControlAction].self),
    GoldenCase("payload-events", EventsPayload.self),
    GoldenCase("payload-events-no-offset", EventsPayload.self),
    GoldenCase("payload-execution-events", ExecutionEventsPayload.self),
    GoldenCase("payload-execution-events-no-offset", ExecutionEventsPayload.self),
    GoldenCase(
      "payload-create-workstream-from-plan", CreateWorkstreamFromPlanPayload.self),
    GoldenCase("payload-control", ControlPayload.self),
    GoldenCase("payload-retry-issue", RetryIssuePayload.self),
    GoldenCase("payload-execution-send", ExecutionSendPayload.self),
    GoldenCase("payload-interrupt", InterruptPayload.self),
    GoldenCase("payload-answer-ui-request", AnswerUiRequestPayload.self),
    GoldenCase("response-workstream-id", WorkstreamId.self),
    GoldenCase("contract-errors", [ContractError].self)
  ]
}
