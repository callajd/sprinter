import SprinterBackend
import SprinterContract
import Testing

@testable import SprinterSession

/// Tests for the planner view model (BE3.2) against a FAKE, deterministic, offline
/// `Backend`: planning runs as a normal interactive session (the reused
/// `SessionViewModel`'s transcript builds), and the distinct materialize step
/// submits the `WorkstreamPlan` through the port, reflecting the created
/// `WorkstreamId` or the mirrored `PlanRejected` reason. No daemon, no network.
@Suite("Planner view model")
@MainActor
struct PlannerViewModelTests {
  private static let session = SessionId(rawValue: "plan-session")
  private static let repositoryKey = RepositoryKey(host: .github, owner: "acme", name: "pipe")
  private static let plan = WorkstreamPlan(
    name: "Postgres sink", repository: repositoryKey, spec: "batch writes to Postgres")

  /// Planning IS a normal interactive session: the reused `SessionViewModel`'s
  /// transcript builds live off the scripted planning feed, driven by the fake.
  @Test("planning runs as an interactive session — the transcript builds")
  func planningSessionTranscriptBuilds() async throws {
    let backend = PlannerFakeBackend(
      knownSession: Self.session, materializeResult: .success(WorkstreamId(rawValue: "ws-1")))
    let planner = PlannerViewModel(backend: backend, planningSessionId: Self.session)
    planner.session.start()

    backend.emit(.turnStarted)
    backend.emit(.messageStarted(messageId: "m1"))
    backend.emit(.messageDelta(messageId: "m1", text: "Let's plan", reasoning: nil))

    #expect(await waitUntil { planner.session.transcript.items.map(\.id) == ["message:m1"] })
    #expect(planner.session.transcript.isTurnActive)
    // No materialize yet — the planner starts idle.
    #expect(planner.outcome == .idle)
    #expect(planner.createdWorkstreamId == nil)

    planner.session.stop()
    await backend.close()
  }

  /// The materialize step submits the exact `WorkstreamPlan` through the port and,
  /// on success, reflects the created `WorkstreamId` (so the shell can navigate).
  @Test("materialize submits the plan and reflects the created WorkstreamId")
  func materializeCreatesWorkstream() async throws {
    let created = WorkstreamId(rawValue: "ws-42")
    let backend = PlannerFakeBackend(
      knownSession: Self.session, materializeResult: .success(created))
    let planner = PlannerViewModel(backend: backend, planningSessionId: Self.session)
    var observed = backend.submittedPlans.makeAsyncIterator()

    try await planner.materialize(Self.plan)

    // The fake observed exactly the submitted plan payload.
    #expect(await observed.next() == Self.plan)
    // The outcome reflects the new workstream id.
    #expect(planner.outcome == .created(created))
    #expect(planner.createdWorkstreamId == created)
    #expect(planner.rejectionReason == nil)

    await backend.close()
  }

  /// A `materialize` issued while one is already in flight is a no-op: the guard
  /// returns before a second `createWorkstreamFromPlan`, so a double-submit can't
  /// race the reflected `outcome` or create a duplicate workstream.
  @Test("a re-entrant materialize while one is in flight is a no-op")
  func reentrantMaterializeIsNoOp() async throws {
    let created = WorkstreamId(rawValue: "ws-once")
    let backend = PlannerFakeBackend(
      knownSession: Self.session, materializeResult: .success(created), gated: true)
    let planner = PlannerViewModel(backend: backend, planningSessionId: Self.session)

    // First call: enters materialize, sets `.materializing`, then suspends on the gate.
    let first = Task { try await planner.materialize(Self.plan) }
    #expect(await waitUntil { planner.outcome == .materializing })

    // Second call WHILE the first is in flight — the guard makes it a no-op (no
    // second port call, `outcome` untouched).
    try await planner.materialize(Self.plan)
    #expect(backend.submissionCount == 1)
    #expect(planner.outcome == .materializing)

    // Release the first; it resolves to the created workstream — still one submission.
    backend.releaseGate()
    try await first.value
    #expect(planner.outcome == .created(created))
    #expect(planner.createdWorkstreamId == created)
    #expect(backend.submissionCount == 1)

    await backend.close()
  }

  /// A rejected plan surfaces the mirrored `PlanRejected` reason for correction and
  /// retry — reflected into the outcome, not thrown at the caller.
  @Test("materialize surfaces the mirrored PlanRejected reason")
  func materializeSurfacesPlanRejected() async throws {
    let backend = PlannerFakeBackend(
      knownSession: Self.session,
      materializeResult: .failure(.planRejected(reason: "repo not connected")))
    let planner = PlannerViewModel(backend: backend, planningSessionId: Self.session)

    try await planner.materialize(Self.plan)

    #expect(planner.outcome == .rejected(reason: "repo not connected"))
    #expect(planner.rejectionReason == "repo not connected")
    #expect(planner.createdWorkstreamId == nil)

    await backend.close()
  }

  /// An unexpected (non-`PlanRejected`) error from the port is NOT silently dropped:
  /// it is rethrown and the outcome resets to `.idle` so the caller can retry.
  @Test("materialize rethrows an unexpected port error and resets to idle")
  func materializeRethrowsUnexpectedError() async throws {
    let backend = PlannerFakeBackend(
      knownSession: Self.session,
      materializeResult: .failure(.sessionNotFound(id: Self.session)))
    let planner = PlannerViewModel(backend: backend, planningSessionId: Self.session)

    await #expect(throws: ContractError.sessionNotFound(id: Self.session)) {
      try await planner.materialize(Self.plan)
    }
    #expect(planner.outcome == .idle)

    await backend.close()
  }

  /// After a rejection, a corrected plan can be re-submitted (retry) and the outcome
  /// flips to `.created` — the reject → correct → retry loop.
  @Test("a corrected plan retries to success after a rejection")
  func rejectionThenRetrySucceeds() async throws {
    // First a rejecting backend, then a fresh accepting one (a corrected retry).
    let rejecting = PlannerFakeBackend(
      knownSession: Self.session, materializeResult: .failure(.planRejected(reason: "bad spec")))
    let planner = PlannerViewModel(backend: rejecting, planningSessionId: Self.session)

    try await planner.materialize(Self.plan)
    #expect(planner.outcome == .rejected(reason: "bad spec"))
    await rejecting.close()

    let created = WorkstreamId(rawValue: "ws-retry")
    let accepting = PlannerViewModel(
      backend: PlannerFakeBackend(
        knownSession: Self.session, materializeResult: .success(created)),
      planningSessionId: Self.session)
    let corrected = WorkstreamPlan(
      name: Self.plan.name, repository: Self.plan.repository, spec: "corrected spec")
    try await accepting.materialize(corrected)
    #expect(accepting.outcome == .created(created))
    #expect(accepting.createdWorkstreamId == created)
  }

  /// The plan-construction FORM (CE3.2): `canMaterialize` gates on a non-empty name
  /// AND a COMPLETE repository key — both halves, since the contract's key is a triple
  /// and a half-filled one names nothing. Whitespace-only fields don't count, and the
  /// plan is built explicitly from the fields, never extracted from the transcript.
  @Test("the form gates materialize on a non-empty name and a complete repository key")
  func formValidationGatesMaterialize() {
    let backend = PlannerFakeBackend(
      knownSession: Self.session, materializeResult: .success(WorkstreamId(rawValue: "ws-1")))
    let planner = PlannerViewModel(backend: backend, planningSessionId: Self.session)

    // Empty form → cannot materialize.
    #expect(!planner.canMaterialize)

    // Name only is not enough; a repository key is required too.
    planner.name = "Postgres sink"
    #expect(!planner.canMaterialize)

    // Whitespace-only fields still don't count.
    planner.owner = "   "
    planner.repositoryName = "   "
    #expect(!planner.canMaterialize)

    // HALF the key is still not a key: an owner with no repository name names nothing.
    planner.owner = "acme"
    planner.repositoryName = ""
    #expect(!planner.canMaterialize)

    // Name + the COMPLETE key (spec may stay empty) → ready.
    planner.repositoryName = "pipe"
    #expect(planner.canMaterialize)
  }

  /// `draftPlan` constructs the plan from the explicit fields, trimming surrounding
  /// whitespace/newlines so a stray trailing space never leaks into the submission.
  @Test("draftPlan constructs the plan from the trimmed form fields")
  func draftPlanTrimsFields() {
    let backend = PlannerFakeBackend(
      knownSession: Self.session, materializeResult: .success(WorkstreamId(rawValue: "ws-1")))
    let planner = PlannerViewModel(backend: backend, planningSessionId: Self.session)
    planner.name = "  Postgres sink\n"
    planner.owner = " acme "
    planner.repositoryName = " pipe "
    planner.spec = "  batch writes  "

    #expect(
      planner.draftPlan
        == WorkstreamPlan(
          name: "Postgres sink", repository: Self.repositoryKey, spec: "batch writes"))
  }

  /// `materializeDraft` submits the plan the form describes through the port and
  /// reflects the created workstream — the explicit, form-driven construction path.
  @Test("materializeDraft submits the form's plan and reflects the created workstream")
  func materializeDraftSubmitsFormPlan() async throws {
    let created = WorkstreamId(rawValue: "ws-form")
    let backend = PlannerFakeBackend(
      knownSession: Self.session, materializeResult: .success(created))
    let planner = PlannerViewModel(backend: backend, planningSessionId: Self.session)
    var observed = backend.submittedPlans.makeAsyncIterator()
    planner.name = "Postgres sink"
    planner.owner = "acme"
    planner.repositoryName = "pipe"
    planner.spec = "batch writes to Postgres"

    try await planner.materializeDraft()

    #expect(await observed.next() == Self.plan)
    #expect(planner.outcome == .created(created))
  }

  /// `materializeDraft` on an incomplete form is a no-op: no `createWorkstreamFromPlan`
  /// is issued and the outcome stays idle, so an empty name/repo can't be submitted.
  @Test("materializeDraft on an incomplete form is a no-op")
  func materializeDraftIncompleteIsNoOp() async throws {
    let backend = PlannerFakeBackend(
      knownSession: Self.session, materializeResult: .success(WorkstreamId(rawValue: "ws-1")))
    let planner = PlannerViewModel(backend: backend, planningSessionId: Self.session)
    planner.name = "Postgres sink"  // repo left empty → incomplete

    try await planner.materializeDraft()

    #expect(backend.submissionCount == 0)
    #expect(planner.outcome == .idle)

    await backend.close()
  }

  /// Polls the main-actor model until `predicate` holds, yielding so the feed task
  /// can run. Returns `false` if the bound is exhausted.
  private func waitUntil(_ predicate: () -> Bool) async -> Bool {
    for _ in 0..<100_000 {
      if predicate() { return true }
      await Task.yield()
    }
    return false
  }
}
