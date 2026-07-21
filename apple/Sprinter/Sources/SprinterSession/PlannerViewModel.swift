import Foundation
import Observation
import SprinterBackend
import SprinterContract

/// The reflected result of the planner's materialize step (BE3.2): the planning
/// session's product is either turned into a new workstream or rejected.
///
/// It is the observable spine the shell reads — `.created` carries the new
/// ``WorkstreamId`` to navigate to; `.rejected` carries the mirrored
/// ``ContractError/planRejected(reason:)`` reason for correction and retry.
public enum PlanningOutcome: Equatable, Sendable {
  /// No materialize attempted yet (or reset for a retry after a correction).
  case idle
  /// A ``PlannerViewModel/materialize(_:)`` call is in flight.
  case materializing
  /// The plan materialized — the created workstream's id (navigate to its board).
  case created(WorkstreamId)
  /// The daemon rejected the plan — the reason to surface for correction/retry.
  case rejected(reason: String)
}

/// The **planner view model** (BE3.2 / D9): planning is just a normal interactive
/// session whose product materializes into the work graph.
///
/// It is `@Observable @MainActor` and it does NOT re-solve the interactive-session
/// surface — it **reuses** BE3.1's ``SessionViewModel`` (the live transcript,
/// `send`/`interrupt`, the inline `extension_ui_request` round-trip, and the
/// idempotent single-consumer feed lifecycle) for a FRESH planning session. The
/// planner adds only one thing on top: the distinct, explicit **materialize** step
/// that submits a ``WorkstreamPlan`` — the plan the shell constructs from the planning
/// conversation (name/repo/spec); the contract carries no structured "plan produced"
/// event, so plan construction is the shell's job, not an extraction seam here — through
/// the ``Backend`` port's ``Backend/createWorkstreamFromPlan(_:)``, reflecting the
/// result into ``outcome``.
///
/// It owns no transport and no localness — it depends only on the ``Backend`` port
/// (INV-PORT) and carries only frozen ``SprinterContract`` DTOs (INV-CONTRACT).
@Observable
@MainActor
public final class PlannerViewModel {
  /// The reused interactive-session surface (BE3.1). The view renders planning as a
  /// normal session through this: ``SessionViewModel/transcript``,
  /// ``SessionViewModel/send(_:)`` / ``SessionViewModel/interrupt()``,
  /// ``SessionViewModel/outstandingRequests`` + ``SessionViewModel/answer(requestId:_:)``,
  /// and the ``SessionViewModel/lifecycle``.
  public let session: SessionViewModel

  /// The plan-construction **form** (CE3.2): the explicit name/repo/spec the user
  /// fills in from the planning conversation. There is deliberately NO
  /// transcript→plan auto-extractor — the contract carries no structured "plan
  /// produced" event, so the shell constructs the plan from these explicit fields.
  /// The view binds `TextField`s to them; ``canMaterialize`` and ``draftPlan`` are
  /// derived, so the construction/validation stays here (tested), not in the View.
  public var name = ""
  /// The repository OWNER the plan names (e.g. `callajd`) — half of the natural key.
  ///
  /// The owner and the repository name are SEPARATE fields rather than one
  /// `owner/name` string because the contract's ``RepositoryKey`` is a triple, and
  /// splitting a user-typed string on `/` here would put a SECOND parser of that
  /// syntax in the tree — one the daemon never sees and cannot validate.
  public var owner = ""
  /// The repository NAME the plan names (e.g. `sprinter`) — the other half of the key.
  public var repositoryName = ""
  public var spec = ""

  /// The reflected materialize result: `.idle` until the first ``materialize(_:)``,
  /// then `.materializing`, then `.created(id)` or `.rejected(reason:)`.
  public private(set) var outcome: PlanningOutcome = .idle

  /// The `Backend` port used for the materialize step. Ignored by observation —
  /// only ``outcome`` (and the reused ``session``) drives the view.
  @ObservationIgnored private let backend: any Backend

  public init(backend: any Backend, planningSessionId: SessionId) {
    self.backend = backend
    self.session = SessionViewModel(backend: backend, sessionId: planningSessionId)
  }

  /// The plan the form currently describes — the explicit name/repository/spec, each
  /// trimmed of surrounding whitespace/newlines so a stray trailing space never
  /// leaks into the submitted ``WorkstreamPlan``.
  public var draftPlan: WorkstreamPlan {
    WorkstreamPlan(
      name: name.trimmed,
      // The plan names the repository by its NATURAL KEY: this client has never seen
      // a RepositoryId and cannot mint one. The daemon resolves the key through its
      // code-host port, and refuses a repository the host does not know.
      repository: RepositoryKey(host: .github, owner: owner.trimmed, name: repositoryName.trimmed),
      spec: spec.trimmed)
  }

  /// Whether the form is complete enough to materialize: a plan needs a non-empty
  /// name AND a complete repository key (the spec may be empty — a bare workstream is
  /// valid). The
  /// view disables the materialize control off this, and ``materializeDraft()``
  /// guards on it too.
  public var canMaterialize: Bool {
    !name.trimmed.isEmpty && !owner.trimmed.isEmpty && !repositoryName.trimmed.isEmpty
  }

  /// Materializes the plan the FORM currently describes — the explicit,
  /// shell-constructed ``draftPlan``. A no-op when the form is incomplete
  /// (``canMaterialize`` is `false`), so an empty name/repo can never be submitted.
  /// Delegates to ``materialize(_:)`` for the reflected-outcome + re-entrancy
  /// semantics.
  public func materializeDraft() async throws {
    guard canMaterialize else { return }
    try await materialize(draftPlan)
  }

  /// The created workstream's id once the plan has materialized, else `nil` — the
  /// convenience the shell reads to navigate to the new board.
  public var createdWorkstreamId: WorkstreamId? {
    guard case .created(let id) = outcome else { return nil }
    return id
  }

  /// The rejection reason once a plan has been rejected, else `nil` — surfaced for
  /// correction and retry.
  public var rejectionReason: String? {
    guard case .rejected(let reason) = outcome else { return nil }
    return reason
  }

  /// Materializes the planning session's product into the work graph — the
  /// distinct, explicit step off the session.
  ///
  /// Submits `plan` through the port's ``Backend/createWorkstreamFromPlan(_:)`` and
  /// reflects the result into ``outcome``: `.created(id)` on success (exposing the
  /// new ``WorkstreamId``) or `.rejected(reason:)` for the mirrored
  /// ``ContractError/planRejected(reason:)`` (surfacing the reason for retry). Any
  /// other (transport-level) error resets ``outcome`` to `.idle` and is rethrown —
  /// it is never silently dropped.
  ///
  /// Re-entrant calls are a no-op: a `materialize` while one is already in flight
  /// (``outcome`` == `.materializing`) returns immediately without issuing a second
  /// ``Backend/createWorkstreamFromPlan(_:)``, so a double-submit can't race the
  /// reflected ``outcome`` or create a duplicate workstream. The `@MainActor`
  /// isolation makes the `.materializing` guard-and-set atomic against the suspension.
  public func materialize(_ plan: WorkstreamPlan) async throws {
    guard outcome != .materializing else { return }
    outcome = .materializing
    do {
      let workstreamId = try await backend.createWorkstreamFromPlan(plan)
      outcome = .created(workstreamId)
    } catch ContractError.planRejected(let reason) {
      outcome = .rejected(reason: reason)
    } catch {
      outcome = .idle
      throw error
    }
  }
}

extension String {
  /// The string with surrounding whitespace and newlines removed — the form's
  /// field-normalisation, so a trailing space never leaks into a submitted plan.
  fileprivate var trimmed: String {
    trimmingCharacters(in: .whitespacesAndNewlines)
  }
}
