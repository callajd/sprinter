import SprinterBackend
import SprinterContract
import SprinterSession
import SwiftUI

/// Thin planner View (CE3.1): renders `PlannerViewModel` — the reused session transcript
/// plus the one explicit materialize step. Planning is just an interactive session (D9),
/// so this reuses `SessionView` for the conversation and adds the plan form + the
/// reflected outcome. No planner logic here; it lives in the tested view model.
struct PlannerView: View {
  @Bindable var model: PlannerViewModel

  @State private var actionError: String?

  var body: some View {
    VStack(spacing: 0) {
      SessionView(model: model.session)
      Divider()
      planForm
    }
    .frame(minWidth: 480, minHeight: 420)
    .task { model.session.start() }
    .onDisappear { model.session.stop() }
    .shellActionErrorAlert($actionError)
  }

  private var planForm: some View {
    VStack(alignment: .leading, spacing: 8) {
      TextField("Workstream name", text: $model.name)
        .textFieldStyle(.roundedBorder)
      TextField("Repository owner", text: $model.owner)
        .textFieldStyle(.roundedBorder)
      TextField("Repository name", text: $model.repositoryName)
        .textFieldStyle(.roundedBorder)
      // A repository key the daemon's schema would refuse is caught HERE, with a
      // message naming the field and the remedy — the wire rejection for the same
      // input is an opaque contract decode failure.
      if let problem = model.repositoryProblem {
        Text(problem).font(.caption).foregroundStyle(.red)
      }
      TextField("Spec", text: $model.spec)
        .textFieldStyle(.roundedBorder)
      HStack {
        Button("Materialize") { materialize() }
          .disabled(!model.canMaterialize)
        outcomeLabel
      }
    }
    .padding()
  }

  @ViewBuilder
  private var outcomeLabel: some View {
    switch model.outcome {
    case .idle:
      EmptyView()
    case .materializing:
      ProgressView()
    case .created(let workstreamId):
      Text("Created \(workstreamId.rawValue)").foregroundStyle(.green)
    case .rejected(let reason):
      Text(reason).foregroundStyle(.red)
    }
  }

  private func materialize() {
    // The plan is constructed from the explicit form fields in the tested view model
    // (`draftPlan`); this View only triggers it. A domain rejection reflects into
    // `model.outcome` (.rejected) and is shown by `outcomeLabel`; a transport-level
    // failure resets `outcome` to `.idle` and rethrows, so surface that here instead
    // of dropping it.
    runShellAction(onError: { actionError = $0 }, action: { try await model.materializeDraft() })
  }
}

/// Opens a `PlannerViewModel` for a planning session the user names, then renders the
/// planner. Spawning a planning session server-side is later shell/product flow (CE3.2);
/// CE3.1 renders the model for a supplied planning session id.
struct PlannerContainer: View {
  let backend: any Backend

  @State private var sessionText = ""
  @State private var planner: PlannerViewModel?

  var body: some View {
    if let planner {
      PlannerView(model: planner)
    } else {
      startForm
    }
  }

  private var startForm: some View {
    VStack(spacing: 12) {
      Text("Start a planning session").font(.headline)
      TextField("Planning session id", text: $sessionText)
        .textFieldStyle(.roundedBorder)
      Button("Open") {
        planner = PlannerViewModel(
          backend: backend, planningSessionId: SessionId(rawValue: sessionText))
      }
      .disabled(sessionText.isEmpty)
    }
    .padding()
    .frame(minWidth: 380, minHeight: 200)
  }
}
