import SprinterBackend
import SprinterContract
import SprinterExecution
import SwiftUI

/// Thin planner View (CE3.1): renders `PlannerViewModel` — the reused execution transcript
/// plus the one explicit materialize step. Planning is just an interactive execution (D9),
/// so this reuses `ExecutionView` for the conversation and adds the plan form + the
/// reflected outcome. No planner logic here; it lives in the tested view model.
struct PlannerView: View {
  @Bindable var model: PlannerViewModel

  @State private var actionError: String?

  var body: some View {
    VStack(spacing: 0) {
      ExecutionView(model: model.execution)
      Divider()
      planForm
    }
    .frame(minWidth: 480, minHeight: 420)
    .task { model.execution.start() }
    .onDisappear { model.execution.stop() }
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

/// Opens a `PlannerViewModel` for a planning execution the user names, then renders the
/// planner. Spawning a planning execution server-side is later shell/product flow (CE3.2);
/// CE3.1 renders the model for a supplied planning execution id.
struct PlannerContainer: View {
  let backend: any Backend

  @State private var executionText = ""
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
      Text("Start a planning execution").font(.headline)
      TextField("Planning execution id", text: $executionText)
        .textFieldStyle(.roundedBorder)
      Button("Open") {
        planner = PlannerViewModel(
          backend: backend, planningExecutionId: ExecutionId(rawValue: executionText))
      }
      .disabled(executionText.isEmpty)
    }
    .padding()
    .frame(minWidth: 380, minHeight: 200)
  }
}
