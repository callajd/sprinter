import SprinterBackend
import SprinterContract
import SprinterSession
import SwiftUI

/// Thin planner View (CE3.1): renders `PlannerViewModel` — the reused session transcript
/// plus the one explicit materialize step. Planning is just an interactive session (D9),
/// so this reuses `SessionView` for the conversation and adds the plan form + the
/// reflected outcome. No planner logic here; it lives in the tested view model.
struct PlannerView: View {
  let model: PlannerViewModel

  @State private var name = ""
  @State private var repo = ""
  @State private var spec = ""

  var body: some View {
    VStack(spacing: 0) {
      SessionView(model: model.session)
      Divider()
      planForm
    }
    .frame(minWidth: 480, minHeight: 420)
    .task { model.session.start() }
    .onDisappear { model.session.stop() }
  }

  private var planForm: some View {
    VStack(alignment: .leading, spacing: 8) {
      TextField("Workstream name", text: $name)
        .textFieldStyle(.roundedBorder)
      TextField("Repository (owner/name)", text: $repo)
        .textFieldStyle(.roundedBorder)
      TextField("Spec", text: $spec)
        .textFieldStyle(.roundedBorder)
      HStack {
        Button("Materialize") { materialize() }
          .disabled(name.isEmpty || repo.isEmpty)
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
    let plan = WorkstreamPlan(name: name, repo: repo, spec: spec)
    Task { try? await model.materialize(plan) }
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
