import SprinterAppSupport
import SprinterBackend
import SprinterContract
import SprinterExecution
import SprinterInspector
import SwiftUI

/// Thin inspector View (CE3.1): pairs the reused execution transcript with the PR pane.
/// `InspectorViewModel` owns both feeds (the execution channel and the work-graph feed that
/// keeps the PR pane live); this View renders them and owns the start/stop lifecycle for
/// the selection it is shown for. The PR pane is read declaratively — `merged` flips as
/// deltas fold in, with no monotonic assumption (the CE2 carried out-of-order constraint).
struct InspectorView: View {
  let model: InspectorViewModel
  let feed: WorkGraphResync

  var body: some View {
    HSplitView {
      ExecutionView(model: model.transcript)
        .frame(minWidth: 380)
      pullRequestPane
        .frame(minWidth: 260)
    }
    .task { model.start(feed) }
    .onDisappear { model.stop() }
  }

  @ViewBuilder
  private var pullRequestPane: some View {
    switch model.pullRequest.state {
    case .unresolved:
      ContentUnavailableView("No PR resolved yet", systemImage: "shippingbox")
    case .awaitingPullRequest:
      ContentUnavailableView("Awaiting the PR", systemImage: "clock")
    case .open(let ref):
      pullRequestDetail(ref)
    }
  }

  private func pullRequestDetail(_ ref: PullRequestRef) -> some View {
    VStack(alignment: .leading, spacing: 8) {
      Text("PR #\(ref.number)").font(.headline)
      Text(ref.url)
        .font(.caption)
        .textSelection(.enabled)
      Text(ref.merged ? "merged" : "open")
        .foregroundStyle(ref.merged ? .green : .secondary)
      Spacer()
    }
    .frame(maxWidth: .infinity, alignment: .leading)
    .padding()
  }
}

/// Builds the `InspectorViewModel` and its fresh work-graph feed once per selection (keyed
/// by the execution id upstream), so a new selection gets a new single-consumer feed rather
/// than re-consuming a spent one.
struct InspectorContainer: View {
  @State private var inspector: InspectorViewModel
  private let feed: WorkGraphResync

  init(model: AppModel, backend: any Backend, executionId: ExecutionId) {
    _inspector = State(initialValue: InspectorViewModel(backend: backend, executionId: executionId))
    self.feed = model.makeWorkGraphFeed()
  }

  var body: some View {
    InspectorView(model: inspector, feed: feed)
  }
}
