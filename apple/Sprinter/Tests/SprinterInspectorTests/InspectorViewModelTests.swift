import SprinterBackend
import SprinterContract
import SprinterExecution
import Testing

@testable import SprinterInspector

/// Inspector view-model tests against a FAKE ``Backend`` — deterministic and
/// offline (no live daemon/network). Each test is self-contained and polls to a
/// fixed point, so every one is green run **in isolation**
/// (`swift test --filter InspectorViewModelTests`), never only by parallel-suite
/// scheduling luck (the carried single-consumer lifecycle constraint).
@Suite("Inspector view model")
@MainActor
struct InspectorViewModelTests {
  private static let execution = InspectorFixtures.executionId

  /// The transcript builds from the scripted execution feed, and a diff-bearing edit
  /// tool call renders its diff off the reused transcript item.
  @Test("the transcript builds and a diff-bearing tool call renders its diff")
  func transcriptRendersDiff() async {
    let backend = InspectorFakeBackend(
      knownExecution: Self.execution,
      snapshot: InspectorFixtures.snapshotWithJobPullRequest(InspectorFixtures.jobPullRequest))
    let feed = WorkGraphResync(connect: { backend }, backoff: .noDelay)
    let model = InspectorViewModel(backend: backend, executionId: Self.execution)
    model.start(feed)

    #expect(await waitUntil { backend.executionFeedCount == 1 })
    backend.emit(
      .toolStarted(
        id: "t1",
        name: "Edit",
        input: .object([
          "file_path": .string("/src/a.swift"),
          "old_string": .string("old"),
          "new_string": .string("new")
        ])))
    backend.emit(.toolCompleted(id: "t1", output: .object([:]), isError: false))

    #expect(await waitUntil { model.transcript.transcript.items.count == 1 })
    let diff = editDiff(in: model)
    #expect(diff?.filePath == "/src/a.swift")
    #expect(
      diff?.lines == [
        DiffLine(kind: .removed, text: "old"),
        DiffLine(kind: .added, text: "new")
      ])

    model.stop()
    await backend.close()
  }

  /// A live `.jobChanged` delta flips the PR pane's `merged` without a refetch.
  @Test("a live .jobChanged delta flips the pane's merged")
  func jobChangedFlipsMerged() async {
    let backend = InspectorFakeBackend(
      knownExecution: Self.execution,
      snapshot: InspectorFixtures.snapshotWithJobPullRequest(InspectorFixtures.jobPullRequest))
    let feed = WorkGraphResync(connect: { backend }, backoff: .noDelay)
    let model = InspectorViewModel(backend: backend, executionId: Self.execution)
    model.start(feed)

    // Baseline: the PR is open but not merged.
    #expect(await waitUntil { model.pullRequest.pullRequest?.merged == false })

    // A live job delta carries the same PR now merged.
    let merged = PullRequestRef(
      number: InspectorFixtures.jobPullRequest.number,
      url: InspectorFixtures.jobPullRequest.url,
      merged: true)
    backend.emit(.jobChanged(InspectorFixtures.jobWithPullRequest(merged)))

    #expect(await waitUntil { model.pullRequest.pullRequest?.merged == true })

    model.stop()
    await backend.close()
  }

  /// A live `.issueChanged` delta flips `merged` when the PR is resolved transitively
  /// off the issue.
  @Test("a live .issueChanged delta flips the pane's merged")
  func issueChangedFlipsMerged() async {
    let backend = InspectorFakeBackend(
      knownExecution: Self.execution,
      snapshot: InspectorFixtures.snapshotWithIssuePullRequest(InspectorFixtures.issuePullRequest))
    let feed = WorkGraphResync(connect: { backend }, backoff: .noDelay)
    let model = InspectorViewModel(backend: backend, executionId: Self.execution)
    model.start(feed)

    #expect(await waitUntil { model.pullRequest.pullRequest?.merged == false })

    let mergedIssue = Issue(
      id: InspectorFixtures.issueId,
      epicId: EpicId(rawValue: "epic-1"),
      number: 7,
      title: "Inspector",
      status: .done,
      dependsOn: [],
      pullRequest: PullRequestRef(
        number: InspectorFixtures.issuePullRequest.number,
        url: InspectorFixtures.issuePullRequest.url,
        merged: true))
    backend.emit(.issueChanged(mergedIssue))

    #expect(await waitUntil { model.pullRequest.pullRequest?.merged == true })

    model.stop()
    await backend.close()
  }

  /// `apply` re-resolves the pane directly (the pure main-actor core) and surfaces
  /// the "no PR yet" state and the execution↔PR link both ways.
  @Test("apply resolves the pane and links execution ↔ PR both ways")
  func applyResolvesAndLinks() {
    let model = InspectorViewModel(
      backend: InspectorFakeBackend(
        knownExecution: Self.execution,
        snapshot: Snapshot(
          repositories: [], workstreams: [], epics: [], issues: [], jobs: [], executions: [],
          agents: [],
          generation: StoreGenerationId(rawValue: "gen-test"))),
      executionId: Self.execution)

    // Before any snapshot the pane is unresolved.
    #expect(model.pullRequest.state == .unresolved)

    // A snapshot whose job has no PR yet → the "no PR yet" state, issue identified.
    model.apply(InspectorFixtures.snapshotWithJobPullRequest(nil))
    #expect(model.pullRequest.state == .awaitingPullRequest)
    #expect(model.pullRequest.issueId == InspectorFixtures.issueId)

    // Link both ways: the pane names the execution, and the transcript drives it too.
    #expect(model.pullRequest.executionId == model.executionId)
    #expect(model.transcript.executionId == model.executionId)
  }

  /// `start` is idempotent (the single-consumer work-graph feed is not respun and the
  /// transcript feed is not re-subscribed); `stop` then `start` on a fresh feed
  /// re-consumes.
  @Test("start is idempotent; stop then start on a fresh feed re-consumes")
  func lifecycleIsIdempotent() async {
    let backend = InspectorFakeBackend(
      knownExecution: Self.execution,
      snapshot: InspectorFixtures.snapshotWithJobPullRequest(InspectorFixtures.jobPullRequest))
    let feed = WorkGraphResync(connect: { backend }, backoff: .noDelay)
    let model = InspectorViewModel(backend: backend, executionId: Self.execution)

    model.start(feed)
    // A second start while running is a no-op — the transcript feed is not
    // re-subscribed (single-consumer).
    model.start(feed)
    #expect(await waitUntil { backend.executionFeedCount == 1 })
    #expect(await waitUntil { model.pullRequest.pullRequest != nil })
    #expect(model.transcript.lifecycle == .live)
    #expect(backend.executionFeedCount == 1)

    model.stop()
    model.stop()  // idempotent
    #expect(model.transcript.lifecycle == .ended)

    // Reconnect: the PR pane consumes a FRESH work-graph feed (its own backend,
    // reconnected per attempt), while the transcript re-subscribes a fresh execution
    // feed on its original long-lived backend (the execution channel is bound at
    // construction — count 1 → 2).
    let backend2 = InspectorFakeBackend(
      knownExecution: Self.execution,
      snapshot: InspectorFixtures.snapshotWithJobPullRequest(InspectorFixtures.jobPullRequest))
    let feed2 = WorkGraphResync(connect: { backend2 }, backoff: .noDelay)
    model.start(feed2)
    #expect(await waitUntil { backend.executionFeedCount == 2 })
    #expect(model.transcript.lifecycle == .live)
    #expect(await waitUntil { model.pullRequest.pullRequest != nil })

    model.stop()
    await backend.close()
    await backend2.close()
  }

  /// The first recognized edit tool call's diff in the model's transcript.
  private func editDiff(in model: InspectorViewModel) -> TranscriptDiff? {
    for item in model.transcript.transcript.items {
      if case .toolCall(let call) = item, let diff = call.fileDiff {
        return diff
      }
    }
    return nil
  }

  /// Polls on the main actor until `predicate` holds, yielding between checks so the
  /// feed tasks can run. Returns `false` if the bound is exhausted.
  private func waitUntil(_ predicate: () -> Bool) async -> Bool {
    for _ in 0..<100_000 {
      if predicate() { return true }
      await Task.yield()
    }
    return false
  }
}
