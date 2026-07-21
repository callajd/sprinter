import SprinterContract

/// The resolution state of the PR an execution produced (BE4.1).
///
/// There is no direct Execution→PR field, so the pane can be in one of three
/// first-class states — each is a real, renderable outcome, never a crash:
/// - ``unresolved`` — the execution's job is not (yet) in the snapshot, so no PR can
///   be named;
/// - ``awaitingPullRequest`` — the job is resolved but has produced no PR yet
///   ("no PR yet");
/// - ``open(_:)`` — the ``PullRequestRef`` the execution produced.
public enum PullRequestPaneState: Equatable, Sendable {
  case unresolved
  case awaitingPullRequest
  case open(PullRequestRef)
}

/// The **PR pane** state paired with a transcript in the inspector (BE4.1): the
/// execution↔PR linkage surfaced as observable state.
///
/// The pane identifies the execution it belongs to and the issue the work resolved to
/// (when known), so the link is reachable *from the PR side* (the transcript's
/// ``SprinterExecution/ExecutionViewModel`` already carries the `executionId` for the
/// other direction). Its ``state`` reflects the resolved PR — updated live as
/// `.jobChanged`/`.issueChanged` deltas fold onto the snapshot, so `merged` flips
/// with no manual refetch.
public struct PullRequestPane: Equatable, Sendable {
  /// The execution this pane pairs with (the execution↔PR link, PR → execution).
  public let executionId: ExecutionId
  /// The issue the execution's job resolved to, when the job is in the snapshot.
  public let issueId: IssueId?
  /// The resolved PR state.
  public let state: PullRequestPaneState

  public init(executionId: ExecutionId, issueId: IssueId?, state: PullRequestPaneState) {
    self.executionId = executionId
    self.issueId = issueId
    self.state = state
  }

  /// The resolved PR, when one is open; `nil` while unresolved or awaiting.
  public var pullRequest: PullRequestRef? {
    if case .open(let pullRequest) = state { return pullRequest }
    return nil
  }
}
