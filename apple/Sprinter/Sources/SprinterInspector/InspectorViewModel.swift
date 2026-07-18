import Observation
import SprinterBackend
import SprinterContract
import SprinterSession

/// The **Inspector view model** (BE4.1): given a ``SessionId``, it pairs the full
/// transcript the session produced with the PR that session produced — a
/// transcript pane + a PR pane + the session↔PR link.
///
/// It is `@Observable @MainActor` and platform-neutral (Foundation + Observation,
/// no AppKit/UIKit): the SwiftUI shell (convergence, not this epic) observes it and
/// re-renders. It owns no transport and no localness — everything flows over the
/// injected ``Backend`` port (INV-PORT) carrying only frozen ``SprinterContract``
/// DTOs (INV-CONTRACT).
///
/// Both sides are **reused, not re-solved**:
/// - the **transcript** is a wholesale ``SprinterSession/SessionViewModel`` — the
///   same `TranscriptProjection` fold (messages/reasoning, tool calls incl.
///   diff-bearing edits, notices/status/retry/compaction); the inspector adds no
///   parallel renderer, only the ``TranscriptToolCall/fileDiff`` transform over the
///   existing tool-call item;
/// - the **PR pane** is projected by the pure ``SessionPullRequestResolver`` off the
///   same ``WorkGraphResync`` snapshot feed BE2's board consumes, mirroring
///   ``MissionControlBoard``'s `consume`/`apply` lifecycle.
///
/// The PR pane is **live**: a PR opening/merging arrives as an `.issueChanged`/
/// `.jobChanged` ``WorkGraphEvent`` folded onto the snapshot, so ``pullRequest``'s
/// `merged` flips with no manual refetch. The work-graph feed is single-consumer, so
/// ``start(_:)``/``stop()`` are idempotent (a second start never cancel-and-respins;
/// a reconnect uses a freshly-constructed feed).
@Observable
@MainActor
public final class InspectorViewModel {
  /// The session this inspector pairs with — the session↔PR link's session side.
  public let sessionId: SessionId

  /// The reused interactive-session view model rendering the full transcript
  /// (messages, tool calls incl. diffs, thinking). Its `transcript` is the same
  /// `TranscriptProjection` fold BE3.1 ships — the inspector does NOT re-solve it.
  public let transcript: SessionViewModel

  /// The PR the session produced, resolved from the work-graph snapshot and kept
  /// live off the feed. Starts ``PullRequestPaneState/unresolved`` until the first
  /// snapshot arrives.
  public private(set) var pullRequest: PullRequestPane

  /// The running work-graph feed-consumption task. Ignored by observation — only
  /// ``pullRequest`` (and the transcript) drive the view.
  @ObservationIgnored private var driver: Task<Void, Never>?

  public init(backend: any Backend, sessionId: SessionId) {
    self.sessionId = sessionId
    self.transcript = SessionViewModel(backend: backend, sessionId: sessionId)
    self.pullRequest = PullRequestPane(sessionId: sessionId, issueId: nil, state: .unresolved)
  }

  /// Re-resolves the PR pane from one baseline-consistent snapshot, replacing the
  /// prior pane. The pure, synchronous core — directly testable and reused by
  /// ``consume``.
  public func apply(_ snapshot: Snapshot) {
    pullRequest = SessionPullRequestResolver.resolve(sessionId: sessionId, in: snapshot)
  }

  /// Consumes the port-based work-graph feed to completion, re-resolving the PR pane
  /// from each published snapshot on the main actor. Returns when the feed finishes
  /// or the surrounding task is cancelled.
  public func consume(_ feed: WorkGraphResync) async {
    for await snapshot in await feed.states() {
      apply(snapshot)
    }
  }

  /// Starts BOTH live feeds: the transcript's session feed and the work-graph feed
  /// that keeps the PR pane current. **Idempotent** — the transcript's `start` is a
  /// no-op while already subscribed, and the work-graph driver is not respun while
  /// running (``WorkGraphResync`` is single-consumer; re-consuming it would blank the
  /// pane). To reconnect, ``stop()`` then `start(freshFeed)` with a freshly
  /// constructed ``WorkGraphResync``.
  public func start(_ feed: WorkGraphResync) {
    transcript.start()
    guard driver == nil else { return }
    driver = Task { [weak self] in
      await self?.consume(feed)
    }
  }

  /// Stops both feeds (the work-graph consumer and the transcript subscription).
  /// Idempotent.
  public func stop() {
    driver?.cancel()
    driver = nil
    transcript.stop()
  }
}
