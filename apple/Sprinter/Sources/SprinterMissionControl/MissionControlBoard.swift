import Observation
import SprinterBackend
import SprinterContract

/// The Mission Control **board view model** (BE2.1): the cross-repo
/// `Workstream ⊃ Epic ⊃ Issue` board, rendered from a baseline-consistent snapshot
/// and kept live off BE1's ``WorkGraphResync`` feed.
///
/// It is `@Observable @MainActor`: the SwiftUI shell (convergence, not this epic)
/// observes ``workstreams`` and re-renders when it changes, and every mutation
/// happens on the main actor so the view sees a consistent tree. The view model
/// owns no transport and no localness — it is driven purely by
/// ``WorkGraphResync/states()`` (INV-PORT: the port-based feed), and it consumes
/// the mirrored ``Snapshot`` DTO unchanged (INV-CONTRACT).
///
/// Projection is delegated to the pure ``BoardProjection``; the view model adds
/// only the main-actor state and the feed-consumption lifecycle. Each published
/// snapshot fully replaces the board (snapshot-then-live, D4), so a status flip in
/// a new snapshot is reflected with no incremental-patch logic here.
@Observable
@MainActor
public final class MissionControlBoard {
  /// The rendered board: repo-scoped workstreams, each with its epics and issues.
  public private(set) var workstreams: [BoardWorkstream] = []

  /// The running feed-consumption task, if any. Ignored by observation — only
  /// ``workstreams`` drives the view.
  @ObservationIgnored private var driver: Task<Void, Never>?

  public init() {}

  /// Projects one baseline-consistent snapshot onto the board, replacing the prior
  /// tree. The pure, synchronous core — directly testable and reused by ``consume``.
  public func apply(_ snapshot: Snapshot) {
    workstreams = BoardProjection.project(snapshot)
  }

  /// Consumes the port-based feed to completion, projecting each published
  /// baseline-consistent snapshot onto the board on the main actor. Returns when
  /// the feed finishes or the surrounding task is cancelled.
  public func consume(_ feed: WorkGraphResync) async {
    for await snapshot in await feed.states() {
      apply(snapshot)
    }
  }

  /// Starts consuming `feed` in a detached-from-caller task (retained for
  /// ``stop``). **Idempotent while already running: a second call is a no-op** — it
  /// does NOT cancel-and-respin. ``WorkGraphResync`` is single-consumer (its
  /// `states()` yields the live stream only once), so re-consuming a feed already
  /// being consumed would blank the board. A board therefore consumes ONE feed for
  /// its running lifetime; to reconnect, ``stop()`` then `start(freshFeed)` with a
  /// **freshly-constructed** ``WorkGraphResync`` (never a reused one).
  public func start(_ feed: WorkGraphResync) {
    guard driver == nil else { return }
    driver = Task { [weak self] in
      await self?.consume(feed)
    }
  }

  /// Stops the feed-consumption task started by ``start``. Idempotent.
  public func stop() {
    driver?.cancel()
    driver = nil
  }
}
