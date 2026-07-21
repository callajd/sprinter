import SprinterContract

/// What a reconnecting ``WorkGraphResync`` attempt would resume FROM: the latest
/// published baseline-consistent ``Snapshot`` plus the last-applied
/// contiguous-prefix offset.
///
/// The two are ONE value because they are only ever meaningful together. The offset
/// is a cursor INTO the daemon's durable log, and the snapshot is the state produced
/// by applying everything up to it — and it is also what CARRIES the store generation
/// the cursor is a coordinate in (``Snapshot/generation``), which a resume must send
/// with the cursor for the daemon to validate it at all; resuming means asking for the deltas strictly
/// after the cursor and folding them onto that snapshot. Either one alone is not a
/// resume point — a cursor with no baseline has nothing to fold onto, and a baseline
/// with no cursor cannot say where to continue from — so ``resumable`` yields a point
/// only when BOTH are present, and a first connect (or a drop before any baseline was
/// published) falls back to subscribe-around-snapshot.
///
/// Keeping them together is also what makes ``discard()`` correct by construction.
/// When the daemon reports
/// ``ContractError/resyncRequired(sinceOffset:maxOffset:generation:)`` its store was
/// dropped and recreated, so the cursor names a coordinate space that no longer exists
/// AND the baseline describes entities the reset destroyed. BOTH
/// must go: clearing only the cursor would leave the engine folding fresh deltas onto
/// a phantom baseline it can never correct, because the delta model is upsert-only
/// (there is no `*Removed` variant) — nothing the daemon can send removes a stale
/// entity, and only a fresh `snapshot()` replaces it.
struct ResumePoint: Sendable, Equatable {
  /// The latest published baseline-consistent state, retained so a reconnect resumes
  /// incrementally by folding onto it (never re-deriving from a fresh snapshot).
  private var state: Snapshot?
  /// The last-applied contiguous-prefix offset — the `sinceOffset` cursor a reconnect
  /// resumes strictly after. Contiguous, never max-seen (see
  /// ``ContiguousOffsetTracker``).
  private var offset: Int?

  /// The incremental-resume point, or `nil` when there is nothing to resume from.
  ///
  /// A recorded offset of `0` is deliberately NOT resumable. It is a real state — the
  /// cursor is the CONTIGUOUS prefix, so an attempt whose first applied delta arrived
  /// out of order records a baseline while the prefix is still `0` — but it is a resume
  /// that would ask the daemon for "everything strictly after nothing", i.e. exactly the
  /// origin, while carrying a retained baseline the daemon would then NOT refresh. There
  /// is nothing to gain from it and a whole class of confusion to lose, so this client
  /// takes the subscribe-around-`snapshot()` path instead.
  ///
  /// This is belt-and-braces, not the guard. The daemon compares the generation on EVERY
  /// present ``SprinterContract/ResumeContext``, offset `0` included, so a stale
  /// zero-cursor resume is refused there even if some future call site builds one; this
  /// simply means the canonical client never sends the shape at all.
  var resumable: (state: Snapshot, offset: Int)? {
    guard let state, let offset, offset > 0 else { return nil }
    return (state, offset)
  }

  /// Records the applied state and contiguous-prefix cursor so they survive to the
  /// next attempt. A `nil` `contiguous` records the baseline WITHOUT moving the
  /// cursor — the first-connect case, where a snapshot has been published but no
  /// delta applied yet, so there is no offset to resume strictly after.
  mutating func record(state: Snapshot, contiguous: Int?) {
    self.state = state
    if let contiguous {
      offset = contiguous
    }
  }

  /// Drops BOTH halves, so the next attempt subscribes around a fresh `snapshot()`
  /// instead of resuming (see the type docstring on why it must be both).
  mutating func discard() {
    state = nil
    offset = nil
  }
}
