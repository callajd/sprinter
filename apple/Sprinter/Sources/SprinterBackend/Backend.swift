import Foundation
import SprinterContract

/// The **`Backend` port** (INV-PORT / D14) — the seam feature code depends on to
/// reach the daemon.
///
/// It speaks only owned domain types (``Snapshot``, ``WorkstreamId``,
/// ``WorkGraphEvent``) and the neutral ``ContractError`` channel. Nothing here
/// references *where* the daemon runs or *which* transport carries the traffic:
/// local-daemon vs. remote-daemon is an adapter choice made when the connection's
/// transport is selected (see ``BackendConnector``), never a distinction the
/// client surface can observe.
/// The offset-stamped work-graph feed a ``Backend`` serves: each item pairs a delta with
/// the DURABLE offset it was journaled at, which is what a reconnecting client resumes
/// from. Named so the offset feed's signature stays one readable line as it gains the
/// generation the cursor belongs to.
public typealias OffsetEventStream = AsyncThrowingStream<OffsetEvent, any Error>

public protocol Backend: Sendable {
  /// Hydrates the full owned read model (snapshot-on-connect, D4).
  func snapshot() async throws -> Snapshot

  /// Submits a plan; yields the new ``WorkstreamId`` or throws
  /// ``ContractError/planRejected(reason:)``.
  func createWorkstreamFromPlan(_ plan: WorkstreamPlan) async throws -> WorkstreamId

  /// Applies a lifecycle action; throws ``ContractError/workstreamNotFound(id:)``
  /// for an unknown workstream.
  func control(workstreamId: WorkstreamId, action: ControlAction) async throws

  /// Requeues an issue; throws ``ContractError/issueNotFound(id:)`` for an unknown
  /// issue.
  func retryIssue(issueId: IssueId) async throws

  /// The live work-graph delta subscription (INV-REACTIVE): a stream of
  /// ``WorkGraphEvent`` fed until the daemon ends the subscription.
  func events() -> AsyncThrowingStream<WorkGraphEvent, any Error>

  /// The live work-graph subscription **with durable offsets** for reconnect replay
  /// (CE2.2). `resume` is the resume context: `nil` replays from the log ORIGIN, a value
  /// resumes STRICTLY AFTER its `sinceOffset` (an incremental resume, not a snapshot
  /// re-derive). Each item is an ``OffsetEvent`` pairing the delta with the durable
  /// offset it was journaled at, so a reconnecting client can track its last-applied
  /// position (see ``WorkGraphResync``). The daemon replays gaplessly after the cursor,
  /// so no delta is missed across a disconnect. A default adapter implementation wraps
  /// ``events()`` with synthetic offsets; the live ``RpcBackend`` carries real ones.
  ///
  /// The cursor and the ``Snapshot/generation`` it was minted under are ONE value, not
  /// two parameters that happen to be passed together: a durable offset is only
  /// meaningful inside one store generation, so the daemon refuses a resume whose
  /// generation is stale
  /// (``ContractError/resyncRequired(sinceOffset:maxOffset:generation:)``) instead of
  /// resuming it against a log it never belonged to — and the ABSENCE of the whole
  /// context, never a particular offset value, is what makes a request an ORIGIN
  /// subscription.
  func events(resume: ResumeContext?) -> OffsetEventStream

  // MARK: - Session channel (BE1.2 / D9)

  /// The live session feed: a stream of owned ``SessionEvent`` for `sessionId`
  /// (turn lifecycle, message/tool deltas, `UiRequestRaised`, …), fed until the
  /// daemon ends the subscription. An unknown `_tag` is a decode failure, never a
  /// silent drop.
  func sessionEvents(sessionId: SessionId) -> AsyncThrowingStream<SessionEvent, any Error>

  /// Drives input INTO a session (a fresh prompt, a mid-turn steer, or a
  /// follow-up); throws ``ContractError/sessionNotFound(id:)`` for an unknown
  /// session.
  func sessionSend(sessionId: SessionId, input: SessionInput) async throws

  /// Interrupts a running session (D9 — every session is interruptible); throws
  /// ``ContractError/sessionNotFound(id:)`` for an unknown session.
  func interrupt(sessionId: SessionId) async throws

  /// Answers an outstanding `UiRequestRaised` with the neutral ``UiResponse`` that
  /// keys the answer to its request id; throws ``ContractError/sessionNotFound(id:)``
  /// for an unknown session.
  func answerUiRequest(sessionId: SessionId, response: UiResponse) async throws

  /// Tears the connection down deterministically: cancels the inbound receive
  /// loop, closes the underlying transport, and fails every in-flight request with
  /// ``BackendError/connectionClosed``. Idempotent. Downstream `.app`/feature
  /// wiring (and the future live transport) needs this to drop a connection without
  /// leaking the receive task or the socket — so the port contract owns teardown,
  /// not just the transport provider.
  func close() async
}

extension Backend {
  /// Default offset-aware feed for adapters WITHOUT durable offsets (the in-memory test
  /// fakes): it wraps the bare ``events()`` stream, assigning synthetic sequential
  /// offsets starting strictly after `resume.sinceOffset`. Real durable replay is provided by
  /// adapters that override this (``RpcBackend``), so a fake never has to model offsets
  /// yet still satisfies the port — and the offset-driven reconnect logic is exercised
  /// against the real adapter.
  ///
  /// The synthetic sequence honors the SAME offset origin as production: durable offsets
  /// are strictly `> 0` (the daemon's journal starts at 1), so an origin subscription
  /// (`resume: nil`) mints its first offset at `1`, not `0`. A `0`-based origin would
  /// contradict that `> 0` invariant AND be silently dropped by ``ContiguousOffsetTracker``
  /// (whose origin prefix seeds at `0`, so an offset of `0` is not `> contiguous` and never
  /// advances the prefix) — so the fake would exercise a different origin boundary than the
  /// real adapter. Starting at `1` keeps the fake faithful to the production invariant.
  ///
  /// A fake has no durable store and therefore no generation to check, so the context's
  /// `generation` is ignored here — the port's shape is what the fake satisfies; the real
  /// guard is the daemon's, exercised against ``RpcBackend``.
  public func events(resume: ResumeContext?) -> OffsetEventStream {
    let base = events()
    return AsyncThrowingStream { continuation in
      let task = Task {
        var offset = (resume?.sinceOffset ?? 0) + 1
        do {
          for try await event in base {
            continuation.yield(OffsetEvent(offset: offset, event: event))
            offset += 1
          }
          continuation.finish()
        } catch {
          continuation.finish(throwing: error)
        }
      }
      continuation.onTermination = { _ in task.cancel() }
    }
  }
}

/// Transport / protocol-level failures surfaced by the client, distinct from the
/// daemon's owned ``ContractError`` channel. A `Fail` cause decodes to a
/// ``ContractError``; the cases here cover the envelope's non-`Fail` outcomes and
/// broken-connection conditions.
public enum BackendError: Error, Equatable, Sendable {
  /// The transport ended before an in-flight request reached its terminal `Exit`.
  case connectionClosed
  /// The daemon reported an unrecoverable defect (a `Die` cause or `Defect` frame).
  case daemonDefect
  /// The in-flight request was interrupted (an `Interrupt` cause).
  case interrupted
  /// The server reported a client protocol error affecting the connection.
  case protocolError
  /// A response frame could not be decoded into the expected owned type.
  case malformedResponse
}
