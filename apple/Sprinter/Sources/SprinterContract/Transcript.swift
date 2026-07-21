/// An ``Execution``'s transcript — mirror of the contract's `Transcript` tagged union
/// (DE2.2, decision D4).
///
/// The two variants are DISTINCT TYPES here, exactly as they are in the contract, and
/// for the same reason: a tail subscription must be UNREPRESENTABLE on a sealed
/// transcript rather than accepted and refused at runtime. An affordance that tails
/// takes a ``LiveTranscript`` in its signature, so handing it a ``SealedTranscript`` is
/// a COMPILE error on the client too — not a runtime branch a future view model can
/// forget to write. Neither type carries a flag saying which it is; the union's tag is
/// the whole difference.
///
/// Liveness is therefore read off THIS value (``Execution/isLive``) and nowhere else:
/// there is no execution-status enum beside it to keep in agreement.
public enum Transcript: Codable, Equatable, Sendable {
  /// The execution is still running — an OPEN offset range, so a reader that wants to
  /// stay current must tail it.
  case live(LiveTranscript)
  /// The run has ended — `[0, lastOffset]` is a final, immutable PREFIX of the
  /// transcript, so it is cacheable once read. `lastOffset` is a LOWER BOUND on the
  /// extent, not a claim that nothing exists beyond it (see ``SealedTranscript``).
  case sealed(SealedTranscript)

  private enum CodingKeys: String, CodingKey {
    case tag = "_tag"
    case lastOffset
  }

  public init(from decoder: any Decoder) throws {
    let container = try decoder.container(keyedBy: CodingKeys.self)
    let tag = try container.decode(String.self, forKey: .tag)
    switch tag {
    case "LiveTranscript":
      self = .live(LiveTranscript())
    case "SealedTranscript":
      self = .sealed(
        SealedTranscript(lastOffset: try container.decode(Int.self, forKey: .lastOffset)))
    default:
      throw DecodingError.dataCorruptedError(
        forKey: .tag,
        in: container,
        debugDescription: "Unknown Transcript tag: \(tag)"
      )
    }
  }

  public func encode(to encoder: any Encoder) throws {
    var container = encoder.container(keyedBy: CodingKeys.self)
    switch self {
    case .live:
      try container.encode("LiveTranscript", forKey: .tag)
    case .sealed(let sealed):
      try container.encode("SealedTranscript", forKey: .tag)
      try container.encode(sealed.lastOffset, forKey: .lastOffset)
    }
  }
}

/// The OPEN transcript of a RUNNING execution: it has no last entry yet, which is what
/// "open" means, so it carries no extent. It is the type a TAIL subscription takes.
public struct LiveTranscript: Codable, Equatable, Sendable {
  public init() {}
}

/// The transcript of a SETTLED execution. `lastOffset` is a durable offset in the
/// daemon's CURRENT store generation (``Snapshot/generation``) — `0` for a run that
/// produced no durable entry at all, which is a valid, EMPTY sealed transcript.
///
/// CONTRACT — `lastOffset` is a LOWER BOUND on the extent, mirroring the daemon's
/// `SealedTranscript`. `[0, lastOffset]` is guaranteed COMPLETE and IMMUTABLE, which is
/// the entire cacheability claim and is unconditionally true: entries never change and
/// per-execution offsets never reset, so a cached prefix can never be invalidated. It is
/// NOT a guarantee that the daemon's durable log holds nothing beyond it — the seal
/// falls back to `0` on a transient extent read, an append in flight when the run
/// terminates can land after the extent is read, and a re-dispatch appends to the same
/// log. A client may therefore cache what it has read and must still be prepared to be
/// handed more; it must never treat a sealed transcript as proof of the whole.
///
/// It has no tail, and that is expressed by it NOT being a ``LiveTranscript`` rather
/// than by a runtime refusal.
public struct SealedTranscript: Codable, Equatable, Sendable {
  public let lastOffset: Int

  public init(lastOffset: Int) {
    self.lastOffset = lastOffset
  }
}
