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
  /// The run has ended — the CLOSED range `[0, lastOffset]`: complete, immutable, and
  /// therefore cacheable once read.
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

/// The CLOSED transcript `[0, lastOffset]` of a settled execution: complete and
/// immutable, so a reader may cache it. `lastOffset` is a durable offset in the
/// daemon's CURRENT store generation (``Snapshot/generation``) — `0` for a run that
/// produced no durable entry at all, which is a valid, EMPTY sealed transcript.
///
/// It has no tail, and that is expressed by it NOT being a ``LiveTranscript`` rather
/// than by a runtime refusal.
public struct SealedTranscript: Codable, Equatable, Sendable {
  public let lastOffset: Int

  public init(lastOffset: Int) {
    self.lastOffset = lastOffset
  }
}
