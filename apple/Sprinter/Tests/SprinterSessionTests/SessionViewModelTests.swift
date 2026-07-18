import SprinterBackend
import SprinterContract
import Testing

@testable import SprinterSession

@Suite("Session view model")
@MainActor
struct SessionViewModelTests {
  private static let session = SessionId(rawValue: "session-a")

  /// The transcript builds live off a scripted feed: assorted variants — message
  /// deltas, a tool call/result, a notice, and a durable `EntryAppended` — project
  /// into the view-facing transcript, driven by a fake `Backend`, no daemon.
  @Test("the transcript builds from the scripted feed")
  func transcriptBuildsFromFeed() async throws {
    let backend = SessionFakeBackend(knownSession: Self.session)
    let model = SessionViewModel(backend: backend, sessionId: Self.session)
    model.start()

    backend.emit(.turnStarted)
    backend.emit(.messageStarted(messageId: "m1"))
    backend.emit(.messageDelta(messageId: "m1", text: "Hello", reasoning: nil))
    backend.emit(.toolStarted(id: "t1", name: "read", input: .string("x")))
    backend.emit(.toolCompleted(id: "t1", output: .string("y"), isError: false))
    backend.emit(.notice(id: "n-headsup", level: .info, message: "heads up"))
    backend.emit(.entryAppended(entry: .userMessage(id: "u1", text: "and this")))

    #expect(await waitUntil(model) { $0.count == 4 })
    let transcript = model.transcript
    #expect(transcript.isTurnActive)
    #expect(
      transcript.items.map(\.id) == ["message:m1", "tool:t1", "notice:n-headsup", "message:u1"])

    model.stop()
    await backend.close()
  }

  /// `send` drives `sessionSend` through the session channel with the exact
  /// `SessionInput` (the fake observes it), and resolves successfully.
  @Test("send drives sessionSend with the exact input")
  func sendDrivesInput() async throws {
    let backend = SessionFakeBackend(knownSession: Self.session)
    let model = SessionViewModel(backend: backend, sessionId: Self.session)
    var observed = backend.sent.makeAsyncIterator()

    let input = SessionInput(text: "steer left", images: nil, mode: .steer)
    try await model.send(input)

    #expect(await observed.next() == input)
    await backend.close()
  }

  /// `interrupt` drives `interrupt` through the session channel (the fake observes
  /// the session id), and resolves successfully.
  @Test("interrupt drives interrupt through the session channel")
  func interruptDrivesAbort() async throws {
    let backend = SessionFakeBackend(knownSession: Self.session)
    let model = SessionViewModel(backend: backend, sessionId: Self.session)
    var observed = backend.interrupted.makeAsyncIterator()

    try await model.interrupt()

    #expect(await observed.next() == Self.session)
    await backend.close()
  }

  /// `send`/`interrupt` surface the mirrored `SessionNotFound` for an unknown
  /// session, rather than silently succeeding.
  @Test("send and interrupt surface the mirrored SessionNotFound")
  func actionsSurfaceSessionNotFound() async throws {
    let unknown = SessionId(rawValue: "ghost")
    // The backend only knows `session`; the model drives the unknown `ghost`.
    let backend = SessionFakeBackend(knownSession: Self.session)
    let model = SessionViewModel(backend: backend, sessionId: unknown)

    await #expect(throws: ContractError.sessionNotFound(id: unknown)) {
      try await model.send(SessionInput(text: "hi", images: nil, mode: .prompt))
    }
    await #expect(throws: ContractError.sessionNotFound(id: unknown)) {
      try await model.interrupt()
    }
    await backend.close()
  }

  /// An inline `extension_ui_request` surfaces in `outstandingRequests`, and
  /// answering it drives the neutral `UiResponse` back (the fake observes it) and
  /// clears the prompt — the round-trip.
  @Test("an inline UI request is answered and clears")
  func uiRequestRoundTrip() async throws {
    let backend = SessionFakeBackend(knownSession: Self.session)
    let model = SessionViewModel(backend: backend, sessionId: Self.session)
    var observed = backend.answered.makeAsyncIterator()
    model.start()

    backend.emit(
      .uiRequestRaised(id: "req-1", kind: .select, prompt: "branch?", options: ["main", "dev"]))
    #expect(await waitUntilRequests(model) { $0.count == 1 })
    let request = try #require(model.outstandingRequests.first)
    #expect(request.id == "req-1")
    #expect(request.kind == .select)
    #expect(request.prompt == "branch?")
    #expect(request.options == ["main", "dev"])

    try await model.answer(requestId: "req-1", .value(value: "dev"))

    // The fake observed exactly the neutral UiResponse, keyed to the request id.
    let response = try #require(await observed.next())
    #expect(response == UiResponse(requestId: "req-1", answer: .value(value: "dev")))

    // The answered prompt left the outstanding set.
    #expect(await waitUntilRequests(model) { $0.isEmpty })

    model.stop()
    await backend.close()
  }

  /// Polls the main-actor model until `predicate` holds over its transcript items.
  private func waitUntil(
    _ model: SessionViewModel,
    _ predicate: ([TranscriptItem]) -> Bool
  ) async -> Bool {
    for _ in 0..<100_000 {
      if predicate(model.transcript.items) { return true }
      await Task.yield()
    }
    return false
  }

  /// Polls the main-actor model until `predicate` holds over its outstanding requests.
  private func waitUntilRequests(
    _ model: SessionViewModel,
    _ predicate: ([OutstandingUiRequest]) -> Bool
  ) async -> Bool {
    for _ in 0..<100_000 {
      if predicate(model.outstandingRequests) { return true }
      await Task.yield()
    }
    return false
  }
}
