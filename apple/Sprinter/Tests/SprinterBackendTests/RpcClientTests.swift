import Foundation
import SprinterContract
import Testing

@testable import SprinterBackend

@Suite("RPC client queries and streams")
struct RpcClientTests {
  @Test("a snapshot query resolves off its correlated Exit")
  func snapshotQuery() async throws {
    let transport = FakeTransport()
    let backend = RpcBackend(transport: transport)
    var outbound = transport.outbound.makeAsyncIterator()

    let task = Task { try await backend.snapshot() }
    let request = try await nextSent(&outbound)
    #expect(request.envelopeTag == "Request")
    #expect(request.rpcTag == "snapshot")
    let id = try #require(request.id)

    transport.emit(Wire.exitSuccess(requestId: id, value: try Wire.encoded(Fixtures.snapshot)))
    #expect(try await task.value == Fixtures.snapshot)
    transport.close()
  }

  @Test("close() fails an in-flight request, rejects new ones, and is idempotent (teardown seam)")
  func closeTearsDownTheConnection() async throws {
    let transport = FakeTransport()
    let backend = RpcBackend(transport: transport)
    var outbound = transport.outbound.makeAsyncIterator()

    // An in-flight request whose Exit never arrives — the connection is torn down
    // under it. Awaiting the sent Request guarantees it is registered as pending.
    let inflight = Task { try await backend.snapshot() }
    _ = try await nextSent(&outbound)

    await backend.close()

    // The in-flight request fails with connectionClosed (the receive loop was
    // cancelled and the transport closed — no leak against a real socket).
    await #expect(throws: BackendError.connectionClosed) { try await inflight.value }
    // A second close is a no-op, and a request after close is rejected.
    await backend.close()
    await #expect(throws: BackendError.connectionClosed) { try await backend.snapshot() }
  }

  @Test("a cancelled request resumes rather than waiting for an Exit that never comes")
  func cancelledRequestResumes() async throws {
    // #94's general bar — no unbounded waits. A request's suspension is otherwise released
    // ONLY by its correlated `Exit` or by the connection's teardown, so a cancelled caller
    // whose daemon never answers (and whose connection nobody closes) would wait forever.
    let transport = FakeTransport()
    let connection = RpcConnection(transport: transport)
    var outbound = transport.outbound.makeAsyncIterator()

    let inflight = Task { try await connection.request(tag: "snapshot", payload: nil) }
    // Awaiting the sent Request guarantees the entry is registered as pending, so this
    // exercises cancelling an ALREADY-suspended request, not an early-exit check.
    let request = try await nextSent(&outbound)
    #expect(request.envelopeTag == "Request")

    inflight.cancel()
    await #expect(throws: CancellationError.self) { try await inflight.value }
    // And the daemon is told to stop working on it, exactly as an abandoned stream is.
    let interrupt = try await nextSent(&outbound)
    #expect(interrupt.envelopeTag == "Interrupt")
    #expect(interrupt.requestId == request.id)
    transport.close()
  }

  @Test("cancelling a MUTATING request releases the caller without interrupting the daemon")
  func cancelledMutatingRequestIsNotInterrupted() async throws {
    // The daemon interrupts the handler's FIBER on an `Interrupt`, and
    // `createWorkstreamFromPlan` commits `putRepository` and `putWorkstream` as two
    // independent transactions. Interrupting it can therefore leave a persisted repository
    // with no workstream. So the cancellation bounds the CLIENT's wait (a `CancellationError`)
    // without changing the wire semantics for a mutation: no `Interrupt` goes out, and the
    // daemon runs the write to completion exactly as it did before #94.
    let transport = FakeTransport()
    let connection = RpcConnection(transport: transport)
    var outbound = transport.outbound.makeAsyncIterator()

    let inflight = Task {
      try await connection.request(tag: "createWorkstreamFromPlan", payload: nil)
    }
    let request = try await nextSent(&outbound)
    #expect(request.rpcTag == "createWorkstreamFromPlan")

    inflight.cancel()
    // The wait is still bounded — this is #94's actual bar, and it is unchanged.
    await #expect(throws: CancellationError.self) { try await inflight.value }

    // ...but nothing was sent for it. A following `Ping` is the probe: the retirement path
    // transmits on the actor without suspending, so an `Interrupt` — had one been emitted —
    // would be recorded strictly BEFORE this ping, and would be the frame read here.
    await connection.ping()
    let next = try await nextSent(&outbound)
    #expect(
      next.envelopeTag == "Ping",
      Comment(
        rawValue: """
          expected no frame for the cancelled mutation, but the next frame on the wire was \
          \(next.envelopeTag) — a cancelled mutation is interrupting daemon-side work.
          """))
    transport.close()
  }

  @Test("a cancelled request's Interrupt never overtakes its own Request on the wire")
  func cancelledRequestInterruptFollowsItsRequest() async throws {
    // The transmit and the cancellation are two UNSTRUCTURED tasks hopping onto the same
    // actor; their relative order is not a language guarantee (priority escalation can
    // reorder them). If the `Interrupt` won, the daemon would drop it as an unknown id and
    // then run the `Request` to completion with no client-side consumer left — its `Exit` is
    // discarded by `handleExit`'s missing-entry guard — leaking server work on every
    // cancelled request. So the ordering is enforced by state, and asserted here.
    //
    // Cancelled WITHOUT first awaiting the sent `Request`, which is what makes both
    // interleavings reachable; repeated so a scheduling-dependent regression cannot pass by
    // happening to win the race once.
    for iteration in 0..<64 {
      let transport = FakeTransport()
      let connection = RpcConnection(transport: transport)
      var outbound = transport.outbound.makeAsyncIterator()

      let inflight = Task { try await connection.request(tag: "snapshot", payload: nil) }
      inflight.cancel()
      await #expect(throws: CancellationError.self) { try await inflight.value }

      let first = try await nextSent(&outbound)
      #expect(
        first.envelopeTag == "Request",
        Comment(
          rawValue: """
            iteration \(iteration): the first frame on the wire was \(first.envelopeTag) — \
            an Interrupt overtook its own Request.
            """))
      let second = try await nextSent(&outbound)
      #expect(second.envelopeTag == "Interrupt")
      #expect(second.requestId == first.id)
      transport.close()
    }
  }

  @Test("createWorkstreamFromPlan sends the plan payload and returns the id")
  func createWorkstreamQuery() async throws {
    let transport = FakeTransport()
    let backend = RpcBackend(transport: transport)
    var outbound = transport.outbound.makeAsyncIterator()

    let task = Task { try await backend.createWorkstreamFromPlan(Fixtures.plan) }
    let request = try await nextSent(&outbound)
    #expect(request.rpcTag == "createWorkstreamFromPlan")
    let payload = try #require(request.payload)
    let expected = try toJSONValue(CreateWorkstreamFromPlanPayload(plan: Fixtures.plan))
    #expect(payload == expected)

    let id = try #require(request.id)
    transport.emit(Wire.exitSuccess(requestId: id, value: #""ws-9""#))
    #expect(try await task.value == WorkstreamId(rawValue: "ws-9"))
    transport.close()
  }

  @Test("a void-success command resolves on Exit Success")
  func voidCommand() async throws {
    let transport = FakeTransport()
    let backend = RpcBackend(transport: transport)
    var outbound = transport.outbound.makeAsyncIterator()

    let task = Task {
      try await backend.control(workstreamId: WorkstreamId(rawValue: "ws-1"), action: .pause)
    }
    let request = try await nextSent(&outbound)
    #expect(request.rpcTag == "control")
    transport.emit(Wire.exitSuccessVoid(requestId: try #require(request.id)))
    try await task.value
    transport.close()
  }

  @Test("a Fail cause surfaces the mirrored ContractError")
  func contractErrorOffFailure() async throws {
    let transport = FakeTransport()
    let backend = RpcBackend(transport: transport)
    var outbound = transport.outbound.makeAsyncIterator()

    let task = Task { try await backend.createWorkstreamFromPlan(Fixtures.plan) }
    let id = try #require(try await nextSent(&outbound).id)
    transport.emit(
      Wire.exitFail(requestId: id, error: #"{"_tag":"PlanRejected","reason":"too big"}"#))

    await #expect(throws: ContractError.planRejected(reason: "too big")) {
      _ = try await task.value
    }
    transport.close()
  }

  @Test("retryIssue surfaces IssueNotFound off a Fail cause")
  func issueNotFoundError() async throws {
    let transport = FakeTransport()
    let backend = RpcBackend(transport: transport)
    var outbound = transport.outbound.makeAsyncIterator()

    let task = Task { try await backend.retryIssue(issueId: IssueId(rawValue: "iss-9")) }
    let id = try #require(try await nextSent(&outbound).id)
    transport.emit(Wire.exitFail(requestId: id, error: #"{"_tag":"IssueNotFound","id":"iss-9"}"#))

    await #expect(throws: ContractError.issueNotFound(id: IssueId(rawValue: "iss-9"))) {
      try await task.value
    }
    transport.close()
  }

  @Test("a Die cause surfaces as a daemon defect")
  func dieCause() async throws {
    let transport = FakeTransport()
    let backend = RpcBackend(transport: transport)
    var outbound = transport.outbound.makeAsyncIterator()

    let task = Task { try await backend.snapshot() }
    let id = try #require(try await nextSent(&outbound).id)
    transport.emit(Wire.exitDie(requestId: id))
    await #expect(throws: BackendError.daemonDefect) { try await task.value }
    transport.close()
  }

  @Test("an Interrupt cause surfaces as interrupted")
  func interruptCause() async throws {
    let transport = FakeTransport()
    let backend = RpcBackend(transport: transport)
    var outbound = transport.outbound.makeAsyncIterator()

    let task = Task { try await backend.snapshot() }
    let id = try #require(try await nextSent(&outbound).id)
    transport.emit(Wire.exitInterrupt(requestId: id))
    await #expect(throws: BackendError.interrupted) { try await task.value }
    transport.close()
  }

  @Test("a closed transport fails an in-flight query")
  func closedConnectionFailsQuery() async throws {
    let transport = FakeTransport()
    let backend = RpcBackend(transport: transport)
    var outbound = transport.outbound.makeAsyncIterator()

    let task = Task { try await backend.snapshot() }
    _ = try await nextSent(&outbound)
    transport.close()
    await #expect(throws: BackendError.connectionClosed) { try await task.value }
  }

  @Test("a Defect frame fails an in-flight query")
  func defectFrameFailsQuery() async throws {
    let transport = FakeTransport()
    let backend = RpcBackend(transport: transport)
    var outbound = transport.outbound.makeAsyncIterator()

    let task = Task { try await backend.snapshot() }
    _ = try await nextSent(&outbound)
    transport.emit(Wire.defect())
    await #expect(throws: BackendError.daemonDefect) { try await task.value }
    transport.close()
  }
}
