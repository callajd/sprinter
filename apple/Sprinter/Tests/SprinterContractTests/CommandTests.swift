import Foundation
import Testing

@testable import SprinterContract

@Suite("Commands and errors")
struct CommandTests {
  /// The store generation the resume-bearing goldens were frozen under.
  private let generation = StoreGenerationId(
    rawValue: "8f0d0a3e-4a7a-4a2e-9b5e-0f2c1d3e4a5b")

  @Test("decodes the workstream plan")
  func decodesWorkstreamPlan() throws {
    let plan = try Golden.decode(WorkstreamPlan.self, from: "workstream-plan")
    #expect(plan.name == "Foundation")
    #expect(plan.repo == "callajd/sprinter")
    #expect(try Golden.roundTrip(plan) == plan)
  }

  @Test("decodes every control action literal")
  func decodesControlActions() throws {
    let actions = try Golden.decode([ControlAction].self, from: "control-actions")
    #expect(actions == [.start, .pause, .resume, .cancel])
  }

  @Test("decodes the createWorkstreamFromPlan payload")
  func decodesCreatePayload() throws {
    let payload = try Golden.decode(
      CreateWorkstreamFromPlanPayload.self, from: "payload-create-workstream-from-plan")
    #expect(payload.plan.name == "Foundation")
    #expect(try Golden.roundTrip(payload) == payload)
  }

  @Test("decodes the control payload")
  func decodesControlPayload() throws {
    let payload = try Golden.decode(ControlPayload.self, from: "payload-control")
    #expect(payload.workstreamId == WorkstreamId(rawValue: "ws-1"))
    #expect(payload.action == .pause)
    #expect(try Golden.roundTrip(payload) == payload)
  }

  @Test("decodes the retryIssue payload")
  func decodesRetryPayload() throws {
    let payload = try Golden.decode(RetryIssuePayload.self, from: "payload-retry-issue")
    #expect(payload.issueId == IssueId(rawValue: "iss-1"))
    #expect(try Golden.roundTrip(payload) == payload)
  }

  @Test("decodes the events payload — the resume context, cursor and generation as one")
  func decodesEventsPayload() throws {
    let payload = try Golden.decode(EventsPayload.self, from: "payload-events")
    // The cursor never travels alone: a durable offset is a coordinate in ONE store
    // generation, so the wire nests the two INSIDE a single `resume` object rather than
    // carrying two independent optional keys. There is no wire form for a cursor without
    // its generation, which is what makes the daemon's guard un-bypassable.
    #expect(payload.resume?.sinceOffset == 12)
    #expect(payload.resume?.generation == generation)
    #expect(try Golden.roundTrip(payload) == payload)
  }

  @Test("decodes the events payload with the resume context absent (origin replay)")
  func decodesEventsPayloadNoOffset() throws {
    let payload = try Golden.decode(EventsPayload.self, from: "payload-events-no-offset")
    // An ORIGIN request names no coordinate, so the WHOLE `resume` key is omitted from
    // the wire (never `null`). Its absence — not any offset value — is what makes this
    // an origin request.
    #expect(payload.resume == nil)
    #expect(try Golden.roundTrip(payload) == payload)
  }

  @Test("decodes the sessionEvents payload with a resume context")
  func decodesSessionEventsPayload() throws {
    let payload = try Golden.decode(SessionEventsPayload.self, from: "payload-session-events")
    #expect(payload.sessionId == SessionId(rawValue: "ses-1"))
    // The session channel's cursor is generation-scoped in exactly the same way (its
    // per-session log is dropped by a schema bump too), so it carries the SAME nested
    // value — the guard is not weaker here, structurally or otherwise.
    #expect(payload.resume?.sinceOffset == 12)
    #expect(payload.resume?.generation == generation)
    #expect(try Golden.roundTrip(payload) == payload)
  }

  @Test("decodes the sessionEvents payload with the resume context absent (origin replay)")
  func decodesSessionEventsPayloadNoOffset() throws {
    let payload = try Golden.decode(
      SessionEventsPayload.self, from: "payload-session-events-no-offset")
    #expect(payload.sessionId == SessionId(rawValue: "ses-1"))
    #expect(payload.resume == nil)
    #expect(try Golden.roundTrip(payload) == payload)
  }

  @Test("decodes the sessionSend payload")
  func decodesSessionSendPayload() throws {
    let payload = try Golden.decode(SessionSendPayload.self, from: "payload-session-send")
    #expect(payload.sessionId == SessionId(rawValue: "ses-1"))
    #expect(payload.input.mode == .prompt)
    #expect(try Golden.roundTrip(payload) == payload)
  }

  @Test("decodes the interrupt payload")
  func decodesInterruptPayload() throws {
    let payload = try Golden.decode(InterruptPayload.self, from: "payload-interrupt")
    #expect(payload.sessionId == SessionId(rawValue: "ses-1"))
    #expect(try Golden.roundTrip(payload) == payload)
  }

  @Test("decodes the answerUiRequest payload")
  func decodesAnswerPayload() throws {
    let payload = try Golden.decode(AnswerUiRequestPayload.self, from: "payload-answer-ui-request")
    #expect(payload.sessionId == SessionId(rawValue: "ses-1"))
    #expect(payload.response.answer == .confirmed(confirmed: true))
    #expect(try Golden.roundTrip(payload) == payload)
  }

  @Test("decodes the createWorkstreamFromPlan response (bare WorkstreamId)")
  func decodesResponseId() throws {
    let id = try Golden.decode(WorkstreamId.self, from: "response-workstream-id")
    #expect(id == WorkstreamId(rawValue: "ws-1"))
    #expect(try Golden.roundTrip(id) == id)
  }

  @Test("decodes every contract error variant")
  func decodesContractErrors() throws {
    let errors = try Golden.decode([ContractError].self, from: "contract-errors")
    #expect(errors.count == 5)
    #expect(errors[0] == .workstreamNotFound(id: WorkstreamId(rawValue: "ws-9")))
    #expect(errors[1] == .issueNotFound(id: IssueId(rawValue: "iss-9")))
    #expect(errors[2] == .sessionNotFound(id: SessionId(rawValue: "ses-9")))
    #expect(errors[3] == .planRejected(reason: "empty spec"))
    // The resume refusal shared by BOTH cursor-bearing feeds: the client's cursor belongs
    // to a store generation the daemon dropped, so it must re-hydrate from `snapshot`.
    // Note the cursor is WITHIN the log's extent (2 <= 3) — the case no offset comparison
    // can detect, which is why the generation is an explicit identity on the wire.
    #expect(
      errors[4]
        == .resyncRequired(
          sinceOffset: 2, maxOffset: 3,
          generation: StoreGenerationId(rawValue: "8f0d0a3e-4a7a-4a2e-9b5e-0f2c1d3e4a5b")))
    for error in errors {
      #expect(try Golden.roundTrip(error) == error)
    }
  }
}
