import Foundation
import Testing

@testable import SprinterContract

/// The registry mirror (DE1.1) decoded against the committed goldens — the wire
/// bytes the TypeScript contract actually produced. Covers both optional keys
/// present and absent, the append-only `supersedes` link, and retired-ness read off
/// the `retiredAt` stamp.
@Suite("Registry")
struct RegistryTests {
  @Test("decodes an original agent revision (both optional keys absent -> nil)")
  func decodesOriginal() throws {
    let agent = try Golden.decode(Agent.self, from: "agent-original")
    #expect(agent.id == AgentId(rawValue: "agt-1"))
    #expect(agent.name == "implementer")
    #expect(agent.model == "claude-opus-4-8")
    #expect(agent.version == "1.0.0")
    #expect(agent.tools == ["read", "edit", "bash"])
    #expect(agent.supersedes == nil)
    #expect(agent.retiredAt == nil)
    #expect(agent.isOriginalRevision)
    #expect(!agent.isRetired)
    #expect(try Golden.roundTrip(agent) == agent)
  }

  @Test("decodes a revision linked to the one it supersedes (append-only edit)")
  func decodesRevised() throws {
    let agent = try Golden.decode(Agent.self, from: "agent-revised")
    #expect(agent.id == AgentId(rawValue: "agt-2"))
    #expect(agent.supersedes == AgentId(rawValue: "agt-1"))
    #expect(!agent.isOriginalRevision)
    #expect(!agent.isRetired)
    #expect(try Golden.roundTrip(agent) == agent)
  }

  @Test("reads retired-ness off the retiredAt stamp — there is no status enum")
  func decodesRetired() throws {
    let agent = try Golden.decode(Agent.self, from: "agent-retired")
    #expect(agent.retiredAt == "2026-07-20T12:00:00.000Z")
    #expect(agent.isRetired)
    #expect(agent.tools.isEmpty)
    #expect(try Golden.roundTrip(agent) == agent)
  }

  @Test("builds an agent through its memberwise initializer")
  func buildsAgent() throws {
    let built = Agent(
      id: AgentId(rawValue: "agt-9"),
      name: "reviewer",
      model: "claude-opus-4-8",
      version: "2.0.0",
      tools: ["read"],
      supersedes: nil,
      retiredAt: nil)
    #expect(built.isOriginalRevision)
    #expect(!built.isRetired)
    #expect(try Golden.roundTrip(built) == built)
  }
}
