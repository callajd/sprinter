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

  /// A retirement is LIFECYCLE-ONLY: it repeats the revision it retires verbatim and
  /// differs ONLY in `id`, `supersedes` and `retiredAt`. The fixture is generated as
  /// exactly that (the revised agent's content plus the stamp), and this asserts the
  /// difference field by field against `agent-revised` — so a fixture that quietly
  /// rewrote content while retiring would fail here rather than teach the mirror a
  /// shape the daemon's `StateStore` now refuses to write.
  @Test("reads retired-ness off the retiredAt stamp, and retirement preserves content")
  func decodesRetired() throws {
    let agent = try Golden.decode(Agent.self, from: "agent-retired")
    let retires = try Golden.decode(Agent.self, from: "agent-revised")
    #expect(agent.retiredAt == "2026-07-20T12:00:00.000Z")
    #expect(agent.isRetired)
    #expect(agent.supersedes == retires.id)
    // Content is carried over unchanged — only the lifecycle fields differ.
    #expect(agent.name == retires.name)
    #expect(agent.model == retires.model)
    #expect(agent.version == retires.version)
    #expect(agent.tools == retires.tools)
    #expect(agent.id != retires.id)
    #expect(try Golden.roundTrip(agent) == agent)
  }

  /// The LINEAGE question — the one a UI actually means by "is this agent still in
  /// service", and the one ``Agent/isRetired`` deliberately does not answer.
  @Test("isLineageRetired answers the lineage question that isRetired cannot")
  func lineageRetirement() throws {
    let original = try Golden.decode(Agent.self, from: "agent-original")
    let revised = try Golden.decode(Agent.self, from: "agent-revised")
    let retirement = try Golden.decode(Agent.self, from: "agent-retired")
    let registry = [original, revised, retirement]

    // The trap this exists to close: NOT ONE of the two earlier revisions carries a
    // stamp — they are immutable and were never rewritten — so a view keyed on
    // `isRetired` renders a retired lineage as live.
    #expect(!original.isRetired)
    #expect(!revised.isRetired)
    // Walking FORWARD along the reverse `supersedes` link reaches the stamp from any
    // revision of the lineage, which is the actual answer.
    #expect(isLineageRetired(original, in: registry))
    #expect(isLineageRetired(revised, in: registry))
    #expect(isLineageRetired(retirement, in: registry))

    // WITHOUT the retiring revision, the same lineage is live — the predicate reads the
    // collection it is handed, never a hidden global.
    #expect(!isLineageRetired(original, in: [original, revised]))
    // A lone revision with no successors is live.
    #expect(!isLineageRetired(original, in: [original]))
  }

  @Test("isLineageRetired ignores other lineages and terminates on a cyclic history")
  func lineageIsolationAndTermination() throws {
    let original = try Golden.decode(Agent.self, from: "agent-original")
    let revised = try Golden.decode(Agent.self, from: "agent-revised")
    let retirement = try Golden.decode(Agent.self, from: "agent-retired")

    // A retired revision of a DIFFERENT lineage says nothing about this one: the walk
    // follows `supersedes` links, not mere co-membership of the registry.
    let other = Agent(
      id: AgentId(rawValue: "agt-other-2"),
      name: "reviewer",
      model: "claude-opus-4-8",
      version: "1.0.0",
      tools: ["read"],
      supersedes: AgentId(rawValue: "agt-other-1"),
      retiredAt: "2026-07-20T12:00:00.000Z")
    #expect(!isLineageRetired(original, in: [original, revised, other]))

    // A CYCLE is excluded by the writer's precondition, but the walk must still
    // terminate if it is ever handed one (it visits each revision at most once).
    let cyclicOne = Agent(
      id: AgentId(rawValue: "agt-a"),
      name: "a", model: "m", version: "1", tools: [],
      supersedes: AgentId(rawValue: "agt-b"), retiredAt: nil)
    let cyclicTwo = Agent(
      id: AgentId(rawValue: "agt-b"),
      name: "b", model: "m", version: "1", tools: [],
      supersedes: AgentId(rawValue: "agt-a"), retiredAt: nil)
    #expect(!isLineageRetired(cyclicOne, in: [cyclicOne, cyclicTwo]))
    // And the mirror agrees with the TS helper on the retired-lineage case above.
    #expect(isLineageRetired(retirement, in: [original, revised, retirement]))
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
