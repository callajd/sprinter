# Runbook — CE4.1 genuinely-real Issue → PR through the app

> **Status:** operator runbook. This is the human-driven counterpart to the
> automated, deterministic acceptance test
> (`packages/daemon/src/acceptance.test.ts`). The automated test proves the loop
> **composes** against the real daemon over the real socket with a **scripted `pi`**
> and a **sandboxed `Repository`**; this runbook drives the **genuinely real** thing
> — the real `pi` binary and real GitHub — against a **throwaway sandbox repo**.
>
> **Do not run this in CI and do not point it at a real product repo.** It fires a
> real agent that opens a real PR. Use a disposable repository you own.

## What this proves

From the running macOS app, against a real local daemon, you materialize a plan
into a workstream, dispatch a real Sprinter **Issue**, and watch — live — a real
**PR open**, with:

- **Mission Control** — the board updating live as the workstream/epic/issue/job
  transition (`snapshot` + the `events` feed).
- **Interactive session** — the agent's session driving and remaining
  interruptible (the `sessionEvents` / `sessionSend` / `interrupt` channel).
- **Inspector** — the agent's transcript paired with the PR it produced.

## Prerequisites

1. **Bun** — the repo's pinned version (`.bun-version`). `bun --version`.
2. **The `pi` binary on `PATH`**, authenticated. `pi --version` must succeed and
   `~/.pi/agent/auth.json` must hold valid provider credentials (the daemon does
   not reimplement Pi/provider auth — it spawns the binary). Confirm the installed
   `pi` matches the wire version the adapter mirrors
   (`packages/domain/src/pi/wire.ts` — `PI_WIRE_VERSION`); a mismatch surfaces as a
   decode failure on the live session.
3. **A throwaway GitHub sandbox repo you own**, e.g. `your-user/sprinter-sandbox`,
   with at least one open Issue whose body is a small, self-contained task. Note
   the Issue number.
4. **A GitHub token** (`GITHUB_TOKEN`) with `repo` scope on THAT sandbox repo, and
   `gh auth status` green (the agent opens the PR via `gh` from inside its
   worktree). **Never** use a token with access to a real product repo.

## Steps

### 1. Build the app and the daemon

```bash
# Daemon (bun/TypeScript side)
bun install
bun run check                      # gates green before you start

# App (Swift side) — first run pays a one-time SwiftLintPlugins/swift-syntax build
cd apple/Sprinter && make check && cd -
```

Open the macOS app from Xcode (or `swift run SprinterApp` from `apple/Sprinter`).

### 2. Start the real daemon against the sandbox repo

The daemon's config comes from the environment (`configFromEnv`,
`packages/daemon/src/main.ts`). `GITHUB_TOKEN` is **required** — the daemon fails
fast at boot without it.

```bash
export GITHUB_TOKEN=ghp_your_sandbox_scoped_token
export SPRINTER_REPO_OWNER=your-user
export SPRINTER_REPO_NAME=sprinter-sandbox
export SPRINTER_SOCKET="$PWD/.sprinter/daemon.sock"
export SPRINTER_DB="$PWD/.sprinter/state.db"
export SPRINTER_WORKSPACE="$PWD/.sprinter/worktrees"
mkdir -p "$PWD/.sprinter"

bun packages/daemon/src/run.ts
```

The daemon binds the Unix socket at `SPRINTER_SOCKET`, opens the file-backed
`StateStore`, and runs the startup reconcile. Leave it running.

### 3. Point the app at the daemon and connect

In the app, set the daemon endpoint to the same `SPRINTER_SOCKET` path (the app's
`UnixSocketTransport` dials it). On connect you should see the current snapshot —
empty on a fresh sandbox.

### 4. Materialize a plan into a workstream

In the app's **Planner**, create a workstream for the sandbox repo
(`your-user/sprinter-sandbox`) with a short spec. Confirm a new workstream appears
live in **Mission Control** (a `WorkstreamChanged` delta).

> The current daemon materializes the top-level workstream from the plan; the
> epic/issue breakdown is produced by a planning Job. For this runbook, if planning
> has not yet populated the graph, seed the target Issue's epic/issue directly (or
> drive the interactive planning session) so there is a queued Job for the sandbox
> Issue to dispatch. This is the one manual bridge until the planner materializer
> lands.

### 5. Dispatch the Issue and watch the PR open

Start the workstream (or retry the Issue) from Mission Control. The daemon
dispatches the Job through the real `LocalPi` runner, which spawns `pi --mode rpc`
in the Job's worktree. Then, live:

- **Interactive session** — open the session. You should see the transcript stream
  (the agent's real reasoning/tool calls). Try `sessionSend` (steer it) and
  `interrupt` (abort the turn) — both must take effect on the live session.
- The agent does the work and **opens a real PR** against the sandbox repo via
  `gh`. Confirm the PR exists on GitHub.
- **Mission Control** — the Issue/Job transition live (`in_progress` →
  `in_review`/`done`) as the daemon reconciles the PR against the `Repository`
  port.

### 6. Confirm the Inspector pairing

Open the **Inspector** for the session. The agent's complete transcript must be
paired with the PR it produced (the session → job → PR resolution). This is the
end-to-end acceptance: **one transcript, one PR, paired**.

### 7. Tear down

Stop the app. `Ctrl-C` the daemon (SIGINT tears the socket + child `pi`
processes down cleanly). Delete the sandbox worktrees/state:

```bash
rm -rf "$PWD/.sprinter"
```

Close or delete the sandbox PR/Issue as you like — it is disposable.

## Differences from the automated test (why both exist)

| Aspect | Automated acceptance test | This runbook |
|---|---|---|
| Daemon | real `main.ts` graph, real Unix socket | real `main.ts` graph, real Unix socket |
| Client wire | real `RpcClient` (NDJSON over the socket) | the app's real `UnixSocketTransport` |
| Agent | **scripted `pi`** (canned transcript, deterministic) | **real `pi`** binary |
| Code host | **sandboxed in-process `Repository`** (no network) | **real GitHub** (throwaway repo) |
| PR | deterministic PR-open fixture | a genuinely opened PR |
| Runs in CI | yes (deterministic, bounded, hermetic) | no (fires real agent + real PR) |

The automated test is the gate; this runbook is the periodic real-world
confidence check the automated test deliberately cannot be.
