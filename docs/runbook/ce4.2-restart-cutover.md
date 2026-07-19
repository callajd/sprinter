# Runbook — CE4.2 genuinely-real restart-safe cutover

> **Status:** operator runbook. This is the human-driven counterpart to the automated,
> deterministic restart integration test
> (`packages/daemon/src/restart-cutover.test.ts`). The automated test proves the loop
> **survives a daemon restart mid-flight** against the real daemon graph over a real
> socket with a **scripted `pi`** and a **sandboxed `Repository`**, restarting the daemon
> by tearing down and rebuilding its layer scope on the same DB file + socket. This
> runbook drives the **genuinely real** restart — the real `pi` binary, real GitHub, and a
> real process **SIGKILL** — against a **throwaway sandbox repo**.
>
> **Do not run this in CI and do not point it at a real product repo.** It fires a real
> agent that opens a real PR. Use a disposable repository you own.

## What the automated test proves (deterministic, in CI)

`bun run check` runs `restart-cutover.test.ts`, which is the **build-write-RESTART-read**
core, hermetic and hard-timeout-bounded:

1. **build + write** — dispatch an Issue's Job through the real daemon over the real
   socket; the scripted `pi` drives it to a mid-flight `running` state persisted to the
   file-backed SQLite `StateStore`, held live (no `agent_settled`).
2. **RESTART** — tear the daemon's whole layer scope down (the socket unbinds, SQLite
   closes with the Job still durably `running`, NOT settled), then bring a **fresh** daemon
   graph up on the **same** DB file + socket path.
3. **read** — the restarted daemon's boot `StartupReconcile` **re-dispatches** the
   persisted in-flight Job (`StartupReconcile` → CE1.2 file durability), re-attaching the
   **SAME** persisted session id (1 Job = 1 session, `UNIQUE(session.jobId)`), and drives
   it to `succeeded`. A fresh client (the app re-dialing) **resyncs** by resuming the
   `events` feed from its last-applied durable **offset** (the v3 cursor): every post-restart
   delta arrives strictly after the cursor (no duplication) and the terminal `succeeded`
   delta arrives (no loss).

It also proves **SQLite WAL replay of a committed mid-write** without a graceful
checkpoint (a second connection opened on the same file while the writer is still open
reads the committed `running` Job back) — the deterministic core of the crash-mid-write →
recovery watch-item.

The following are what this **runbook** adds that the deterministic test deliberately
cannot: a **real `pi` terminal-result** across the restart, and a **real SIGKILL** (not a
graceful scope teardown) exercising real on-disk WAL recovery.

## Prerequisites

Same as `docs/runbook/ce4.1-real-cutover.md`:

1. **Bun** — the pinned version (`.bun-version`).
2. **The `pi` binary on `PATH`**, authenticated (`~/.pi/agent/auth.json`), matching the
   wire version the adapter mirrors (`packages/domain/src/pi/wire.ts`).
3. **A throwaway GitHub sandbox repo you own** with an open Issue whose body is a small,
   self-contained task.
4. **A GitHub token** (`GITHUB_TOKEN`, `repo` scope on the sandbox only), `gh auth status`
   green. **Never** a token with access to a real product repo.

## Steps

### 1. Build both sides

```bash
bun install && bun run check                 # daemon gate green
cd apple/Sprinter && make check && cd -       # app gate green (first run pays the cold compile)
```

### 2. Start the real daemon against the sandbox repo

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

The daemon binds the socket, opens the file-backed `StateStore`, and runs the startup
reconcile. Note the daemon's **PID** (you will `kill -9` it):

```bash
# in another shell
pgrep -f 'packages/daemon/src/run.ts'
```

### 3. Connect the app and dispatch the Issue

Open the app (`swift run SprinterApp` from `apple/Sprinter`, or from Xcode) with the same
`SPRINTER_SOCKET`. Materialize a plan, then start the workstream (or retry the Issue) so
the daemon dispatches the Job through the real `LocalPi` runner. In **Interactive session**
you should see the transcript stream. **Leave the agent mid-run** — before it settles.

Confirm the Job is durably `running` before the kill:

```bash
# the Job row is `running` while the agent works
sqlite3 "$SPRINTER_DB" "select id, status, session_id from jobs;"
```

### 4. KILL the daemon mid-flight (the real crash)

`SIGKILL` the daemon process — no graceful teardown, no SQLite checkpoint, the socket left
on disk, the child `pi` orphaned:

```bash
kill -9 "$(pgrep -f 'packages/daemon/src/run.ts')"
pkill -9 -f 'pi --mode rpc' || true          # reap the orphaned agent from the killed daemon
```

In the app, **Mission Control** stops updating and the session channel drops. The app's
board feed reconnects on its own; the session-channel backend **re-dials** (CE4.2) — the
shell should show "Connecting…" rather than staying stuck on a dead backend.

### 5. RESTART the daemon on the SAME files

```bash
bun packages/daemon/src/run.ts
```

On boot, `unlinkStaleSocket` removes the crashed daemon's stale socket (its owner is dead,
the probe connection is refused) and rebinds; SQLite **replays the WAL** and recovers the
committed state; `StartupReconcile` finds the `running` Job under the still-active
workstream and **re-dispatches** it, re-driving the prompt on the **same** session id. Watch
the daemon's `startup reconcile complete` log line report the resumed Job.

### 6. Confirm the work continued — no loss, no duplication

- **App resyncs** — the app re-dials the restarted daemon; **Mission Control** repopulates
  from the snapshot + offset replay, and the session channel resolves the resumed session
  with no manual retry.
- **The agent finishes** — the resumed `pi` runs to a real terminal result and opens (or,
  if it had already opened one before the kill, continues toward) the real PR. The **Job**
  reaches `succeeded` with a `transcriptRef`.
- **1 Job = 1 session** — exactly one session row for the Job, re-attached under the id from
  before the kill (not a new one):

  ```bash
  sqlite3 "$SPRINTER_DB" "select id, status from sessions;"     # one row, same id, terminal
  sqlite3 "$SPRINTER_DB" "select id, status from jobs;"         # succeeded
  ```

> **Real-`pi` terminal-result note (CE1.1-F1).** With the real binary, the settle-watcher
> is armed only after the prompt is acked, and output can arrive before `Completed`. Confirm
> the resumed session reaches its terminal cleanly (the Job flips to `succeeded`/`failed`,
> not stuck `running`). This is the real-`pi` behavior the deterministic test's scripted
> stand-in cannot reproduce.

### 7. Tear down

Stop the app (closing the window releases the transport thread + fd via the scene-lifecycle
`stop()`), `Ctrl-C` the daemon, and delete the sandbox state:

```bash
rm -rf "$PWD/.sprinter"
```

## Differences from the automated test (why both exist)

| Aspect | Automated restart test | This runbook |
|---|---|---|
| Restart | layer-scope teardown + rebuild (same DB + socket) | real process **SIGKILL** + relaunch |
| WAL recovery | committed mid-write via a second connection (no checkpoint) | real on-disk WAL replay after a crash |
| Agent | **scripted `pi`** (canned transcript, deterministic) | **real `pi`** binary, real terminal result |
| Code host | **sandboxed in-process `Repository`** (no network) | **real GitHub** (throwaway repo) |
| Socket rebind | fresh bind on the freed path | `unlinkStaleSocket` removes the crashed daemon's leftover |
| Runs in CI | yes (deterministic, bounded, hermetic) | no (fires a real agent + real PR) |

The automated test is the gate; this runbook is the periodic real-world confidence check
the automated test deliberately cannot be.
