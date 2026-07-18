/**
 * The publishing `StateStore` decorator (Track A, task AE4.1) — the seam that
 * makes the daemon maximally reactive (D17 / INV-REACTIVE).
 *
 * It wraps any {@link StateStore} adapter and, on every successful `put*`
 * mutation, fans out the matching owned {@link WorkGraphEvent} delta to the
 * {@link WorkGraphEvents} feed. Because it is itself a `StateStore` (same port),
 * *every* consumer that writes through the port — the AE4.1 command handlers AND
 * the `@sprinter/job` `JobRunner` — publishes deltas for free, with no consumer
 * knowing the feed exists (INV-PORT). Reads and the durable event log pass
 * straight through unchanged.
 *
 * The decorator is composed with {@link layerPublishing}: it both requires a base
 * `StateStore` (supplied via `Layer.provide`) and provides the decorated
 * `StateStore` to consumers — the standard service-decorator shape.
 */
import { Context, Effect, Layer } from "effect";
import { StateStore, type StateStoreError } from "@sprinter/state";
import { WorkGraphEvents } from "./work-graph-events.ts";

/** Build the decorated store shape: delegate to `base`, publishing a delta after each `put*`. */
const publishing = (
  base: Context.Service.Shape<typeof StateStore>,
  feed: Context.Service.Shape<typeof WorkGraphEvents>,
): Context.Service.Shape<typeof StateStore> =>
  StateStore.of({
    workGraph: {
      putWorkstream: (workstream) =>
        base.workGraph
          .putWorkstream(workstream)
          .pipe(Effect.tap(() => feed.publish({ _tag: "WorkstreamChanged", workstream }))),
      putEpic: (epic) =>
        base.workGraph
          .putEpic(epic)
          .pipe(Effect.tap(() => feed.publish({ _tag: "EpicChanged", epic }))),
      putIssue: (issue) =>
        base.workGraph
          .putIssue(issue)
          .pipe(Effect.tap(() => feed.publish({ _tag: "IssueChanged", issue }))),
      getWorkstream: base.workGraph.getWorkstream,
      getEpic: base.workGraph.getEpic,
      getIssue: base.workGraph.getIssue,
      listWorkstreams: base.workGraph.listWorkstreams,
      listEpics: base.workGraph.listEpics,
      listIssues: base.workGraph.listIssues,
    },
    jobs: {
      putJob: (job) =>
        base.jobs.putJob(job).pipe(Effect.tap(() => feed.publish({ _tag: "JobChanged", job }))),
      getJob: base.jobs.getJob,
      listJobsForIssue: base.jobs.listJobsForIssue,
      putSession: (session) =>
        base.jobs
          .putSession(session)
          .pipe(Effect.tap(() => feed.publish({ _tag: "SessionChanged", session }))),
      getSession: base.jobs.getSession,
      getSessionForJob: base.jobs.getSessionForJob,
    },
    events: base.events,
    // Transactions pass straight through to the backing; the live fan-out (the
    // `tap(publish)` above) stays OUTSIDE the transaction — a delta is published
    // only after the durable write commits, never on a rolled-back one.
    withTransaction: base.withTransaction,
  });

/**
 * Decorate `baseLayer` with delta publishing: the resulting layer provides a
 * {@link StateStore} that publishes every mutation to the {@link WorkGraphEvents}
 * feed. The base store is wired in via `Layer.provide`, so consumers see only the
 * decorated port (INV-PORT).
 */
export const layerPublishing = <RIn>(
  baseLayer: Layer.Layer<StateStore, StateStoreError, RIn>,
): Layer.Layer<StateStore, StateStoreError, RIn | WorkGraphEvents> =>
  Layer.effect(
    StateStore,
    Effect.gen(function* () {
      const base = yield* StateStore;
      const feed = yield* WorkGraphEvents;
      return publishing(base, feed);
    }),
  ).pipe(Layer.provide(baseLayer));
