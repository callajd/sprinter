/**
 * The runnable daemon process entrypoint (CE1.2). It is deliberately a THIN shell
 * over the tested composition root in `./main.ts`: read the environment into a
 * {@link DaemonConfig}, launch the {@link mainLayer} graph, and hand it to Bun's
 * `runMain` (signal-aware, so SIGINT/SIGTERM tears the daemon down cleanly).
 *
 * This is the only module that binds a real socket and runs forever, so it cannot
 * be exercised deterministically in the offline suite — it is excluded from the
 * coverage gate (see `vitest.config.ts`) and verified by the documented manual smoke
 * step (PR body). Every unit of LOGIC it touches lives in `./main.ts` and is tested
 * there (composition, config resolution, the served handlers via `RpcTest`).
 */
import { Layer } from "effect";
import { BunRuntime } from "@effect/platform-bun";
import { configFromEnv, mainLayer } from "./main.ts";

BunRuntime.runMain(Layer.launch(mainLayer(configFromEnv(process.env))));
