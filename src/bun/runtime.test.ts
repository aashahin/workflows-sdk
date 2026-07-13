import { describe, expect, test } from "bun:test";
import { defineWorkflow, defineWorkflowRegistry, durationToMs } from "../index";
import { BunSqliteWorkflowAdapter } from "./sqlite-adapter";
import { createBunWorkflowRuntime } from "./runtime";

describe("BunWorkflowRuntime", () => {
  test("dispatches and executes queued SQLite workflows", async () => {
    const adapter = new BunSqliteWorkflowAdapter({ path: ":memory:" });
    const workflow = defineWorkflow("counter/increment", {
      async run(ctx, payload) {
        const value = await ctx.step("increment", () =>
          Number(payload.value) + 1,
        );
        return { value };
      },
    });
    const runtime = createBunWorkflowRuntime({
      adapter,
      registry: defineWorkflowRegistry([workflow]),
    });

    const result = await runtime.client.dispatch("counter/increment", {
      value: 41,
    });
    await runtime.processReady();

    const instance = await adapter.getInstance(result.ids[0]!);
    expect(instance?.status).toBe("complete");
    expect(instance?.output).toEqual({ value: 42 });
    expect(await adapter.getStepResult(result.ids[0]!, "increment")).toBe(42);
  });

  test("deduplicates explicit idempotency keys", async () => {
    const adapter = new BunSqliteWorkflowAdapter({ path: ":memory:" });
    const workflow = defineWorkflow("noop", {
      run: () => undefined,
    });
    const runtime = createBunWorkflowRuntime({
      adapter,
      registry: defineWorkflowRegistry([workflow]),
    });

    const first = await runtime.client.dispatch("noop", {}, {
      idempotencyKey: "same-key",
    });
    const second = await runtime.client.dispatch("noop", {}, {
      idempotencyKey: "same-key",
    });

    expect(second.ids).toEqual(first.ids);
  });

  test("injects services into workflow execution", async () => {
    interface Services {
      backend: {
        execute(path: string): Promise<string>;
      };
    }

    const adapter = new BunSqliteWorkflowAdapter({ path: ":memory:" });
    const workflow = defineWorkflow<
      "backend/call",
      Record<string, unknown>,
      string,
      Services
    >("backend/call", {
      run(ctx) {
        return ctx.services.backend.execute("backend/call");
      },
    });
    const runtime = createBunWorkflowRuntime({
      adapter,
      registry: defineWorkflowRegistry([workflow]),
      services: {
        backend: {
          async execute(path: string) {
            return `called:${path}`;
          },
        },
      },
    });

    const result = await runtime.client.dispatch("backend/call", {});
    await runtime.processReady();

    await expect(adapter.getInstance(result.ids[0]!)).resolves.toMatchObject({
      status: "complete",
      output: "called:backend/call",
    });
  });

  test("caches undefined step results", async () => {
    const adapter = new BunSqliteWorkflowAdapter({ path: ":memory:" });
    let calls = 0;
    const workflow = defineWorkflow("side-effect", {
      async run(ctx) {
        return ctx.step("side-effect", () => {
          calls++;
          return undefined;
        });
      },
    });
    const runtime = createBunWorkflowRuntime({
      adapter,
      registry: defineWorkflowRegistry([workflow]),
    });

    await adapter.saveStepResult("wf_undefined", "side-effect", undefined);
    await expect(
      adapter.getStepResult("wf_undefined", "side-effect"),
    ).resolves.toBeUndefined();
    await expect(
      adapter.hasStepResult("wf_undefined", "side-effect"),
    ).resolves.toBe(true);
    await runtime.runEnvelope({
      id: "wf_undefined",
      name: "side-effect",
      payload: {},
      traceId: "trace",
      idempotencyKey: "idem",
      createdAt: new Date().toISOString(),
    });
    expect(calls).toBe(0);

    const row = adapter.db
      .query(
        `select result_json from workflow_step_results
         where instance_id = ? and step_name = ?`,
      )
      .get("wf_undefined", "side-effect") as { result_json: string } | null;
    expect(row?.result_json).toBe('{"hasValue":true}');
  });

  test("recovers stale running SQLite workflows", async () => {
    const adapter = new BunSqliteWorkflowAdapter({ path: ":memory:" });
    const workflow = defineWorkflow("noop", {
      run: () => undefined,
    });
    const runtime = createBunWorkflowRuntime({
      adapter,
      registry: defineWorkflowRegistry([workflow]),
    });

    const result = await runtime.client.dispatch("noop", {});
    await adapter.claimNext(new Date("2026-05-24T09:00:00.000Z"));
    expect((await adapter.getInstance(result.ids[0]!))?.status).toBe("running");

    const recovered = await adapter.recoverStalled(new Date(Date.now() + 1_000));
    expect(recovered).toBe(1);
    expect((await adapter.getInstance(result.ids[0]!))?.status).toBe("queued");
  });

  test("requeues failed workflows according to workflow retry policy", async () => {
    const adapter = new BunSqliteWorkflowAdapter({ path: ":memory:" });
    let calls = 0;
    let now = new Date("2026-05-24T09:00:00.000Z");
    const workflow = defineWorkflow("flaky", {
      retry: {
        maxAttempts: 1,
        initialIntervalMs: 1_000,
        multiplier: 1,
        maxIntervalMs: 1_000,
      },
      run: () => {
        calls++;
        if (calls === 1) throw new Error("temporary");
        return "ok";
      },
    });
    const runtime = createBunWorkflowRuntime({
      adapter,
      registry: defineWorkflowRegistry([workflow]),
      now: () => now,
    });

    const result = await runtime.client.dispatch("flaky", {});
    await runtime.processReady(now);
    expect((await adapter.getInstance(result.ids[0]!))?.status).toBe("scheduled");

    now = new Date("2026-05-24T09:00:01.000Z");
    await runtime.processReady(now);

    expect(calls).toBe(2);
    expect(await adapter.getInstance(result.ids[0]!)).toMatchObject({
      status: "complete",
      output: "ok",
    });
  });

  test("scheduled handler filters OS cron ticks by controller cron expression", async () => {
    const adapter = new BunSqliteWorkflowAdapter({ path: ":memory:" });
    const morning = defineWorkflow("morning", {
      cron: [{ name: "morning", schedule: "0 9 * * *" }],
      run: () => undefined,
    });
    const evening = defineWorkflow("evening", {
      cron: [{ name: "evening", schedule: "0 18 * * *" }],
      run: () => undefined,
    });
    const runtime = createBunWorkflowRuntime({
      adapter,
      registry: defineWorkflowRegistry([morning, evening]),
      scheduler: { mode: "os" },
      now: () => new Date("2026-05-24T09:00:00.000Z"),
    });

    const result = await runtime.scheduled({
      cron: "0 18 * * *",
      scheduledTime: Date.parse("2026-05-24T18:00:00.000Z"),
    });

    expect(result.claimed).toBe(1);
    const claimed = await adapter.claimNext(new Date("2026-05-24T18:00:00.000Z"));
    expect(claimed?.name).toBe("evening");
  });

  test("parses Cloudflare-style sleep duration strings in Bun runtime", () => {
    expect(durationToMs("1 hour")).toBe(3_600_000);
    expect(durationToMs("30 minutes")).toBe(1_800_000);
    expect(durationToMs("2 weeks")).toBe(14 * 86_400_000);
  });

  test("dead-letters an unregistered workflow instead of throwing again", async () => {
    const adapter = new BunSqliteWorkflowAdapter({ path: ":memory:" });
    const runtime = createBunWorkflowRuntime({
      adapter,
      registry: defineWorkflowRegistry([]),
    });

    // An envelope whose workflow name is not (or no longer) registered.
    await adapter.dispatch({
      id: "ghost-1",
      name: "ghost/workflow",
      payload: {},
      traceId: "trace",
      idempotencyKey: "idem-ghost",
      createdAt: new Date().toISOString(),
    });

    const processed = await runtime.processReady();
    expect(processed).toBe(1);

    const instance = await adapter.getInstance("ghost-1");
    expect(instance?.status).toBe("dead");

    const dead = adapter.db
      .query(`select id from workflow_dead_letters where instance_id = ?`)
      .get("ghost-1");
    expect(dead).not.toBeNull();
  });

  test("does not requeue a succeeded workflow when persisting completion fails", async () => {
    class FailingCompleteAdapter extends BunSqliteWorkflowAdapter {
      completeCalls = 0;
      override async updateInstance(
        id: string,
        status: Parameters<BunSqliteWorkflowAdapter["updateInstance"]>[1],
        patch?: Parameters<BunSqliteWorkflowAdapter["updateInstance"]>[2],
      ): Promise<void> {
        if (status === "complete") {
          this.completeCalls++;
          throw new Error("db write failed");
        }
        return super.updateInstance(id, status, patch);
      }
    }

    const adapter = new FailingCompleteAdapter({ path: ":memory:" });
    let runs = 0;
    const workflow = defineWorkflow("persist-fail", {
      run: () => {
        runs++;
        return "ok";
      },
    });
    const runtime = createBunWorkflowRuntime({
      adapter,
      registry: defineWorkflowRegistry([workflow]),
    });

    const result = await runtime.client.dispatch("persist-fail", {});
    await runtime.processReady();

    // Ran once, tried to persist completion twice (initial + one retry).
    expect(runs).toBe(1);
    expect(adapter.completeCalls).toBe(2);

    // The succeeded run is NOT requeued or dead-lettered — it stays "running"
    // and stalled recovery is the fallback, so a fresh drain re-runs nothing.
    expect((await adapter.getInstance(result.ids[0]!))?.status).toBe("running");
    await runtime.processReady();
    expect(runs).toBe(1);
  });

  test("stop() waits for the in-flight drain to finish", async () => {
    const adapter = new BunSqliteWorkflowAdapter({ path: ":memory:" });
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let completed = false;
    const workflow = defineWorkflow("slow", {
      async run() {
        await gate;
        completed = true;
        return "done";
      },
    });
    const runtime = createBunWorkflowRuntime({
      adapter,
      registry: defineWorkflowRegistry([workflow]),
    });

    const result = await runtime.client.dispatch("slow", {});
    const draining = runtime.processReady();
    // Let the drain claim the job and enter the (blocked) run().
    await Promise.resolve();

    const stopPromise = runtime.stop();
    release();
    await stopPromise;

    // stop() must not resolve until the running job has finished.
    expect(completed).toBe(true);
    expect((await adapter.getInstance(result.ids[0]!))?.status).toBe("complete");
    await draining;
  });
});
