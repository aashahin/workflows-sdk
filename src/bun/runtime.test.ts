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
});
