import { describe, expect, test } from "bun:test";
import { defineWorkflow, defineWorkflowRegistry } from "../index";
import { InMemoryWorkflowAdapter } from "../testing";
import { createRunContext, durationToMs, runWorkflowEnvelope } from "./runtime";
import type { WorkflowEventEnvelope } from "./types";

function makeEnvelope(
  name: string,
  overrides: Partial<WorkflowEventEnvelope> = {},
): WorkflowEventEnvelope {
  return {
    id: "wf_test",
    name,
    payload: {},
    traceId: "trace_test",
    idempotencyKey: "idem_test",
    createdAt: "2026-05-24T09:00:00.000Z",
    ...overrides,
  };
}

describe("durationToMs", () => {
  test("passes through numbers as milliseconds", () => {
    expect(durationToMs(1000)).toBe(1000);
    expect(durationToMs(-5)).toBe(0);
  });

  test("computes remaining time until a Date", () => {
    const future = new Date(Date.now() + 5_000);
    expect(durationToMs(future)).toBeGreaterThan(4_000);
    const past = new Date(Date.now() - 5_000);
    expect(durationToMs(past)).toBe(0);
  });

  test("parses unit-suffixed duration strings", () => {
    expect(durationToMs("5s")).toBe(5_000);
    expect(durationToMs("10 minutes")).toBe(600_000);
    expect(durationToMs("250ms")).toBe(250);
  });

  test("treats bare numeric strings as milliseconds, not calendar years", () => {
    expect(durationToMs("1000")).toBe(1000);
    expect(durationToMs("1")).toBe(1);
    expect(durationToMs("100")).toBe(100);
  });

  test("still parses clearly date-like strings via Date.parse", () => {
    const future = new Date(Date.now() + 10_000).toISOString();
    expect(durationToMs(future)).toBeGreaterThan(9_000);
  });

  test("throws on unsupported garbage strings", () => {
    expect(() => durationToMs("0 9 * * 1")).toThrow("Unsupported duration");
    expect(() => durationToMs("not-a-duration")).toThrow("Unsupported duration");
  });
});

describe("createRunContext dispatch client", () => {
  test("derives the client from options.clientConfig when no client is passed", async () => {
    const adapter = new InMemoryWorkflowAdapter();
    const workflow = defineWorkflow("wf/parent", { run: () => undefined });

    const ctx = createRunContext(makeEnvelope("wf/parent"), workflow, {
      registry: defineWorkflowRegistry([workflow]),
      clientConfig: { adapter, idGenerator: () => "wf_child" },
    });

    const result = await ctx.dispatch("wf/child", { tenantId: "t1" });
    expect(result.ids).toEqual(["wf_child"]);
    expect(adapter.instances.has("wf_child")).toBe(true);
  });

  test("throws when neither client nor clientConfig is available", async () => {
    const workflow = defineWorkflow("wf/parent", { run: () => undefined });
    const ctx = createRunContext(makeEnvelope("wf/parent"), workflow, {
      registry: defineWorkflowRegistry([workflow]),
    });

    expect(() => ctx.dispatch("wf/child", {})).toThrow(
      "ctx.dispatch requires a workflow client",
    );
  });
});

describe("ctx.step timeout + retry", () => {
  test("retries after a timeout and a late attempt cannot clobber the result", async () => {
    const adapter = new InMemoryWorkflowAdapter();
    let attempts = 0;

    const workflow = defineWorkflow("wf/timeout", {
      run: async (ctx) =>
        ctx.step(
          "slow",
          async () => {
            attempts += 1;
            if (attempts === 1) {
              // First attempt outlives the timeout, then completes late.
              await new Promise((resolve) => setTimeout(resolve, 50));
              return "stale";
            }
            return "fresh";
          },
          {
            timeoutMs: 10,
            retry: {
              maxAttempts: 2,
              initialIntervalMs: 1,
              multiplier: 1,
              maxIntervalMs: 1,
            },
          },
        ),
    });

    const value = await runWorkflowEnvelope(makeEnvelope("wf/timeout"), {
      registry: defineWorkflowRegistry([workflow]),
      getStepResult: adapter.getStepResult.bind(adapter),
      hasStepResult: adapter.hasStepResult.bind(adapter),
      saveStepResult: adapter.saveStepResult.bind(adapter),
    });

    expect(value).toBe("fresh");
    // Give the timed-out first attempt time to settle late; it must not
    // overwrite the persisted winning result.
    await new Promise((resolve) => setTimeout(resolve, 80));
    expect(await adapter.getStepResult("wf_test", "slow")).toBe("fresh");
  });
});
