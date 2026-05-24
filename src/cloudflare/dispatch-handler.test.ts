import { describe, expect, test } from "bun:test";
import { defineWorkflow, defineWorkflowRegistry } from "../index";
import { createCloudflareDispatchHandler } from "./dispatch-handler";

describe("createCloudflareDispatchHandler", () => {
  test("can find instance status by id without an in-memory name hint", async () => {
    const first = defineWorkflow("first", { run: () => undefined });
    const second = defineWorkflow("second", { run: () => undefined });
    const handler = createCloudflareDispatchHandler({
      registry: defineWorkflowRegistry([first, second]),
      resolveWorkflow(name) {
        return {
          create: async () => ({}),
          get: async () => ({
            status: async () =>
              name === "second"
                ? { status: "paused", output: { ok: true } }
                : { status: "unknown" },
          }),
        };
      },
    });

    const response = await handler.fetch(
      new Request("https://example.com/status/wf_1"),
      {},
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({
      id: "wf_1",
      name: "second",
      status: "paused",
      output: { ok: true },
    });
  });

  test("continues status search when Cloudflare get throws for a missing instance", async () => {
    const first = defineWorkflow("first", { run: () => undefined });
    const second = defineWorkflow("second", { run: () => undefined });
    const handler = createCloudflareDispatchHandler({
      registry: defineWorkflowRegistry([first, second]),
      resolveWorkflow(name) {
        return {
          create: async () => ({}),
          get: async () => {
            if (name === "first") throw new Error("instance not found");
            return {
              status: async () => ({ status: "complete" }),
            };
          },
        };
      },
    });

    const response = await handler.fetch(
      new Request("https://example.com/status/wf_1"),
      {},
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({
      id: "wf_1",
      name: "second",
      status: "complete",
    });
  });

  test("scheduled cron uses idempotent createBatch when available", async () => {
    const workflow = defineWorkflow("daily", {
      cron: [{ name: "nine", schedule: "0 9 * * *" }],
      run: () => undefined,
    });
    const created: unknown[] = [];
    const handler = createCloudflareDispatchHandler({
      registry: defineWorkflowRegistry([workflow]),
      now: () => new Date("2026-05-24T09:05:00.000Z"),
      resolveWorkflow() {
        return {
          create: async () => {
            throw new Error("create should not be called");
          },
          createBatch: async (batch) => {
            created.push(...batch);
            return batch;
          },
        };
      },
    });

    await expect(handler.scheduled({}, {})).resolves.toEqual({ dispatched: 1 });
    expect(created).toHaveLength(1);
  });

  test("scheduled cron uses controller scheduledTime when provided", async () => {
    const workflow = defineWorkflow("daily", {
      cron: [{ name: "nine", schedule: "0 9 * * *" }],
      run: () => undefined,
    });
    const created: Array<{ params?: unknown }> = [];
    const handler = createCloudflareDispatchHandler({
      registry: defineWorkflowRegistry([workflow]),
      now: () => new Date("2026-05-24T09:59:00.000Z"),
      resolveWorkflow() {
        return {
          create: async () => {
            throw new Error("create should not be called");
          },
          createBatch: async (batch) => {
            created.push(...batch);
            return batch;
          },
        };
      },
    });

    await expect(
      handler.scheduled({ scheduledTime: Date.parse("2026-05-24T09:05:00.000Z") }, {}),
    ).resolves.toEqual({ dispatched: 1 });
    expect((created[0]?.params as { scheduledAt?: string }).scheduledAt).toBe(
      "2026-05-24T09:00:00.000Z",
    );
  });
});
