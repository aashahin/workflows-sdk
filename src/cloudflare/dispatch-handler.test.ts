import { describe, expect, test } from "bun:test";
import { defineWorkflow, defineWorkflowRegistry } from "../index";
import { createCloudflareDispatchHandler } from "./dispatch-handler";

describe("createCloudflareDispatchHandler", () => {
  test("fails closed when configured bearer auth resolves empty", async () => {
    const workflow = defineWorkflow("email/send", { run: () => undefined });
    const handler = createCloudflareDispatchHandler({
      registry: defineWorkflowRegistry([workflow]),
      auth: {
        bearerToken: () => undefined,
      },
      resolveWorkflow() {
        return { create: async () => ({}) };
      },
    });

    const response = await handler.fetch(
      new Request("https://example.com/dispatch", {
        method: "POST",
        body: JSON.stringify({ events: [] }),
      }),
      {},
    );

    expect(response.status).toBe(500);
  });

  test("fails closed when configured bearer auth is an empty string", async () => {
    const workflow = defineWorkflow("email/send", { run: () => undefined });
    const handler = createCloudflareDispatchHandler({
      registry: defineWorkflowRegistry([workflow]),
      auth: {
        bearerToken: "",
      },
      resolveWorkflow() {
        return { create: async () => ({}) };
      },
    });

    const response = await handler.fetch(
      new Request("https://example.com/dispatch", {
        method: "POST",
        body: JSON.stringify({ events: [] }),
      }),
      {},
    );

    expect(response.status).toBe(500);
  });

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

  test("scheduled cron validates payloads before creating instances", async () => {
    const workflow = defineWorkflow("daily", {
      cron: [{ name: "nine", schedule: "0 9 * * *" }],
      schema: {
        parse() {
          throw new Error("Workflow payload validation failed: tenantId is required");
        },
      },
      run: () => undefined,
    });
    let created = 0;
    const handler = createCloudflareDispatchHandler({
      registry: defineWorkflowRegistry([workflow]),
      now: () => new Date("2026-05-24T09:05:00.000Z"),
      resolveWorkflow() {
        return {
          create: async () => {
            created++;
            return {};
          },
        };
      },
    });

    await expect(handler.scheduled({}, {})).rejects.toThrow(
      "Workflow payload validation failed: tenantId is required",
    );
    expect(created).toBe(0);
  });

  test("rejects oversized dispatch requests before parsing JSON", async () => {
    const workflow = defineWorkflow("email/send", { run: () => undefined });
    const handler = createCloudflareDispatchHandler({
      registry: defineWorkflowRegistry([workflow]),
      maxRequestBytes: 10,
      resolveWorkflow() {
        return { create: async () => ({}) };
      },
    });

    const response = await handler.fetch(
      new Request("https://example.com/dispatch", {
        method: "POST",
        headers: { "content-length": "11" },
        body: "{}",
      }),
      {},
    );

    expect(response.status).toBe(413);
  });

  test("rejects oversized streamed dispatch requests without content-length", async () => {
    const workflow = defineWorkflow("email/send", { run: () => undefined });
    const handler = createCloudflareDispatchHandler({
      registry: defineWorkflowRegistry([workflow]),
      maxRequestBytes: 10,
      resolveWorkflow() {
        return { create: async () => ({}) };
      },
    });

    const response = await handler.fetch(
      new Request("https://example.com/dispatch", {
        method: "POST",
        body: JSON.stringify({ events: [] }),
      }),
      {},
    );

    expect(response.status).toBe(413);
  });

  test("rate limits dispatch requests per isolate", async () => {
    const workflow = defineWorkflow("email/send", { run: () => undefined });
    const handler = createCloudflareDispatchHandler({
      registry: defineWorkflowRegistry([workflow]),
      rateLimit: { max: 1, windowMs: 60_000 },
      resolveWorkflow() {
        return { create: async () => ({}) };
      },
    });
    const body = JSON.stringify({
      events: [
        {
          id: "wf_1",
          name: "email/send",
          payload: {},
          traceId: "trace",
          idempotencyKey: "idem",
          createdAt: new Date().toISOString(),
        },
      ],
    });

    const first = await handler.fetch(
      new Request("https://example.com/dispatch", {
        method: "POST",
        body,
      }),
      {},
    );
    const second = await handler.fetch(
      new Request("https://example.com/dispatch", {
        method: "POST",
        body,
      }),
      {},
    );

    expect(first.status).toBe(200);
    expect(second.status).toBe(429);
  });

  test("dispatch groups events by binding and uses createBatch", async () => {
    const first = defineWorkflow("first", { run: () => undefined });
    const second = defineWorkflow("second", { run: () => undefined });
    const created: unknown[] = [];
    const binding = {
      create: async () => {
        throw new Error("create should not be called");
      },
      createBatch: async (batch: unknown[]) => {
        created.push(...batch);
        return batch;
      },
    };
    const handler = createCloudflareDispatchHandler({
      registry: defineWorkflowRegistry([first, second]),
      resolveWorkflow() {
        return binding;
      },
    });

    const response = await handler.fetch(
      new Request("https://example.com/dispatch", {
        method: "POST",
        body: JSON.stringify({
          events: [
            {
              id: "wf_1",
              name: "first",
              payload: {},
              traceId: "trace_1",
              idempotencyKey: "idem_1",
              createdAt: new Date().toISOString(),
            },
            {
              id: "wf_2",
              name: "second",
              payload: {},
              traceId: "trace_2",
              idempotencyKey: "idem_2",
              createdAt: new Date().toISOString(),
            },
          ],
        }),
      }),
      {},
    );

    expect(response.status).toBe(200);
    expect(created).toHaveLength(2);
  });

  test("reports createBatch items that did not create an instance", async () => {
    const first = defineWorkflow("first", { run: () => undefined });
    const second = defineWorkflow("second", { run: () => undefined });
    const handler = createCloudflareDispatchHandler({
      registry: defineWorkflowRegistry([first, second]),
      resolveWorkflow() {
        return {
          create: async () => {
            throw new Error("create should not be called");
          },
          createBatch: async () => [{ id: "wf_1" }],
        };
      },
    });

    const response = await handler.fetch(
      new Request("https://example.com/dispatch", {
        method: "POST",
        body: JSON.stringify({
          events: [
            {
              id: "wf_1",
              name: "first",
              payload: {},
              traceId: "trace_1",
              idempotencyKey: "idem_1",
              createdAt: new Date().toISOString(),
            },
            {
              id: "wf_2",
              name: "second",
              payload: {},
              traceId: "trace_2",
              idempotencyKey: "idem_2",
              createdAt: new Date().toISOString(),
            },
          ],
        }),
      }),
      {},
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({
      ids: ["wf_1"],
      errors: [
        {
          id: "wf_2",
          error: "Cloudflare Workflow instance was not created by createBatch",
        },
      ],
    });
  });

  test("returns per-item errors for malformed dispatch events", async () => {
    const workflow = defineWorkflow("email/send", { run: () => undefined });
    const handler = createCloudflareDispatchHandler({
      registry: defineWorkflowRegistry([workflow]),
      resolveWorkflow() {
        return { create: async () => ({}) };
      },
    });

    const response = await handler.fetch(
      new Request("https://example.com/dispatch", {
        method: "POST",
        body: JSON.stringify({
          events: [
            null,
            {
              id: "wf_bad",
              name: "email/send",
              payload: {},
              traceId: "trace",
              createdAt: new Date().toISOString(),
            },
          ],
        }),
      }),
      {},
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({
      ids: [],
      instances: [],
      errors: [
        { id: "unknown", error: "Invalid event structure at index 0" },
        { id: "wf_bad", error: "Invalid event structure at index 1" },
      ],
    });
  });

  test("validates workflow payloads before creating Cloudflare instances", async () => {
    const workflow = defineWorkflow("email/send", {
      schema: {
        parse(value) {
          if (
            typeof value !== "object" ||
            value === null ||
            typeof (value as { email?: unknown }).email !== "string"
          ) {
            throw new Error("Workflow payload validation failed: email is required");
          }
          return value as Record<string, unknown>;
        },
      },
      run: () => undefined,
    });
    let created = 0;
    const handler = createCloudflareDispatchHandler({
      registry: defineWorkflowRegistry([workflow]),
      resolveWorkflow() {
        return {
          create: async () => {
            created++;
            return {};
          },
        };
      },
    });

    const response = await handler.fetch(
      new Request("https://example.com/dispatch", {
        method: "POST",
        body: JSON.stringify({
          events: [
            {
              id: "wf_bad",
              name: "email/send",
              payload: {},
              traceId: "trace",
              idempotencyKey: "idem",
              createdAt: new Date().toISOString(),
            },
          ],
        }),
      }),
      {},
    );

    expect(response.status).toBe(200);
    expect(created).toBe(0);
    await expect(response.json()).resolves.toMatchObject({
      ids: [],
      errors: [
        {
          id: "wf_bad",
          error: "Workflow payload validation failed: email is required",
        },
      ],
    });
  });
});
