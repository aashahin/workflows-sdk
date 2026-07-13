import { describe, expect, test } from "bun:test";
import { defineWorkflow, defineWorkflowRegistry } from "../index";
import {
  createCloudflareDispatchHandler,
  createCloudflareWorkflowDispatch,
} from "./dispatch-handler";

describe("createCloudflareWorkflowDispatch", () => {
  test("creates idempotent Cloudflare instances for ctx.dispatch", async () => {
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
    const created: Array<{ id: string; params?: unknown }> = [];
    const dispatch = createCloudflareWorkflowDispatch({
      registry: defineWorkflowRegistry([workflow]),
      now: () => new Date("2026-05-24T09:00:00.000Z"),
      idGenerator: () => "wf_nested",
      traceIdGenerator: () => "trace_nested",
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

    const result = await dispatch(
      "email/send",
      { email: "student@example.com" },
      {
        delayMs: 60_000,
        idempotencyKey: "idem_nested",
        metadata: { source: "parent" },
      },
      {},
    );

    expect(result.ids).toEqual(["wf_nested"]);
    expect(result.instances?.[0]).toMatchObject({
      id: "wf_nested",
      name: "email/send",
      status: "scheduled",
      traceId: "trace_nested",
      idempotencyKey: "idem_nested",
      scheduledAt: "2026-05-24T09:01:00.000Z",
    });
    expect(created).toHaveLength(1);
    expect(created[0]?.params).toMatchObject({
      id: "wf_nested",
      name: "email/send",
      payload: { email: "student@example.com" },
      metadata: { source: "parent" },
    });
  });

  test("validates nested dispatch payloads before creating instances", async () => {
    const workflow = defineWorkflow("email/send", {
      schema: {
        parse() {
          throw new Error("Workflow payload validation failed: email is required");
        },
      },
      run: () => undefined,
    });
    let created = 0;
    const dispatch = createCloudflareWorkflowDispatch({
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

    await expect(dispatch("email/send", {}, undefined, {})).rejects.toThrow(
      "Workflow payload validation failed: email is required",
    );
    expect(created).toBe(0);
  });
});

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

  test("returns a controlled 502 when a status binding fails unexpectedly", async () => {
    const workflow = defineWorkflow("email/send", { run: () => undefined });
    const handler = createCloudflareDispatchHandler({
      registry: defineWorkflowRegistry([workflow]),
      resolveWorkflow() {
        return {
          create: async () => ({}),
          get: async () => {
            throw new Error("Cloudflare API unavailable");
          },
        };
      },
    });

    const response = await handler.fetch(
      new Request("https://example.com/status/wf_1?name=email/send"),
      {},
    );

    expect(response.status).toBe(502);
    await expect(response.json()).resolves.toMatchObject({
      error: "Cloudflare API unavailable",
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

    await expect(handler.scheduled({}, {})).resolves.toEqual({
      dispatched: 1,
      errors: 0,
    });
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
    ).resolves.toEqual({ dispatched: 1, errors: 0 });
    expect((created[0]?.params as { scheduledAt?: string }).scheduledAt).toBe(
      "2026-05-24T09:00:00.000Z",
    );
  });

  test("scheduled cron isolates a failing run and reports it via errors count", async () => {
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

    // A payload validation failure no longer aborts the whole sweep; it is
    // isolated per-run, counted, and the handler resolves.
    await expect(handler.scheduled({}, {})).resolves.toEqual({
      dispatched: 0,
      errors: 1,
    });
    expect(created).toBe(0);
  });

  test("scheduled cron continues to remaining workflows after one throws", async () => {
    const failing = defineWorkflow("failing", {
      cron: [{ name: "nine", schedule: "0 9 * * *" }],
      schema: {
        parse() {
          throw new Error("Workflow payload validation failed");
        },
      },
      run: () => undefined,
    });
    const healthy = defineWorkflow("healthy", {
      cron: [{ name: "nine", schedule: "0 9 * * *" }],
      run: () => undefined,
    });
    const created: unknown[] = [];
    const handler = createCloudflareDispatchHandler({
      registry: defineWorkflowRegistry([failing, healthy]),
      now: () => new Date("2026-05-24T09:05:00.000Z"),
      resolveWorkflow() {
        return {
          create: async () => {
            throw new Error("create should not be called");
          },
          createBatch: async (batch: unknown[]) => {
            created.push(...batch);
            return batch;
          },
        };
      },
    });

    await expect(handler.scheduled({}, {})).resolves.toEqual({
      dispatched: 1,
      errors: 1,
    });
    expect(created).toHaveLength(1);
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

  test("treats createBatch-skipped (already existing) instances as idempotent successes", async () => {
    const first = defineWorkflow("first", { run: () => undefined });
    const second = defineWorkflow("second", { run: () => undefined });
    const handler = createCloudflareDispatchHandler({
      registry: defineWorkflowRegistry([first, second]),
      resolveWorkflow() {
        return {
          create: async () => {
            throw new Error("create should not be called");
          },
          // wf_2 already exists, so createBatch idempotently skips it and
          // excludes it from the result. That is a successful re-dispatch,
          // not a failure.
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
    const body = (await response.json()) as {
      ids: string[];
      errors?: unknown[];
    };
    expect(body.ids).toEqual(["wf_1", "wf_2"]);
    expect(body.errors).toBeUndefined();
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
