import { describe, expect, test } from "bun:test";
import { defineWorkflow, defineWorkflowRegistry } from "../index";
import type { DispatchOptions } from "../core/types";
import { createCloudflareWorkflowEntrypoint } from "./runner";

describe("createCloudflareWorkflowEntrypoint", () => {
  test("sleeps until scheduledAt and forwards step options", async () => {
    const workflow = defineWorkflow("email/send", {
      async run(ctx) {
        return ctx.step(
          "send",
          () => "ok",
          {
            retry: {
              maxAttempts: 2,
              initialIntervalMs: 1_000,
              multiplier: 2,
              maxIntervalMs: 30_000,
            },
            timeoutMs: 5_000,
          },
        );
      },
    });
    class Base {
      env = {};
    }
    const Entrypoint = createCloudflareWorkflowEntrypoint(Base, {
      registry: defineWorkflowRegistry([workflow]),
    });
    const calls: Array<{ name: string; value?: unknown }> = [];
    const instance = new Entrypoint();

    const result = await instance.run(
      {
        payload: {
          id: "wf_1",
          name: "email/send",
          payload: {},
          traceId: "trace",
          idempotencyKey: "idem",
          scheduledAt: new Date(Date.now() + 1_000).toISOString(),
          createdAt: new Date().toISOString(),
        },
      },
      {
        do<T>(name: string, optionsOrFn: unknown, fn?: () => Promise<T> | T): Promise<T> {
          calls.push({ name, value: typeof optionsOrFn === "function" ? null : optionsOrFn });
          const operation = (fn ?? optionsOrFn) as () => Promise<T> | T;
          return Promise.resolve(operation());
        },
        sleep(name, duration) {
          calls.push({ name, value: duration });
          return Promise.resolve();
        },
        sleepUntil(name, timestamp) {
          calls.push({ name, value: timestamp });
          return Promise.resolve();
        },
      },
    );

    expect(result).toBe("ok");
    expect(calls[0]?.name).toBe("sdk scheduledAt");
    const forwarded = calls[1]?.value as {
      retries: {
        limit: number;
        delay: (input: { ctx: { attempt: number }; error: Error }) => number;
      };
      timeout: number;
    };
    expect(calls[1]?.name).toBe("send");
    expect(forwarded.retries.limit).toBe(3);
    expect(forwarded.retries.delay({ ctx: { attempt: 1 }, error: new Error() })).toBe(1_000);
    expect(forwarded.retries.delay({ ctx: { attempt: 8 }, error: new Error() })).toBe(30_000);
    expect(forwarded.timeout).toBe(5_000);
  });

  test("uses WorkflowStepConfig constant backoff shape", async () => {
    const workflow = defineWorkflow("email/send", {
      async run(ctx) {
        return ctx.step(
          "send",
          () => "ok",
          {
            retry: {
              maxAttempts: 1,
              initialIntervalMs: 500,
              multiplier: 1,
              maxIntervalMs: 500,
            },
          },
        );
      },
    });
    class Base {
      env = {};
    }
    const Entrypoint = createCloudflareWorkflowEntrypoint(Base, {
      registry: defineWorkflowRegistry([workflow]),
    });
    const calls: Array<{ name: string; value?: unknown }> = [];

    await new Entrypoint().run(
      {
        payload: {
          id: "wf_1",
          name: "email/send",
          payload: {},
          traceId: "trace",
          idempotencyKey: "idem",
          createdAt: new Date().toISOString(),
        },
      },
      {
        do<T>(name: string, optionsOrFn: unknown, fn?: () => Promise<T> | T): Promise<T> {
          calls.push({ name, value: typeof optionsOrFn === "function" ? null : optionsOrFn });
          const operation = (fn ?? optionsOrFn) as () => Promise<T> | T;
          return Promise.resolve(operation());
        },
        sleep() {
          return Promise.resolve();
        },
      },
    );

    const forwarded = calls[0]?.value as {
      retries: {
        limit: number;
        delay: (input: { ctx: { attempt: number }; error: Error }) => number;
      };
    };
    expect(calls[0]?.name).toBe("send");
    expect(forwarded.retries.limit).toBe(2);
    expect(forwarded.retries.delay({ ctx: { attempt: 1 }, error: new Error() })).toBe(500);
    expect(forwarded.retries.delay({ ctx: { attempt: 9 }, error: new Error() })).toBe(500);
  });

  test("applies workflow-level retry and timeout defaults", async () => {
    const workflow = defineWorkflow("email/send", {
      retry: false,
      timeoutMs: 2_000,
      async run(ctx) {
        return ctx.step("send", () => "ok");
      },
    });
    class Base {
      env = {};
    }
    const Entrypoint = createCloudflareWorkflowEntrypoint(Base, {
      registry: defineWorkflowRegistry([workflow]),
    });
    const calls: Array<{ name: string; value?: unknown }> = [];

    await new Entrypoint().run(
      {
        payload: {
          id: "wf_1",
          name: "email/send",
          payload: {},
          traceId: "trace",
          idempotencyKey: "idem",
          createdAt: new Date().toISOString(),
        },
      },
      {
        do<T>(name: string, optionsOrFn: unknown, fn?: () => Promise<T> | T): Promise<T> {
          calls.push({ name, value: typeof optionsOrFn === "function" ? null : optionsOrFn });
          const operation = (fn ?? optionsOrFn) as () => Promise<T> | T;
          return Promise.resolve(operation());
        },
        sleep() {
          return Promise.resolve();
        },
      },
    );

    expect(calls[0]).toEqual({
      name: "send",
      value: {
        retries: {
          limit: 1,
          delay: 0,
          backoff: "constant",
        },
        timeout: 2_000,
      },
    });
  });

  test("injects runtime services into workflow context", async () => {
    interface Services {
      backend: {
        call(path: string): Promise<string>;
      };
    }

    const workflow = defineWorkflow<
      "email/send",
      Record<string, unknown>,
      string,
      Services
    >("email/send", {
      async run(ctx) {
        return ctx.services.backend.call("email/send");
      },
    });
    class Base {
      env = {};
    }
    const Entrypoint = createCloudflareWorkflowEntrypoint(Base, {
      registry: defineWorkflowRegistry([workflow]),
      services: {
        backend: {
          call: async (path: string) => `called:${path}`,
        },
      },
    });

    const result = await new Entrypoint().run(
      {
        payload: {
          id: "wf_1",
          name: "email/send",
          payload: {},
          traceId: "trace",
          idempotencyKey: "idem",
          createdAt: new Date().toISOString(),
        },
      },
      {
        do<T>(_name: string, optionsOrFn: unknown, fn?: () => Promise<T> | T): Promise<T> {
          const operation = (fn ?? optionsOrFn) as () => Promise<T> | T;
          return Promise.resolve(operation());
        },
        sleep() {
          return Promise.resolve();
        },
      },
    );

    expect(result).toBe("called:email/send");
  });

  test("accepts legacy eventName/data payloads for rollout compatibility", async () => {
    const workflow = defineWorkflow("email/send", {
      async run(ctx, payload) {
        return {
          id: ctx.event.id,
          name: ctx.event.name,
          payload,
          traceId: ctx.traceId,
        };
      },
    });
    class Base {
      env = {};
    }
    const Entrypoint = createCloudflareWorkflowEntrypoint(Base, {
      registry: defineWorkflowRegistry([workflow]),
    });

    const result = await new Entrypoint().run(
      {
        payload: {
          eventId: "legacy_1",
          eventName: "email/send",
          data: { tenantId: "tenant_1" },
          traceId: "trace_1",
          idempotencyKey: "idem_1",
        },
      },
      {
        do<T>(_name: string, optionsOrFn: unknown, fn?: () => Promise<T> | T): Promise<T> {
          const operation = (fn ?? optionsOrFn) as () => Promise<T> | T;
          return Promise.resolve(operation());
        },
        sleep() {
          return Promise.resolve();
        },
      },
    );

    expect(result).toEqual({
      id: "legacy_1",
      name: "email/send",
      payload: { tenantId: "tenant_1" },
      traceId: "trace_1",
    });
  });

  test("uses numeric milliseconds for subsecond Workflow sleeps", async () => {
    const workflow = defineWorkflow("maintenance/pause", {
      async run(ctx) {
        await ctx.sleep("short pause", 250);
        return "ok";
      },
    });
    class Base {
      env = {};
    }
    const Entrypoint = createCloudflareWorkflowEntrypoint(Base, {
      registry: defineWorkflowRegistry([workflow]),
    });
    const sleeps: unknown[] = [];

    await new Entrypoint().run(
      {
        payload: {
          id: "wf_sleep",
          name: "maintenance/pause",
          payload: {},
          traceId: "trace",
          idempotencyKey: "idem",
          createdAt: "2026-05-24T09:00:00.000Z",
        },
      },
      {
        do<T>(_name: string, optionsOrFn: unknown, fn?: () => Promise<T> | T) {
          const operation = (fn ?? optionsOrFn) as () => Promise<T> | T;
          return Promise.resolve(operation());
        },
        sleep(_name, duration) {
          sleeps.push(duration);
          return Promise.resolve();
        },
      },
    );

    expect(sleeps).toEqual([250]);
  });

  test("normalizes duration strings and keeps absolute Date values and strings replay-stable", async () => {
    const target = new Date("2026-05-24T09:01:00.000Z");
    const targetString = "2026-05-24T09:02:00.000Z";
    const workflow = defineWorkflow("maintenance/sleep-forms", {
      async run(ctx) {
        await ctx.sleep("short string", "250ms");
        await ctx.sleep("seconds string", "5s");
        await ctx.sleep("fixed date", target);
        await ctx.sleep("fixed date string", targetString);
      },
    });
    class Base {
      env = {};
    }
    const Entrypoint = createCloudflareWorkflowEntrypoint(Base, {
      registry: defineWorkflowRegistry([workflow]),
    });
    const sleeps: unknown[] = [];
    const sleepUntil: unknown[] = [];

    await new Entrypoint().run(
      {
        payload: {
          id: "wf_sleep_forms",
          name: "maintenance/sleep-forms",
          payload: {},
          traceId: "trace",
          idempotencyKey: "idem",
          createdAt: "2026-05-24T09:00:00.000Z",
        },
      },
      {
        do<T>(_name: string, optionsOrFn: unknown, fn?: () => Promise<T> | T) {
          const operation = (fn ?? optionsOrFn) as () => Promise<T> | T;
          return Promise.resolve(operation());
        },
        sleep(_name, duration) {
          sleeps.push(duration);
          return Promise.resolve();
        },
        sleepUntil(_name, timestamp) {
          sleepUntil.push(timestamp);
          return Promise.resolve();
        },
      },
    );

    expect(sleeps).toEqual([250, 5_000]);
    expect(sleepUntil).toEqual([target.getTime(), Date.parse(targetString)]);
  });

  test("skips deterministic zero-duration sleeps", async () => {
    const workflow = defineWorkflow("maintenance/no-sleep", {
      async run(ctx) {
        await ctx.sleep("zero", 0);
        await ctx.sleep("zero string", "0ms");
      },
    });
    class Base {
      env = {};
    }
    const Entrypoint = createCloudflareWorkflowEntrypoint(Base, {
      registry: defineWorkflowRegistry([workflow]),
    });
    let sleeps = 0;

    await new Entrypoint().run(
      {
        payload: {
          id: "wf_no_sleep",
          name: "maintenance/no-sleep",
          payload: {},
          traceId: "trace",
          idempotencyKey: "idem",
          createdAt: new Date().toISOString(),
        },
      },
      {
        do<T>(_name: string, optionsOrFn: unknown, fn?: () => Promise<T> | T) {
          return Promise.resolve(((fn ?? optionsOrFn) as () => T)());
        },
        sleep() {
          sleeps += 1;
          return Promise.resolve();
        },
      },
    );

    expect(sleeps).toBe(0);
  });

  test("rejects non-finite sleep durations before a durable step", async () => {
    const workflow = defineWorkflow("maintenance/bad-sleep", {
      async run(ctx) {
        await ctx.sleep("invalid", Number.POSITIVE_INFINITY);
      },
    });
    class Base {
      env = {};
    }
    const Entrypoint = createCloudflareWorkflowEntrypoint(Base, {
      registry: defineWorkflowRegistry([workflow]),
    });

    await expect(
      new Entrypoint().run(
        {
          payload: {
            id: "wf_bad_sleep",
            name: "maintenance/bad-sleep",
            payload: {},
            traceId: "trace",
            idempotencyKey: "idem",
            createdAt: new Date().toISOString(),
          },
        },
        {
          do<T>(_name: string, optionsOrFn: unknown, fn?: () => Promise<T> | T) {
            return Promise.resolve(((fn ?? optionsOrFn) as () => T)());
          },
          sleep() {
            throw new Error("sleep must not be called");
          },
        },
      ),
    ).rejects.toThrow("must be finite");
  });

  test("creates deterministic child Workflows inside a durable step", async () => {
    const workflow = defineWorkflow("course/rebuild", {
      async run(ctx) {
        return ctx.dispatch(
          "course/rebuild-part",
          { courseId: "course_1" },
          { childKey: "part-1", delayMs: 5_000 },
        );
      },
    });
    class Base {
      env = {};
    }
    let insideStep = false;
    const childCalls: Array<{ options: DispatchOptions | undefined }> = [];
    const Entrypoint = createCloudflareWorkflowEntrypoint(Base, {
      registry: defineWorkflowRegistry([workflow]),
      async dispatch(name, _payload, options) {
        expect(insideStep).toBe(true);
        childCalls.push({ options });
        return {
          ids: [options?.id ?? "missing"],
          envelopes: [],
        };
      },
    });

    const event = {
      payload: {
        id: "wf_parent",
        name: "course/rebuild",
        payload: {},
        traceId: "trace_parent",
        idempotencyKey: "idem_parent",
        createdAt: "2026-05-24T09:00:00.000Z",
        scheduledAt: "2026-05-24T10:00:00.000Z",
      },
    };
    const stepNames: string[] = [];
    const step = {
      async do<T>(name: string, optionsOrFn: unknown, fn?: () => Promise<T> | T) {
        stepNames.push(name);
        insideStep = true;
        try {
          const operation = (fn ?? optionsOrFn) as () => Promise<T> | T;
          return await operation();
        } finally {
          insideStep = false;
        }
      },
      sleep() {
        return Promise.resolve();
      },
      sleepUntil() {
        return Promise.resolve();
      },
    };

    await new Entrypoint().run(event, step);
    await new Entrypoint().run(event, step);

    expect(childCalls).toHaveLength(2);
    expect(childCalls[0]?.options?.id).toBe(childCalls[1]?.options?.id);
    expect(childCalls[0]?.options).toMatchObject({
      traceId: "trace_parent",
      createdAt: "2026-05-24T09:00:00.000Z",
      delayMs: 5_000,
    });
    expect(childCalls[0]?.options?.scheduledAt).toBeUndefined();
    expect(childCalls[0]?.options?.idempotencyKey).toMatch(/^child:[a-f0-9]{64}$/);
    expect(stepNames[0]).toMatch(/^sdk dispatch course-rebuild-part /);
    expect(stepNames[0]).toBe(stepNames[1]);
  });

  test("rejects ambiguous duplicate unnamed child dispatches", async () => {
    const workflow = defineWorkflow("course/rebuild", {
      async run(ctx) {
        await ctx.dispatch("course/rebuild-part", { part: 1 });
        await ctx.dispatch("course/rebuild-part", { part: 2 });
      },
    });
    class Base {
      env = {};
    }
    let created = 0;
    const Entrypoint = createCloudflareWorkflowEntrypoint(Base, {
      registry: defineWorkflowRegistry([workflow]),
      async dispatch(_name, _payload, options) {
        created += 1;
        return { ids: [options?.id ?? "missing"], envelopes: [] };
      },
    });

    await expect(
      new Entrypoint().run(
        {
          payload: {
            id: "wf_parent",
            name: "course/rebuild",
            payload: {},
            traceId: "trace",
            idempotencyKey: "idem",
            createdAt: "2026-05-24T09:00:00.000Z",
          },
        },
        {
          do<T>(_name: string, optionsOrFn: unknown, fn?: () => Promise<T> | T) {
            const operation = (fn ?? optionsOrFn) as () => Promise<T> | T;
            return Promise.resolve(operation());
          },
          sleep() {
            return Promise.resolve();
          },
        },
      ),
    ).rejects.toThrow("provide a distinct options.childKey");
    expect(created).toBe(1);
  });
});
