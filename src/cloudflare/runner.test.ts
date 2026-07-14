import { describe, expect, test } from "bun:test";
import { defineWorkflow, defineWorkflowRegistry } from "../index";
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
    expect(calls[1]).toEqual({
      name: "send",
      value: {
        retries: { limit: 3, delay: 1_000, backoff: "exponential" },
        timeout: 5_000,
      },
    });
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

    expect(calls[0]).toEqual({
      name: "send",
      value: {
        retries: {
          limit: 2,
          delay: 500,
          backoff: "constant",
        },
      },
    });
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

  test("derives stable legacy envelope fields from the Cloudflare event", async () => {
    const workflow = defineWorkflow("email/send", {
      async run(ctx) {
        return {
          id: ctx.event.id,
          traceId: ctx.traceId,
          idempotencyKey: ctx.idempotencyKey,
          createdAt: ctx.event.createdAt,
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
          eventName: "email/send",
          data: { tenantId: "tenant_1" },
        },
        instanceId: "cf_instance_1",
        timestamp: new Date("2026-05-24T09:00:00.000Z"),
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

    // No random generation on replay: id comes from the CF instance id, createdAt
    // from the event timestamp, idempotencyKey defaults to the id, traceId is
    // derived deterministically from the id.
    expect(result).toEqual({
      id: "cf_instance_1",
      traceId: "trace_cf_instance_1",
      idempotencyKey: "cf_instance_1",
      createdAt: "2026-05-24T09:00:00.000Z",
    });
  });

  test("wraps ctx.dispatch in a uniquely named step.do so it is memoized on replay", async () => {
    const workflow = defineWorkflow("parent", {
      async run(ctx) {
        await ctx.dispatch("child", { a: 1 });
        await ctx.dispatch("child", { a: 2 });
        return "done";
      },
    });
    class Base {
      env = {};
    }
    const dispatched: Array<{ name: string; payload: unknown }> = [];
    const Entrypoint = createCloudflareWorkflowEntrypoint(Base, {
      registry: defineWorkflowRegistry([workflow]),
      dispatch: async (name, payload) => {
        dispatched.push({ name, payload });
        return { ids: [`${name}_id`], envelopes: [] };
      },
    });
    const stepNames: string[] = [];

    const result = await new Entrypoint().run(
      {
        payload: {
          id: "wf_1",
          name: "parent",
          payload: {},
          traceId: "trace",
          idempotencyKey: "idem",
          createdAt: new Date().toISOString(),
        },
      },
      {
        do<T>(name: string, optionsOrFn: unknown, fn?: () => Promise<T> | T): Promise<T> {
          stepNames.push(name);
          const operation = (fn ?? optionsOrFn) as () => Promise<T> | T;
          return Promise.resolve(operation());
        },
        sleep() {
          return Promise.resolve();
        },
      },
    );

    expect(result).toBe("done");
    expect(stepNames).toEqual(["dispatch:child:0", "dispatch:child:1"]);
    expect(dispatched).toEqual([
      { name: "child", payload: { a: 1 } },
      { name: "child", payload: { a: 2 } },
    ]);
  });

  test("passes numeric ms durations straight to step.sleep instead of a string", async () => {
    const workflow = defineWorkflow("email/send", {
      async run(ctx) {
        await ctx.sleep("sub-second", 500);
        return "ok";
      },
    });
    class Base {
      env = {};
    }
    const Entrypoint = createCloudflareWorkflowEntrypoint(Base, {
      registry: defineWorkflowRegistry([workflow]),
    });
    const sleeps: Array<{ name: string; duration: unknown }> = [];

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
        do<T>(_name: string, optionsOrFn: unknown, fn?: () => Promise<T> | T): Promise<T> {
          const operation = (fn ?? optionsOrFn) as () => Promise<T> | T;
          return Promise.resolve(operation());
        },
        sleep(name, duration) {
          sleeps.push({ name, duration });
          return Promise.resolve();
        },
      },
    );

    expect(sleeps).toEqual([{ name: "sub-second", duration: 500 }]);
  });

  test("caps Cloudflare retry backoff with a delay function when maxIntervalMs would bind", async () => {
    const workflow = defineWorkflow("email/send", {
      async run(ctx) {
        return ctx.step(
          "send",
          () => "ok",
          {
            retry: {
              maxAttempts: 5,
              initialIntervalMs: 1_000,
              multiplier: 10,
              maxIntervalMs: 5_000,
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
    let captured: unknown;

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
        do<T>(_name: string, optionsOrFn: unknown, fn?: () => Promise<T> | T): Promise<T> {
          if (typeof optionsOrFn !== "function") captured = optionsOrFn;
          const operation = (fn ?? optionsOrFn) as () => Promise<T> | T;
          return Promise.resolve(operation());
        },
        sleep() {
          return Promise.resolve();
        },
      },
    );

    const retries = (captured as { retries?: Record<string, unknown> }).retries!;
    expect(retries.limit).toBe(6);
    // No static backoff field: Cloudflare ignores it when delay is a function.
    expect(retries.backoff).toBeUndefined();
    const delay = retries.delay as (info: { ctx: { attempt: number } }) => number;
    expect(delay({ ctx: { attempt: 1 } })).toBe(1_000);
    expect(delay({ ctx: { attempt: 2 } })).toBe(5_000);
    expect(delay({ ctx: { attempt: 4 } })).toBe(5_000);
  });

  test("propagates a non-retryable step error when the Cloudflare runtime is unavailable", async () => {
    const failure = Object.assign(new Error("permanent"), { nonRetryable: true });
    const workflow = defineWorkflow("email/send", {
      async run(ctx) {
        return ctx.step("send", () => {
          throw failure;
        });
      },
    });
    class Base {
      env = {};
    }
    const Entrypoint = createCloudflareWorkflowEntrypoint(Base, {
      registry: defineWorkflowRegistry([workflow]),
    });

    // Outside Workers, cloudflare:workflows cannot be imported, so translation is
    // a best-effort no-op and the original error must still surface.
    await expect(
      new Entrypoint().run(
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
      ),
    ).rejects.toThrow("permanent");
  });
});
