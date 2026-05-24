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
});
