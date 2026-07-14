import { describe, expect, test } from "bun:test";
import { createWorkflowClient, defineWorkflow, defineWorkflowRegistry } from "../index";
import { InMemoryWorkflowAdapter } from "../testing";

describe("workflow client", () => {
  test("dispatch creates a standard envelope", async () => {
    const adapter = new InMemoryWorkflowAdapter();
    const client = createWorkflowClient({
      adapter,
      idGenerator: () => "wf_test",
      now: () => new Date("2026-05-24T09:00:00.000Z"),
    });

    const result = await client.dispatch(
      "email/send",
      { tenantId: "tenant_1" },
      { delayMs: 5_000, traceId: "trace_1" },
    );

    expect(result.ids).toEqual(["wf_test"]);
    expect(result.envelopes[0]).toMatchObject({
      id: "wf_test",
      name: "email/send",
      payload: { tenantId: "tenant_1" },
      traceId: "trace_1",
      scheduledAt: "2026-05-24T09:00:05.000Z",
      createdAt: "2026-05-24T09:00:00.000Z",
    });
  });

  test("a throwing onDispatch hook does not fail a successful dispatch", async () => {
    const adapter = new InMemoryWorkflowAdapter();
    const errors: Array<{ message: string; context?: Record<string, unknown> }> =
      [];
    const client = createWorkflowClient({
      adapter,
      idGenerator: () => "wf_ok",
      logger: {
        error: (message, context) => errors.push({ message, context }),
      },
      hooks: {
        onDispatch: () => {
          throw new Error("hook exploded");
        },
      },
    });

    const result = await client.dispatch("email/send", { tenantId: "t1" });

    // The instance was durably created, so the caller must still get its id.
    expect(result.ids).toEqual(["wf_ok"]);
    expect(adapter.instances.has("wf_ok")).toBe(true);
    expect(errors).toHaveLength(1);
    expect(errors[0]?.message).toBe("workflow.dispatch.hook_failed");
  });

  test("onDispatchFailed still fires and rethrows when the adapter fails", async () => {
    const failed: string[] = [];
    const client = createWorkflowClient({
      adapter: {
        dispatch: async () => {
          throw new Error("adapter down");
        },
        getInstance: async () => null,
      },
      hooks: {
        onDispatchFailed: (envelope) => {
          failed.push(envelope.name);
        },
      },
    });

    await expect(client.dispatch("email/send", {})).rejects.toThrow(
      "adapter down",
    );
    expect(failed).toEqual(["email/send"]);
  });

  test("registry rejects duplicate names", () => {
    const workflow = defineWorkflow("same/name", {
      run: () => undefined,
    });

    expect(() => defineWorkflowRegistry([workflow, workflow])).toThrow(
      "Duplicate workflow name",
    );
  });
});
