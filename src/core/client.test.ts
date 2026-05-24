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

  test("registry rejects duplicate names", () => {
    const workflow = defineWorkflow("same/name", {
      run: () => undefined,
    });

    expect(() => defineWorkflowRegistry([workflow, workflow])).toThrow(
      "Duplicate workflow name",
    );
  });
});
