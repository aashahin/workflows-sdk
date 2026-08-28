import { describe, expect, test } from "bun:test";
import { createRunContext } from "../core/runtime";
import type { WorkflowRunContext, WorkflowStepOptions } from "../core/types";
import {
  createBackendCallbackStepName,
  createBackendCallbackWorkflowRegistry,
  type BackendCallbackExecuteContext,
  type BackendCallbackFailedEvent,
  type BackendCallbackWorkflowServices,
} from "./backend-callback";

const registry = createBackendCallbackWorkflowRegistry();

function createTestContext(
  workflowName: string,
  services: BackendCallbackWorkflowServices,
  metadata?: Record<string, unknown>,
) {
  const workflow = registry.get(workflowName);
  return {
    workflow,
    ctx: createRunContext(
      {
        id: "wf_1",
        name: workflowName,
        payload: { tenantId: "tenant_1" },
        traceId: "trace_1",
        idempotencyKey: "idem_1",
        createdAt: "2026-05-24T09:00:00.000Z",
        metadata,
      },
      workflow,
      {
        registry,
        services,
        logger: {},
      },
    ),
  };
}

function createFastTestContext(
  workflowName: string,
  services: BackendCallbackWorkflowServices,
  metadata?: Record<string, unknown>,
  capturedStepOptions?: Array<WorkflowStepOptions | undefined>,
): WorkflowRunContext<BackendCallbackWorkflowServices> {
  return {
    event: {
      id: "wf_1",
      name: workflowName,
      payload: { tenantId: "tenant_1" },
      traceId: "trace_1",
      idempotencyKey: "idem_1",
      createdAt: "2026-05-24T09:00:00.000Z",
      metadata,
    },
    traceId: "trace_1",
    idempotencyKey: "idem_1",
    logger: {},
    services,
    step(_name, fn, options) {
      capturedStepOptions?.push(options);
      return Promise.resolve(fn());
    },
    sleep() {
      return Promise.resolve();
    },
    dispatch() {
      throw new Error("dispatch is not used in these tests");
    },
  };
}

describe("createBackendCallbackWorkflowRegistry", () => {
  test("keeps deterministic callback step names within Cloudflare's limit", () => {
    const first = `${"a".repeat(128)}/${"b".repeat(127)}`;
    const second = `${"a".repeat(128)}/${"c".repeat(127)}`;
    const firstName = createBackendCallbackStepName(first);
    const secondName = createBackendCallbackStepName(second);

    expect(firstName).toHaveLength(256);
    expect(secondName).toHaveLength(256);
    expect(firstName).not.toBe(secondName);
    expect(createBackendCallbackStepName("email/send")).toBe(
      "callback-email-send",
    );
  });
  test("can reject queue-owned single-step workflow namespaces", () => {
    const queueOwnedRegistry = createBackendCallbackWorkflowRegistry({
      workflowNamePolicy(name) {
        return name.startsWith("email/")
          ? "Email callbacks must use the notification queue"
          : true;
      },
    });

    expect(queueOwnedRegistry.has("email/invitation")).toBe(false);
    expect(() => queueOwnedRegistry.get("email/invitation")).toThrow(
      "Email callbacks must use the notification queue",
    );
    expect(queueOwnedRegistry.has("course/rebuild-index")).toBe(true);
  });

  test("applies a workflow-specific callback step policy before execution", async () => {
    const policyRegistry = createBackendCallbackWorkflowRegistry({
      callbackStepsPolicy(workflowName, steps) {
        return workflowName === "payment/process-payout" &&
          steps.map((step) => step.backendPath).join(",") ===
            "payment/validate-payout,payment/process-payout,payment/notify-payout-status"
          ? true
          : `Unexpected callback plan for ${workflowName}`;
      },
    });
    const workflow = policyRegistry.get("payment/process-payout");
    const ctx = createFastTestContext(
      "payment/process-payout",
      { backend: { async execute() {} } },
      {
        callbackSteps: [
          { stepName: "first", backendPath: "email/reset-password" },
        ],
      },
    );

    await expect(workflow.run(ctx, { tenantId: "tenant_1" })).rejects.toThrow(
      "Unexpected callback plan",
    );
  });

  test("rejects callback paths that can escape the execute namespace", () => {
    for (const path of [
      "../admin/delete",
      "payment/../admin",
      "payment/run?admin=true",
      "payment/%2e%2e/admin",
      "/payment/run",
    ]) {
      expect(registry.has(path)).toBe(false);
      expect(() => registry.get(path)).toThrow(/slash-separated/);
    }
  });

  test("uses caller-provided default step names", async () => {
    const calls: string[] = [];
    const customRegistry = createBackendCallbackWorkflowRegistry({
      defaultStepName: (workflowName) =>
        `send-${workflowName.replace(/[^a-zA-Z0-9]+/g, "-")}`,
    });
    const workflowName = "email/invitation";
    const workflow = customRegistry.get(workflowName);
    const ctx = createRunContext(
      {
        id: "wf_1",
        name: workflowName,
        payload: { tenantId: "tenant_1" },
        traceId: "trace_1",
        idempotencyKey: "idem_1",
        createdAt: "2026-05-24T09:00:00.000Z",
      },
      workflow,
      {
        registry: customRegistry,
        services: {
          backend: {
            async execute(
              _path: string,
              _payload: Record<string, unknown>,
              context: BackendCallbackExecuteContext,
            ) {
              calls.push(context.stepName);
            },
          },
        },
        logger: {},
      },
    );

    await workflow.run(ctx, { tenantId: "tenant_1" });

    expect(calls).toEqual(["send-email-invitation"]);
  });

  test("default workflow calls backend path equal to workflow name", async () => {
    const calls: string[] = [];
    const workflowName = "email/invitation";
    const { workflow, ctx } = createTestContext(workflowName, {
      backend: {
        async execute(path) {
          calls.push(path);
        },
      },
    });

    await workflow.run(ctx, {
      tenantId: "tenant_1",
      email: "student@example.com",
    });

    expect(calls).toEqual([workflowName]);
  });

  test("disables Workflow step retries when Queue recovery owns retries", async () => {
    const captured: Array<WorkflowStepOptions | undefined> = [];
    const workflowName = "course/rebuild-index";
    const workflow = registry.get(workflowName);
    const ctx = createFastTestContext(
      workflowName,
      { backend: { execute: async () => undefined } },
      undefined,
      captured,
    );

    await workflow.run(ctx, { tenantId: "tenant_1" });

    expect(captured[0]).toEqual({ retry: false });
  });

  test("callback step plan preserves distinct backend paths and event ids", async () => {
    const calls: Array<{ path: string; eventId: string }> = [];
    const { workflow, ctx } = createTestContext(
      "payment/process-payout",
      {
        backend: {
          async execute(path, _payload, context) {
            calls.push({ path, eventId: context.eventId });
          },
        },
      },
      {
        callbackSteps: [
          {
            stepName: "validate-payout",
            backendPath: "payment/validate-payout",
            backendEventIdSuffix: "validate-payout",
          },
          {
            stepName: "process-payout",
            backendPath: "payment/process-payout",
            backendEventIdSuffix: "process-payout",
          },
          {
            stepName: "notify-payout-status",
            backendPath: "payment/notify-payout-status",
            backendEventIdSuffix: "notify-payout-status",
          },
        ],
      },
    );

    await workflow.run(ctx, { tenantId: "tenant_1", transactionId: "tx_1" });

    expect(calls).toEqual([
      {
        path: "payment/validate-payout",
        eventId: "wf_1:validate-payout",
      },
      {
        path: "payment/process-payout",
        eventId: "wf_1:process-payout",
      },
      {
        path: "payment/notify-payout-status",
        eventId: "wf_1:notify-payout-status",
      },
    ]);
  });

  test("retryable backend failures are recorded generically", async () => {
    const failed: BackendCallbackFailedEvent[] = [];
    const workflowName = "email/verification";
    const workflow = registry.get(workflowName);
    const ctx = createFastTestContext(workflowName, {
      backend: {
        async execute() {
          throw new Error("backend down");
        },
      },
      failedEvents: {
        async record(event) {
          failed.push(event);
        },
      },
    });

    await expect(
      workflow.run(ctx, {
        tenantId: "tenant_1",
        email: "student@example.com",
        otpCode: "123456",
      }),
    ).resolves.toMatchObject({
      status: "queued_for_retry",
      eventId: "wf_1",
      eventName: workflowName,
      backendPath: workflowName,
      backendEventId: "wf_1",
      error: "backend down",
    });
    expect(failed).toMatchObject([
      {
        eventId: "wf_1",
        workflowName,
        backendPath: workflowName,
        backendEventId: "wf_1",
      },
    ]);
  });

  test("bounds and sanitizes the queued recovery error", async () => {
    const failed: BackendCallbackFailedEvent[] = [];
    const workflowName = "course/rebuild-index";
    const workflow = registry.get(workflowName);
    const ctx = createFastTestContext(workflowName, {
      backend: {
        async execute() {
          throw new Error(`backend\u0000 failed\n${"x".repeat(4_000)}`);
        },
      },
      failedEvents: {
        async record(event) {
          failed.push(event);
        },
      },
    });

    const result = await workflow.run(ctx, { tenantId: "tenant_1" });

    expect(failed[0]?.error.length).toBe(2_048);
    expect(failed[0]?.error).not.toMatch(/[\u0000-\u001f\u007f]/);
    expect(result).toMatchObject({
      status: "queued_for_retry",
      eventId: "wf_1",
      error: failed[0]?.error,
    });
    expect(result).not.toHaveProperty("recovery");
  });

  test("does not enqueue permanent callback failures", async () => {
    let recorded = 0;
    const workflowName = "course/rebuild-index";
    const workflow = registry.get(workflowName);
    const permanent = new Error("validation failed");
    permanent.name = "NonRetryableError";
    const ctx = createFastTestContext(workflowName, {
      backend: {
        async execute() {
          throw permanent;
        },
      },
      failedEvents: {
        async record() {
          recorded += 1;
        },
      },
    });

    await expect(workflow.run(ctx, { tenantId: "tenant_1" })).rejects.toThrow(
      "validation failed",
    );
    expect(recorded).toBe(0);
  });

  test("does not trust non-retryable text embedded in a retryable error", async () => {
    let recorded = 0;
    const workflowName = "course/rebuild-index";
    const workflow = registry.get(workflowName);
    const ctx = createFastTestContext(workflowName, {
      backend: {
        async execute() {
          throw new Error(
            "Backend failed (500): upstream mentioned NonRetryableError",
          );
        },
      },
      failedEvents: {
        async record() {
          recorded += 1;
        },
      },
    });

    await expect(
      workflow.run(ctx, { tenantId: "tenant_1" }),
    ).resolves.toMatchObject({ status: "queued_for_retry" });
    expect(recorded).toBe(1);
  });

  test("failed callback step plans include remaining backend steps", async () => {
    const failed: BackendCallbackFailedEvent[] = [];
    const workflowName = "payment/process-payout";
    const workflow = registry.get(workflowName);
    const ctx = createFastTestContext(
      workflowName,
      {
        backend: {
          async execute(path) {
            if (path === "payment/process-payout") {
              throw new Error("payment provider down");
            }
          },
        },
        failedEvents: {
          async record(event) {
            failed.push(event);
          },
        },
      },
      {
        callbackSteps: [
          {
            stepName: "validate-payout",
            backendPath: "payment/validate-payout",
            backendEventIdSuffix: "validate-payout",
          },
          {
            stepName: "process-payout",
            backendPath: "payment/process-payout",
            backendEventIdSuffix: "process-payout",
          },
          {
            stepName: "notify-payout-status",
            backendPath: "payment/notify-payout-status",
            backendEventIdSuffix: "notify-payout-status",
          },
        ],
      },
    );

    await expect(
      workflow.run(ctx, { tenantId: "tenant_1", transactionId: "tx_1" }),
    ).resolves.toMatchObject({
      status: "queued_for_retry",
      eventId: "wf_1",
      eventName: workflowName,
      backendPath: "payment/process-payout",
      backendEventId: "wf_1:process-payout",
      error: "payment provider down",
    });

    expect(failed).toMatchObject([
      {
        backendPath: "payment/process-payout",
        backendEventId: "wf_1:process-payout",
        backendSteps: [
          {
            backendPath: "payment/process-payout",
            backendEventId: "wf_1:process-payout",
          },
          {
            backendPath: "payment/notify-payout-status",
            backendEventId: "wf_1:notify-payout-status",
          },
        ],
      },
    ]);
  });

  test("keeps the original backend error when failed-event recording fails", async () => {
    const workflowName = "email/verification";
    const workflow = registry.get(workflowName);
    const ctx = createFastTestContext(workflowName, {
      backend: {
        async execute() {
          throw new Error("backend down");
        },
      },
      failedEvents: {
        async record() {
          throw new Error("queue unavailable");
        },
      },
    });

    await expect(
      workflow.run(ctx, {
        tenantId: "tenant_1",
        email: "student@example.com",
        otpCode: "123456",
      }),
    ).rejects.toThrow("backend down");
  });

  test("rejects malformed callback step metadata", async () => {
    const workflowName = "payment/process-payout";
    const workflow = registry.get(workflowName);
    const ctx = createFastTestContext(
      workflowName,
      {
        backend: {
          async execute() {
            throw new Error("should not execute");
          },
        },
      },
      {
        callbackSteps: [{ stepName: "missing-path" }],
      },
    );

    await expect(
      workflow.run(ctx, { tenantId: "tenant_1", transactionId: "tx_1" }),
    ).rejects.toThrow("requires stepName and backendPath");
  });

  test("rejects callback plans above the three-step application maximum", async () => {
    const workflowName = "payment/process-payout";
    const workflow = registry.get(workflowName);
    const ctx = createFastTestContext(
      workflowName,
      { backend: { async execute() {} } },
      {
        callbackSteps: Array.from({ length: 4 }, (_, index) => ({
          stepName: `step-${index}`,
          backendPath: `payment/step-${index}`,
        })),
      },
    );

    await expect(workflow.run(ctx, { tenantId: "tenant_1" })).rejects.toThrow(
      "at most 3",
    );
  });

  test("rejects traversal in callback step metadata before backend execution", async () => {
    const workflowName = "payment/process-payout";
    const workflow = registry.get(workflowName);
    let executions = 0;
    const ctx = createFastTestContext(
      workflowName,
      {
        backend: {
          async execute() {
            executions += 1;
          },
        },
      },
      {
        callbackSteps: [
          { stepName: "escape", backendPath: "payment/../../admin" },
        ],
      },
    );

    await expect(
      workflow.run(ctx, { tenantId: "tenant_1", transactionId: "tx_1" }),
    ).rejects.toThrow(/slash-separated/);
    expect(executions).toBe(0);
  });
});
