import { describe, expect, test } from "bun:test";
import { createRunContext } from "../core/runtime";
import type { WorkflowRunContext } from "../core/types";
import {
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
    step(_name, fn) {
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

  test("rejects duplicate stepName entries before any step executes", async () => {
    const calls: string[] = [];
    const workflowName = "payment/process-payout";
    const workflow = registry.get(workflowName);
    const ctx = createFastTestContext(
      workflowName,
      {
        backend: {
          async execute(path) {
            calls.push(path);
          },
        },
      },
      {
        callbackSteps: [
          { stepName: "notify", backendPath: "payment/step-a" },
          { stepName: "notify", backendPath: "payment/step-b" },
        ],
      },
    );

    await expect(
      workflow.run(ctx, { tenantId: "tenant_1", transactionId: "tx_1" }),
    ).rejects.toThrow('duplicate stepName "notify"');
    expect(calls).toEqual([]);
  });
});
