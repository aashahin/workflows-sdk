import { describe, expect, test } from "bun:test";
import {
  createBackendCallbackWorkflowRegistry,
  defineWorkflow,
  defineWorkflowRegistry,
  type BackendCallbackStep,
} from "../index";
import {
  createCloudflareDispatchHandler,
  createCloudflareWorkflowDispatch,
  type CloudflareWorkflowReceiptClaimInput,
  type CloudflareWorkflowReceiptCleanupCandidate,
  type CloudflareWorkflowReceiptRecord,
  type CloudflareWorkflowReceiptStore,
} from "./dispatch-handler";

class MemoryWorkflowReceiptStore implements CloudflareWorkflowReceiptStore {
  readonly records = new Map<string, CloudflareWorkflowReceiptRecord>();
  readonly cleanupListLimits: number[] = [];

  async claim(input: CloudflareWorkflowReceiptClaimInput) {
    const key = this.key(input.workflowIdentity, input.instanceId);
    const current = this.records.get(key);
    if (!current) {
      this.records.set(key, {
        workflowName: input.workflowName,
        envelopeHash: input.envelopeHash,
        state: "PENDING",
        owner: input.owner,
        fence: 1,
        leaseExpiresAt: input.leaseExpiresAt,
        checkAfter: input.checkAfter,
      });
      return {
        outcome: "claimed" as const,
        fence: 1,
        previousState: "new" as const,
      };
    }
    if (
      current.envelopeHash !== input.envelopeHash ||
      current.workflowName !== input.workflowName
    ) {
      return { outcome: "conflict" as const };
    }
    if (current.state === "CREATED") {
      return { outcome: "created" as const };
    }
    if ((current.leaseExpiresAt ?? 0) > input.now) {
      return {
        outcome: "busy" as const,
        retryAfter: current.leaseExpiresAt!,
      };
    }

    const fence = current.fence + 1;
    this.records.set(key, {
      ...current,
      owner: input.owner,
      fence,
      leaseExpiresAt: input.leaseExpiresAt,
    });
    return {
      outcome: "claimed" as const,
      fence,
      previousState:
        current.state === "ABSENCE_PROVEN"
          ? ("absence_proven" as const)
          : ("pending" as const),
    };
  }

  async get(input: { workflowIdentity: string; instanceId: string }) {
    return this.records.get(this.key(input.workflowIdentity, input.instanceId)) ?? null;
  }

  async markAbsenceProven(input: {
    workflowIdentity: string;
    instanceId: string;
    envelopeHash: string;
    owner: string;
    fence: number;
    now: number;
  }) {
    const current = this.records.get(
      this.key(input.workflowIdentity, input.instanceId),
    );
    if (
      !current ||
      current.state !== "PENDING" ||
      current.envelopeHash !== input.envelopeHash ||
      current.owner !== input.owner ||
      current.fence !== input.fence
    ) {
      return false;
    }
    current.state = "ABSENCE_PROVEN";
    return true;
  }

  async markCreated(input: {
    workflowIdentity: string;
    instanceId: string;
    envelopeHash: string;
    owner: string;
    fence: number;
    now: number;
  }) {
    const key = this.key(input.workflowIdentity, input.instanceId);
    const current = this.records.get(key);
    if (
      !current ||
      current.state !== "ABSENCE_PROVEN" ||
      current.envelopeHash !== input.envelopeHash ||
      current.owner !== input.owner ||
      current.fence !== input.fence
    ) {
      return false;
    }
    this.records.set(key, {
      ...current,
      state: "CREATED",
      owner: null,
      leaseExpiresAt: null,
    });
    return true;
  }

  async release(input: {
    workflowIdentity: string;
    instanceId: string;
    envelopeHash: string;
    owner: string;
    fence: number;
    now: number;
  }) {
    const key = this.key(input.workflowIdentity, input.instanceId);
    const current = this.records.get(key);
    if (
      !current ||
      current.state === "CREATED" ||
      current.envelopeHash !== input.envelopeHash ||
      current.owner !== input.owner ||
      current.fence !== input.fence
    ) {
      return false;
    }
    this.records.set(key, { ...current, leaseExpiresAt: input.now });
    return true;
  }

  async listCleanupCandidates(input: { checkBefore: number; limit: number }) {
    this.cleanupListLimits.push(input.limit);
    return [...this.records.entries()]
      .filter(
        ([, record]) =>
          record.checkAfter <= input.checkBefore &&
          (record.state === "CREATED" ||
            (record.leaseExpiresAt ?? 0) <= input.checkBefore),
      )
      .sort((left, right) => left[1].checkAfter - right[1].checkAfter)
      .slice(0, input.limit)
      .map(([key, record]) => {
        const [workflowIdentity, instanceId] = key.split("\u0000");
        return {
          workflowIdentity: workflowIdentity!,
          instanceId: instanceId!,
          ...record,
        } satisfies CloudflareWorkflowReceiptCleanupCandidate;
      });
  }

  async deleteCleanupCandidate(
    candidate: CloudflareWorkflowReceiptCleanupCandidate,
  ) {
    const key = this.key(candidate.workflowIdentity, candidate.instanceId);
    const current = this.records.get(key);
    if (!current || !this.matchesCandidate(current, candidate)) return false;
    this.records.delete(key);
    return true;
  }

  async deferCleanupCandidate(input: {
    candidate: CloudflareWorkflowReceiptCleanupCandidate;
    now: number;
    checkAfter: number;
  }) {
    const key = this.key(
      input.candidate.workflowIdentity,
      input.candidate.instanceId,
    );
    const current = this.records.get(key);
    if (!current || !this.matchesCandidate(current, input.candidate)) {
      return false;
    }
    current.checkAfter = input.checkAfter;
    return true;
  }

  private key(workflowIdentity: string, instanceId: string) {
    return `${workflowIdentity}\u0000${instanceId}`;
  }

  private matchesCandidate(
    current: CloudflareWorkflowReceiptRecord,
    candidate: CloudflareWorkflowReceiptCleanupCandidate,
  ) {
    return (
      current.workflowName === candidate.workflowName &&
      current.envelopeHash === candidate.envelopeHash &&
      current.state === candidate.state &&
      current.owner === candidate.owner &&
      current.fence === candidate.fence &&
      current.leaseExpiresAt === candidate.leaseExpiresAt &&
      current.checkAfter === candidate.checkAfter
    );
  }
}

const PAYOUT_CALLBACK_STEPS: BackendCallbackStep[] = [
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
];

function strictCallbackRegistry() {
  return createBackendCallbackWorkflowRegistry({
    workflowNamePolicy(name) {
      return name === "email/verification" || name === "payment/process-payout"
        ? true
        : `Unsupported Workflow callback name: ${name}`;
    },
    callbackStepsPolicy(name, steps) {
      if (name !== "payment/process-payout") {
        return steps.length === 0
          ? true
          : `Workflow ${name} does not accept explicit callback steps`;
      }
      return steps.length === PAYOUT_CALLBACK_STEPS.length &&
        steps.every((step, index) => {
          const expected = PAYOUT_CALLBACK_STEPS[index]!;
          return (
            step.stepName === expected.stepName &&
            step.backendPath === expected.backendPath &&
            step.backendEventId === undefined &&
            step.backendEventIdSuffix === expected.backendEventIdSuffix
          );
        })
        ? true
        : "Workflow payment/process-payout requires its canonical three-step callback plan";
    },
  });
}

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
    const created: Array<{
      id?: string;
      params?: unknown;
      retention: { successRetention: string | number; errorRetention: string | number };
    }> = [];
    const dispatch = createCloudflareWorkflowDispatch({
      registry: defineWorkflowRegistry([workflow]),
      now: () => new Date("2026-05-24T09:00:00.000Z"),
      idGenerator: () => "wf_nested",
      traceIdGenerator: () => "trace_nested",
      resolveWorkflow() {
        return {
          create: async (options) => {
            created.push(options);
            return options;
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
    expect(created[0]?.retention).toEqual({
      successRetention: "1 day",
      errorRetention: "3 days",
    });
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

  test("rejects non-JSON child payloads before crossing the binding boundary", async () => {
    const workflow = defineWorkflow("course/rebuild", { run: () => undefined });
    let created = 0;
    const dispatch = createCloudflareWorkflowDispatch({
      registry: defineWorkflowRegistry([workflow]),
      resolveWorkflow() {
        return {
          create: async () => {
            created += 1;
            return {};
          },
        };
      },
    });

    await expect(
      dispatch(
        "course/rebuild",
        { courseId: "course_1", invalid: () => undefined },
        undefined,
        {},
      ),
    ).rejects.toThrow("contains non-JSON value function");
    expect(created).toBe(0);
  });

  test("applies callback plan admission before nested binding create", async () => {
    let created = 0;
    const dispatch = createCloudflareWorkflowDispatch({
      registry: strictCallbackRegistry(),
      resolveWorkflow() {
        return {
          create: async () => {
            created += 1;
            return {};
          },
        };
      },
    });

    await expect(
      dispatch(
        "payment/process-payout",
        { tenantId: "tenant_1" },
        {
          metadata: {
            callbackSteps: PAYOUT_CALLBACK_STEPS.slice(1),
          },
        },
        {},
      ),
    ).rejects.toThrow("canonical three-step callback plan");
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

  test("rejects malformed callback plans before createBatch", async () => {
    let created = 0;
    const handler = createCloudflareDispatchHandler({
      registry: strictCallbackRegistry(),
      resolveWorkflow() {
        return {
          create: async () => {
            created += 1;
            return {};
          },
          createBatch: async (batch) => {
            created += batch.length;
            return batch;
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
              id: "payout_1",
              name: "payment/process-payout",
              payload: { tenantId: "tenant_1" },
              traceId: "trace_1",
              idempotencyKey: "idem_1",
              createdAt: "2026-08-27T00:00:00.000Z",
              metadata: { callbackSteps: PAYOUT_CALLBACK_STEPS.slice(1) },
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
          id: "payout_1",
          error: expect.stringContaining("canonical three-step callback plan"),
        },
      ],
    });
  });

  test("admits ordinary and canonical payout callbacks before createBatch", async () => {
    const created: unknown[] = [];
    const handler = createCloudflareDispatchHandler({
      registry: strictCallbackRegistry(),
      resolveWorkflow() {
        return {
          create: async () => ({}),
          createBatch: async (batch) => {
            created.push(...batch);
            return batch;
          },
        };
      },
    });
    const base = {
      payload: { tenantId: "tenant_1" },
      createdAt: "2026-08-27T00:00:00.000Z",
    };
    const response = await handler.fetch(
      new Request("https://example.com/dispatch", {
        method: "POST",
        body: JSON.stringify({
          events: [
            {
              ...base,
              id: "email_1",
              name: "email/verification",
              traceId: "trace_1",
              idempotencyKey: "idem_1",
            },
            {
              ...base,
              id: "payout_1",
              name: "payment/process-payout",
              traceId: "trace_2",
              idempotencyKey: "idem_2",
              metadata: { callbackSteps: PAYOUT_CALLBACK_STEPS },
            },
          ],
        }),
      }),
      {},
    );

    expect(response.status).toBe(200);
    expect(created).toHaveLength(2);
    await expect(response.json()).resolves.toMatchObject({
      ids: ["email_1", "payout_1"],
    });
  });

  test("requires an explicit workflow name for status lookup", async () => {
    const first = defineWorkflow("first", { run: () => undefined });
    const second = defineWorkflow("second", { run: () => undefined });
    let resolved = 0;
    const handler = createCloudflareDispatchHandler({
      registry: defineWorkflowRegistry([first, second]),
      resolveWorkflow() {
        resolved += 1;
        return {
          create: async () => ({}),
          get: async () => ({ status: async () => ({ status: "paused" }) }),
        };
      },
    });

    const response = await handler.fetch(
      new Request("https://example.com/status/wf_1"),
      {},
    );

    expect(response.status).toBe(400);
    expect(resolved).toBe(0);
    await expect(response.json()).resolves.toMatchObject({
      error: expect.stringContaining("requires the workflow name"),
    });
  });

  test("uses the explicitly named status binding without scanning others", async () => {
    const first = defineWorkflow("first", { run: () => undefined });
    const second = defineWorkflow("second", { run: () => undefined });
    const resolved: string[] = [];
    const handler = createCloudflareDispatchHandler({
      registry: defineWorkflowRegistry([first, second]),
      resolveWorkflow(name) {
        resolved.push(name);
        return {
          create: async () => ({}),
          get: async () => ({
            status: async () => ({ status: "complete" }),
          }),
        };
      },
    });

    const response = await handler.fetch(
      new Request("https://example.com/status/wf_1?name=second"),
      {},
    );

    expect(response.status).toBe(200);
    expect(resolved).toEqual(["second"]);
    await expect(response.json()).resolves.toMatchObject({
      id: "wf_1",
      name: "second",
      status: "complete",
    });
  });

  test("scheduled cron uses deterministic create with explicit retention", async () => {
    const workflow = defineWorkflow("daily", {
      cron: [{ name: "nine", schedule: "0 9 * * *" }],
      run: () => undefined,
    });
    const created: Array<{ retention?: unknown }> = [];
    const handler = createCloudflareDispatchHandler({
      registry: defineWorkflowRegistry([workflow]),
      now: () => new Date("2026-05-24T09:05:00.000Z"),
      resolveWorkflow() {
        return {
          create: async (options) => {
            created.push(options);
            return options;
          },
        };
      },
    });

    await expect(handler.scheduled({}, {})).resolves.toEqual({ dispatched: 1 });
    expect(created).toHaveLength(1);
    expect(created[0]?.retention).toEqual({
      successRetention: "1 day",
      errorRetention: "3 days",
    });
  });

  test("scheduled maintenance continues full cleanup batches and stops after a short batch", async () => {
    const receipts = new MemoryWorkflowReceiptStore();
    for (let index = 0; index < 7; index += 1) {
      receipts.records.set(`binding\u0000expired_${index}`, {
        workflowName: "daily",
        envelopeHash: String(index).repeat(64),
        state: "CREATED",
        owner: null,
        fence: 1,
        leaseExpiresAt: null,
        checkAfter: 900 + index,
      });
    }
    receipts.records.set("binding\u0000future", {
      workflowName: "daily",
      envelopeHash: "b".repeat(64),
      state: "CREATED",
      owner: null,
      fence: 1,
      leaseExpiresAt: null,
      checkAfter: 2_000,
    });
    const handler = createCloudflareDispatchHandler({
      registry: defineWorkflowRegistry([]),
      resolveWorkflow: () => ({
        create: async () => ({}),
        get: async () => {
          throw new Error("instance.not_found");
        },
      }),
      resolveWorkflowIdentity: () => "binding",
      resolveReceiptStore: () => receipts,
      receiptCleanupBatchSize: 3,
      receiptCleanupMaxBatches: 3,
    });

    await expect(
      handler.scheduled({ scheduledTime: 1_000 }, {}),
    ).resolves.toEqual({ dispatched: 0 });
    expect(receipts.cleanupListLimits).toEqual([3, 3, 3]);
    expect(
      [...receipts.records.keys()].filter((key) => key.includes("expired")),
    ).toEqual([]);
    expect(receipts.records.has("binding\u0000future")).toBe(true);
  });

  test("scheduled maintenance remains capped at twelve candidates", async () => {
    const receipts = new MemoryWorkflowReceiptStore();
    for (let index = 0; index < 13; index += 1) {
      receipts.records.set(`binding\u0000expired_${index}`, {
        workflowName: "daily",
        envelopeHash: String(index).padStart(64, "0"),
        state: "CREATED",
        owner: null,
        fence: 1,
        leaseExpiresAt: null,
        checkAfter: 900 + index,
      });
    }
    const handler = createCloudflareDispatchHandler({
      registry: defineWorkflowRegistry([]),
      resolveWorkflow: () => ({
        create: async () => ({}),
        get: async () => {
          throw new Error("instance.not_found");
        },
      }),
      resolveWorkflowIdentity: () => "binding",
      resolveReceiptStore: () => receipts,
    });

    await expect(
      handler.scheduled({ scheduledTime: 1_000 }, {}),
    ).resolves.toEqual({ dispatched: 0 });
    expect(receipts.cleanupListLimits).toEqual([6, 6]);
    expect(
      [...receipts.records.keys()].filter((key) => key.includes("expired")),
    ).toHaveLength(1);
  });

  test("scheduled maintenance deletes only explicitly absent receipt snapshots", async () => {
    const receipts = new MemoryWorkflowReceiptStore();
    const receipt = (
      instanceId: string,
      state: CloudflareWorkflowReceiptRecord["state"] = "CREATED",
      leaseExpiresAt: number | null = null,
    ) => {
      receipts.records.set(`binding\u0000${instanceId}`, {
        workflowName: "daily",
        envelopeHash: instanceId.padEnd(64, "x"),
        state,
        owner: state === "CREATED" ? null : "owner",
        fence: 2,
        leaseExpiresAt,
        checkAfter: 900,
      });
    };
    receipt("absent");
    receipt("existing");
    receipt("ambiguous", "ABSENCE_PROVEN", 800);
    receipt("active", "PENDING", 2_000);

    const handler = createCloudflareDispatchHandler({
      registry: defineWorkflowRegistry([]),
      resolveWorkflow: () => ({
        create: async () => ({}),
        get: async (id: string) => {
          if (id === "absent") throw new Error("instance.not_found");
          return {
            status: async () => {
              if (id === "ambiguous") {
                throw new Error("Workflow status temporarily unavailable");
              }
              return { status: "unknown" };
            },
          };
        },
      }),
      resolveWorkflowIdentity: () => "binding",
      resolveReceiptStore: () => receipts,
      receiptCleanupBatchSize: 6,
      receiptCleanupMaxBatches: 2,
      receiptCleanupRecheckMs: 100,
    });

    await handler.scheduled({ scheduledTime: 1_000 }, {});

    expect(receipts.records.has("binding\u0000absent")).toBe(false);
    expect(receipts.records.get("binding\u0000existing")?.checkAfter).toBe(
      1_100,
    );
    expect(receipts.records.get("binding\u0000ambiguous")?.checkAfter).toBe(
      1_100,
    );
    expect(receipts.records.get("binding\u0000active")?.checkAfter).toBe(900);
  });

  test("requires stable binding identity and bounded receipt settings", () => {
    const registry = defineWorkflowRegistry([]);
    expect(() =>
      createCloudflareDispatchHandler({
        registry,
        resolveWorkflow: () => null,
        resolveReceiptStore: () => new MemoryWorkflowReceiptStore(),
      }),
    ).toThrow("resolveWorkflowIdentity is required");
    expect(() =>
      createCloudflareDispatchHandler({
        registry,
        resolveWorkflow: () => null,
        resolveWorkflowIdentity: () => "binding",
        receiptLeaseMs: 2_000,
        receiptRetentionMs: 1_000,
      }),
    ).toThrow("at least as large as receiptLeaseMs");
    expect(() =>
      createCloudflareDispatchHandler({
        registry,
        resolveWorkflow: () => null,
        resolveWorkflowIdentity: () => "binding",
        receiptCleanupBatchSize: 7,
        receiptCleanupMaxBatches: 2,
      }),
    ).toThrow("limited to 12 candidates");
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
          create: async (options) => {
            created.push(options);
            return options;
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

  test("scheduled cron redelivery produces an identical canonical envelope", async () => {
    const workflow = defineWorkflow("daily", {
      cron: [{ name: "nine", schedule: "0 9 * * *" }],
      run: () => undefined,
    });
    const created: unknown[] = [];
    const handler = createCloudflareDispatchHandler({
      registry: defineWorkflowRegistry([workflow]),
      resolveWorkflow: () => ({
        create: async ({ params }) => {
          created.push(params);
          return {};
        },
      }),
    });
    const controller = {
      scheduledTime: Date.parse("2026-05-24T09:05:00.000Z"),
    };

    await handler.scheduled(controller, {});
    await handler.scheduled(controller, {});

    expect(created).toHaveLength(2);
    expect(created[1]).toEqual(created[0]);
    expect(
      (created[0] as { traceId: string }).traceId.startsWith("trace_cron_"),
    ).toBe(true);
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

  test("rejects dispatch batches above the instance amplification limit", async () => {
    const workflow = defineWorkflow("email/send", { run: () => undefined });
    let created = 0;
    const handler = createCloudflareDispatchHandler({
      registry: defineWorkflowRegistry([workflow]),
      maxEventsPerRequest: 2,
      resolveWorkflow() {
        return {
          create: async () => {
            created += 1;
            return {};
          },
        };
      },
    });
    const event = {
      id: "wf_1",
      name: "email/send",
      payload: {},
      traceId: "trace",
      idempotencyKey: "idem",
      createdAt: new Date().toISOString(),
    };

    const response = await handler.fetch(
      new Request("https://example.com/dispatch", {
        method: "POST",
        body: JSON.stringify({
          events: [event, { ...event, id: "wf_2" }, { ...event, id: "wf_3" }],
        }),
      }),
      {},
    );

    expect(response.status).toBe(413);
    expect(created).toBe(0);
  });

  test("enforces the hard hundred-event cap when the softer cap is disabled", async () => {
    const workflow = defineWorkflow("email/send", { run: () => undefined });
    let created = 0;
    const handler = createCloudflareDispatchHandler({
      registry: defineWorkflowRegistry([workflow]),
      maxEventsPerRequest: false,
      resolveWorkflow() {
        return {
          create: async () => {
            created += 1;
            return {};
          },
        };
      },
    });
    const events = Array.from({ length: 101 }, (_, index) => ({
      id: `wf_${index}`,
      name: "email/send",
      payload: {},
      traceId: `trace_${index}`,
      idempotencyKey: `idem_${index}`,
      createdAt: "2026-08-27T00:00:00.000Z",
    }));

    const response = await handler.fetch(
      new Request("https://example.com/dispatch", {
        method: "POST",
        body: JSON.stringify({ events }),
      }),
      {},
    );

    expect(response.status).toBe(413);
    expect(created).toBe(0);
    await expect(response.json()).resolves.toEqual({
      error: "Too many events; maximum is 100",
    });
  });

  test("funds one status-backed cleanup candidate for every prepared event", async () => {
    const workflow = defineWorkflow("course/rebuild", { run: () => undefined });
    const receipts = new MemoryWorkflowReceiptStore();
    for (let index = 0; index < 100; index += 1) {
      receipts.records.set(`binding\u0000expired_${index}`, {
        workflowName: "course/rebuild",
        envelopeHash: String(index).padStart(64, "0"),
        state: "CREATED",
        owner: null,
        fence: 1,
        leaseExpiresAt: null,
        checkAfter: 900,
      });
    }
    let cleanupStatusCalls = 0;
    let admissionStatusCalls = 0;
    const batchSizes: number[] = [];
    const binding = {
      create: async () => ({}),
      createBatch: async (batch: Array<{ id: string }>) => {
        batchSizes.push(batch.length);
        return batch;
      },
      get: async (id: string) => {
        if (id.startsWith("expired_")) cleanupStatusCalls += 1;
        else admissionStatusCalls += 1;
        throw new Error("instance.not_found");
      },
    };
    const handler = createCloudflareDispatchHandler({
      registry: defineWorkflowRegistry([workflow]),
      resolveWorkflow: () => binding,
      resolveWorkflowIdentity: () => "binding",
      resolveReceiptStore: () => receipts,
      now: () => new Date(1_000),
    });
    const events = Array.from({ length: 100 }, (_, index) => ({
      id: `wf_new_${index}`,
      name: "course/rebuild",
      payload: { courseId: `course_${index}` },
      traceId: `trace_${index}`,
      idempotencyKey: `idem_${index}`,
      createdAt: "2026-08-27T00:00:00.000Z",
    }));

    const response = await handler.fetch(
      new Request("https://example.com/dispatch", {
        method: "POST",
        body: JSON.stringify({ events }),
      }),
      {},
    );
    const result = (await response.json()) as { ids: string[] };

    expect(response.status).toBe(200);
    expect(result.ids).toHaveLength(100);
    expect(receipts.cleanupListLimits).toEqual([100]);
    expect(cleanupStatusCalls).toBe(100);
    expect(admissionStatusCalls).toBe(100);
    expect(batchSizes).toEqual([100]);
  });

  test("invalid and duplicate items do not inflate the admission cleanup budget", async () => {
    const workflow = defineWorkflow("course/rebuild", { run: () => undefined });
    const receipts = new MemoryWorkflowReceiptStore();
    for (let index = 0; index < 2; index += 1) {
      receipts.records.set(`binding\u0000expired_${index}`, {
        workflowName: "course/rebuild",
        envelopeHash: String(index).padStart(64, "0"),
        state: "CREATED",
        owner: null,
        fence: 1,
        leaseExpiresAt: null,
        checkAfter: 900,
      });
    }
    let cleanupStatusCalls = 0;
    let created = 0;
    const binding = {
      create: async () => ({}),
      createBatch: async (batch: Array<{ id: string }>) => {
        created += batch.length;
        return batch;
      },
      get: async (id: string) => {
        if (id.startsWith("expired_")) cleanupStatusCalls += 1;
        throw new Error("instance.not_found");
      },
    };
    const handler = createCloudflareDispatchHandler({
      registry: defineWorkflowRegistry([workflow]),
      resolveWorkflow: () => binding,
      resolveWorkflowIdentity: () => "binding",
      resolveReceiptStore: () => receipts,
      now: () => new Date(1_000),
    });
    const event = {
      id: "wf_unique",
      name: "course/rebuild",
      payload: { version: 1 },
      traceId: "trace_1",
      idempotencyKey: "idem_1",
      createdAt: "2026-08-27T00:00:00.000Z",
    };
    const events: unknown[] = [
      event,
      ...Array.from({ length: 97 }, () => ({ ...event })),
      { ...event, payload: { version: 2 } },
      null,
    ];

    const response = await handler.fetch(
      new Request("https://example.com/dispatch", {
        method: "POST",
        body: JSON.stringify({ events }),
      }),
      {},
    );
    const result = (await response.json()) as {
      ids: string[];
      errors: Array<{ id: string; error: string }>;
    };

    expect(response.status).toBe(200);
    expect(result.ids).toEqual(["wf_unique"]);
    expect(result.errors).toHaveLength(2);
    expect(receipts.cleanupListLimits).toEqual([1]);
    expect(cleanupStatusCalls).toBe(1);
    expect(created).toBe(1);
    expect(
      [...receipts.records.keys()].filter((key) => key.includes("expired")),
    ).toHaveLength(1);
  });

  test("fails before claiming receipts or creating instances when admission cleanup fails", async () => {
    class FailingCleanupStore extends MemoryWorkflowReceiptStore {
      override async listCleanupCandidates(input: {
        checkBefore: number;
        limit: number;
      }): Promise<CloudflareWorkflowReceiptCleanupCandidate[]> {
        this.cleanupListLimits.push(input.limit);
        throw new Error("D1 cleanup unavailable");
      }
    }

    const workflow = defineWorkflow("course/rebuild", { run: () => undefined });
    const receipts = new FailingCleanupStore();
    let bindingOperations = 0;
    const handler = createCloudflareDispatchHandler({
      registry: defineWorkflowRegistry([workflow]),
      resolveWorkflow: () => ({
        create: async () => {
          bindingOperations += 1;
          return {};
        },
        createBatch: async (batch) => {
          bindingOperations += 1;
          return batch;
        },
        get: async () => {
          bindingOperations += 1;
          throw new Error("instance.not_found");
        },
      }),
      resolveWorkflowIdentity: () => "binding",
      resolveReceiptStore: () => receipts,
      now: () => new Date(1_000),
    });

    const response = await handler.fetch(
      new Request("https://example.com/dispatch", {
        method: "POST",
        body: JSON.stringify({
          events: [
            {
              id: "wf_blocked",
              name: "course/rebuild",
              payload: {},
              traceId: "trace_1",
              idempotencyKey: "idem_1",
              createdAt: "2026-08-27T00:00:00.000Z",
            },
          ],
        }),
      }),
      {},
    );

    expect(response.status).toBe(503);
    expect(receipts.cleanupListLimits).toEqual([1]);
    expect(receipts.records.size).toBe(0);
    expect(bindingOperations).toBe(0);
    await expect(response.json()).resolves.toEqual({
      error: "Workflow receipt maintenance failed: D1 cleanup unavailable",
    });
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
    expect(created[0]).toMatchObject({
      retention: {
        successRetention: "1 day",
        errorRetention: "3 days",
      },
    });
  });

  test("chunks large createBatch calls below the Workflow RPC envelope", async () => {
    const workflow = defineWorkflow("course/rebuild", { run: () => undefined });
    const batches: unknown[][] = [];
    const binding = {
      create: async () => ({}),
      createBatch: async (batch: unknown[]) => {
        batches.push(batch);
        return batch;
      },
    };
    const handler = createCloudflareDispatchHandler({
      registry: defineWorkflowRegistry([workflow]),
      maxRequestBytes: false,
      resolveWorkflow() {
        return binding;
      },
    });
    const events = Array.from({ length: 11 }, (_, index) => ({
      id: `wf_large_${index}`,
      name: "course/rebuild",
      payload: { content: "x".repeat(90_000) },
      traceId: `trace_${index}`,
      idempotencyKey: `idem_${index}`,
      createdAt: "2026-05-24T09:00:00.000Z",
    }));

    const response = await handler.fetch(
      new Request("https://example.com/dispatch", {
        method: "POST",
        body: JSON.stringify({ events }),
      }),
      {},
    );

    expect(response.status).toBe(200);
    expect(batches.length).toBeGreaterThan(1);
    expect(batches.flat()).toHaveLength(events.length);
    for (const batch of batches) {
      expect(batch.length).toBeLessThanOrEqual(100);
      expect(new TextEncoder().encode(JSON.stringify(batch)).byteLength).toBeLessThan(
        1_000_000,
      );
    }
  });

  test("collapses identical same-binding IDs and rejects conflicting duplicates", async () => {
    const workflow = defineWorkflow("course/rebuild", { run: () => undefined });
    const batches: unknown[][] = [];
    const binding = {
      create: async () => ({}),
      createBatch: async (batch: unknown[]) => {
        batches.push(batch);
        return batch;
      },
    };
    const handler = createCloudflareDispatchHandler({
      registry: defineWorkflowRegistry([workflow]),
      resolveWorkflow() {
        return binding;
      },
    });
    const first = {
      id: "wf_same",
      name: "course/rebuild",
      payload: { version: 1 },
      traceId: "trace_1",
      idempotencyKey: "idem_1",
      createdAt: "2026-08-27T00:00:00.000Z",
    };

    const response = await handler.fetch(
      new Request("https://example.com/dispatch", {
        method: "POST",
        body: JSON.stringify({
          events: [first, { ...first }, { ...first, payload: { version: 2 } }],
        }),
      }),
      {},
    );

    expect(batches).toHaveLength(1);
    expect(batches[0]).toHaveLength(1);
    await expect(response.json()).resolves.toMatchObject({
      ids: ["wf_same"],
      errors: [
        {
          id: "wf_same",
          error: "Conflicting duplicate Workflow instance id in one request",
        },
      ],
    });
  });

  test("does not accept a createBatch id omitted as an existing cross-request duplicate", async () => {
    const workflow = defineWorkflow("course/rebuild", { run: () => undefined });
    const existing = new Set<string>();
    const handler = createCloudflareDispatchHandler({
      registry: defineWorkflowRegistry([workflow]),
      resolveWorkflow() {
        return {
          create: async () => ({}),
          createBatch: async (batch) =>
            batch.filter((item) => {
              if (existing.has(item.id)) return false;
              existing.add(item.id);
              return true;
            }),
        };
      },
    });
    const event = {
      id: "wf_existing",
      name: "course/rebuild",
      payload: { version: 1 },
      traceId: "trace_1",
      idempotencyKey: "idem_1",
      createdAt: "2026-08-27T00:00:00.000Z",
    };

    const first = await handler.fetch(
      new Request("https://example.com/dispatch", {
        method: "POST",
        body: JSON.stringify({ events: [event] }),
      }),
      {},
    );
    const replay = await handler.fetch(
      new Request("https://example.com/dispatch", {
        method: "POST",
        body: JSON.stringify({
          events: [{ ...event, payload: { version: 2 } }],
        }),
      }),
      {},
    );

    await expect(first.json()).resolves.toMatchObject({ ids: ["wf_existing"] });
    await expect(replay.json()).resolves.toMatchObject({
      ids: [],
      errors: [
        {
          id: "wf_existing",
          error: expect.stringContaining("refusing an unverified existing id"),
        },
      ],
    });
  });

  test("does not accept an individual create conflict as an idempotent replay", async () => {
    const workflow = defineWorkflow("course/rebuild", { run: () => undefined });
    const existing = new Set<string>();
    const handler = createCloudflareDispatchHandler({
      registry: defineWorkflowRegistry([workflow]),
      resolveWorkflow() {
        return {
          create: async ({ id }) => {
            if (id && existing.has(id)) {
              throw new Error("Workflow instance already exists");
            }
            if (id) existing.add(id);
            return { id };
          },
        };
      },
    });
    const event = {
      id: "wf_existing",
      name: "course/rebuild",
      payload: { version: 1 },
      traceId: "trace_1",
      idempotencyKey: "idem_1",
      createdAt: "2026-08-27T00:00:00.000Z",
    };

    const first = await handler.fetch(
      new Request("https://example.com/dispatch", {
        method: "POST",
        body: JSON.stringify({ events: [event] }),
      }),
      {},
    );
    const replay = await handler.fetch(
      new Request("https://example.com/dispatch", {
        method: "POST",
        body: JSON.stringify({ events: [event] }),
      }),
      {},
    );

    await expect(first.json()).resolves.toMatchObject({ ids: ["wf_existing"] });
    await expect(replay.json()).resolves.toMatchObject({
      ids: [],
      errors: [
        { id: "wf_existing", error: "Workflow instance already exists" },
      ],
    });
  });

  for (const batchResult of ["omission", "error"] as const) {
    test(`fails closed on a migration-gap instance before batch ${batchResult}`, async () => {
      const workflow = defineWorkflow("course/rebuild", {
        run: () => undefined,
      });
      const receipts = new MemoryWorkflowReceiptStore();
      let batchCalls = 0;
      let statusCalls = 0;
      const binding = {
        create: async () => ({}),
        createBatch: async () => {
          batchCalls += 1;
          if (batchResult === "error") {
            throw new Error("duplicate id in batch");
          }
          return [];
        },
        get: async () => ({
          status: async () => {
            statusCalls += 1;
            return { status: "running" };
          },
        }),
      };
      const makeHandler = () =>
        createCloudflareDispatchHandler({
          registry: defineWorkflowRegistry([workflow]),
          resolveWorkflow: () => binding,
          resolveWorkflowIdentity: () => "generic-callback-binding",
          resolveReceiptStore: () => receipts,
          receiptOwnerGenerator: () => crypto.randomUUID(),
          now: () => new Date("2026-08-27T00:00:00.000Z"),
        });
      const base = {
        id: "wf_preexisting",
        name: "course/rebuild",
        traceId: "trace_1",
        idempotencyKey: "idem_1",
        createdAt: "2026-08-27T00:00:00.000Z",
      };
      const request = (payload: Record<string, unknown>) =>
        new Request("https://example.com/dispatch", {
          method: "POST",
          body: JSON.stringify({ events: [{ ...base, payload }] }),
        });

      const first = await makeHandler().fetch(
        request({ courseId: "preexisting" }),
        {},
      );
      const differentEnvelope = await makeHandler().fetch(
        request({ courseId: "different" }),
        {},
      );

      await expect(first.json()).resolves.toMatchObject({
        ids: [],
        errors: [
          {
            id: "wf_preexisting",
            error: expect.stringContaining("no matching durable absence proof"),
          },
        ],
      });
      await expect(differentEnvelope.json()).resolves.toMatchObject({
        ids: [],
        errors: [
          {
            id: "wf_preexisting",
            error: expect.stringContaining("receipt conflict"),
          },
        ],
      });
      expect(statusCalls).toBe(1);
      expect(batchCalls).toBe(0);
      expect(
        receipts.records.get("generic-callback-binding\u0000wf_preexisting")
          ?.state,
      ).toBe("PENDING");
    });
  }

  test("recovers an ambiguously committed create and rejects a fresh conflicting envelope", async () => {
    const workflow = defineWorkflow("course/rebuild", { run: () => undefined });
    const receipts = new MemoryWorkflowReceiptStore();
    let createCalls = 0;
    let statusCalls = 0;
    let exists = false;
    let owner = 0;
    const binding = {
      create: async () => {
        createCalls += 1;
        exists = true;
        throw new Error("connection closed after create");
      },
      get: async () => {
        if (!exists) throw new Error("instance.not_found");
        return {
          status: async () => {
            statusCalls += 1;
            return { status: "running" };
          },
        };
      },
    };
    const makeHandler = () =>
      createCloudflareDispatchHandler({
        registry: defineWorkflowRegistry([workflow]),
        resolveWorkflow: () => binding,
        resolveWorkflowIdentity: () => "generic-callback-binding",
        resolveReceiptStore: () => receipts,
        receiptOwnerGenerator: () => `owner_${++owner}`,
        now: () => new Date("2026-08-27T00:00:00.000Z"),
      });
    const event = {
      id: "wf_ambiguous",
      name: "course/rebuild",
      payload: { courseId: "course_1", version: 1 },
      traceId: "trace_1",
      idempotencyKey: "idem_1",
      createdAt: "2026-08-27T00:00:00.000Z",
    };
    const request = (candidate: typeof event) =>
      new Request("https://example.com/dispatch", {
        method: "POST",
        body: JSON.stringify({ events: [candidate] }),
      });

    const committed = await makeHandler().fetch(request(event), {});
    const exactRetry = await makeHandler().fetch(
      request({
        ...event,
        payload: { version: 1, courseId: "course_1" },
      }),
      {},
    );
    const conflict = await makeHandler().fetch(
      request({ ...event, payload: { courseId: "course_2", version: 1 } }),
      {},
    );

    await expect(committed.json()).resolves.toMatchObject({
      ids: ["wf_ambiguous"],
    });
    await expect(exactRetry.json()).resolves.toMatchObject({
      ids: ["wf_ambiguous"],
    });
    await expect(conflict.json()).resolves.toMatchObject({
      ids: [],
      errors: [
        {
          id: "wf_ambiguous",
          error: expect.stringContaining("receipt conflict"),
        },
      ],
    });
    expect(createCalls).toBe(1);
    expect(statusCalls).toBe(1);
  });

  for (const [description, statusResult] of [
    ["an unrecognized new status", { status: "rollingBack" }],
    ["a status object without a status field", { output: null }],
  ] as const) {
    test(`treats ${description} from a successful lookup as existing`, async () => {
      const workflow = defineWorkflow("course/rebuild", {
        run: () => undefined,
      });
      const receipts = new MemoryWorkflowReceiptStore();
      let createCalls = 0;
      let getCalls = 0;
      let statusCalls = 0;
      const binding = {
        create: async () => {
          createCalls += 1;
          throw new Error("connection closed after create");
        },
        get: async () => {
          getCalls += 1;
          if (getCalls === 1) throw new Error("instance.not_found");
          return {
            status: async () => {
              statusCalls += 1;
              return statusResult;
            },
          };
        },
      };
      const handler = createCloudflareDispatchHandler({
        registry: defineWorkflowRegistry([workflow]),
        resolveWorkflow: () => binding,
        resolveWorkflowIdentity: () => "generic-callback-binding",
        resolveReceiptStore: () => receipts,
        receiptOwnerGenerator: () => "owner_1",
        now: () => new Date("2026-08-27T00:00:00.000Z"),
      });

      const response = await handler.fetch(
        new Request("https://example.com/dispatch", {
          method: "POST",
          body: JSON.stringify({
            events: [
              {
                id: "wf_future_status",
                name: "course/rebuild",
                payload: { courseId: "course_1" },
                traceId: "trace_1",
                idempotencyKey: "idem_1",
                createdAt: "2026-08-27T00:00:00.000Z",
              },
            ],
          }),
        }),
        {},
      );

      await expect(response.json()).resolves.toMatchObject({
        ids: ["wf_future_status"],
      });
      expect(createCalls).toBe(1);
      expect(getCalls).toBe(2);
      expect(statusCalls).toBe(1);
      expect(
        receipts.records.get(
          "generic-callback-binding\u0000wf_future_status",
        )?.state,
      ).toBe("CREATED");
    });
  }

  test("retries create only after an explicit instance-not-found error", async () => {
    const workflow = defineWorkflow("course/rebuild", { run: () => undefined });
    const receipts = new MemoryWorkflowReceiptStore();
    let createCalls = 0;
    let getCalls = 0;
    let owner = 0;
    let now = Date.parse("2026-08-27T00:00:00.000Z");
    const binding = {
      create: async ({ id }: { id?: string }) => {
        createCalls += 1;
        if (createCalls === 1) {
          throw new Error("connection failed before create");
        }
        return { id };
      },
      get: async () => {
        getCalls += 1;
        if (getCalls === 1) {
          throw new Error("instance.not_found");
        }
        if (getCalls === 2) {
          return {
            status: async () => {
              throw new Error("Workflow status temporarily unavailable");
            },
          };
        }
        throw new Error("instance.not_found");
      },
    };
    const handler = createCloudflareDispatchHandler({
      registry: defineWorkflowRegistry([workflow]),
      resolveWorkflow: () => binding,
      resolveWorkflowIdentity: () => "generic-callback-binding",
      resolveReceiptStore: () => receipts,
      receiptLeaseMs: 1_000,
      receiptOwnerGenerator: () => `owner_${++owner}`,
      now: () => new Date(now),
    });
    const body = JSON.stringify({
      events: [
        {
          id: "wf_explicitly_missing",
          name: "course/rebuild",
          payload: { courseId: "course_1" },
          traceId: "trace_1",
          idempotencyKey: "idem_1",
          createdAt: "2026-08-27T00:00:00.000Z",
        },
      ],
    });

    const ambiguous = await handler.fetch(
      new Request("https://example.com/dispatch", { method: "POST", body }),
      {},
    );
    now += 1_001;
    const retry = await handler.fetch(
      new Request("https://example.com/dispatch", { method: "POST", body }),
      {},
    );

    await expect(ambiguous.json()).resolves.toMatchObject({
      ids: [],
      errors: [
        {
          id: "wf_explicitly_missing",
          error: "Workflow status temporarily unavailable",
        },
      ],
    });
    await expect(retry.json()).resolves.toMatchObject({
      ids: ["wf_explicitly_missing"],
    });
    expect(createCalls).toBe(2);
    expect(getCalls).toBe(3);
  });

  test("does not infer instance absence from an unrelated not-found message", async () => {
    const workflow = defineWorkflow("course/rebuild", { run: () => undefined });
    const receipts = new MemoryWorkflowReceiptStore();
    let createCalls = 0;
    let owner = 0;
    const binding = {
      create: async () => {
        createCalls += 1;
        throw new Error("connection failed before create");
      },
      get: async () => ({
        status: async () => {
          throw new Error("Workflow instance status not found");
        },
      }),
    };
    const handler = createCloudflareDispatchHandler({
      registry: defineWorkflowRegistry([workflow]),
      resolveWorkflow: () => binding,
      resolveWorkflowIdentity: () => "generic-callback-binding",
      resolveReceiptStore: () => receipts,
      receiptOwnerGenerator: () => `owner_${++owner}`,
      now: () => new Date("2026-08-27T00:00:00.000Z"),
    });
    const body = JSON.stringify({
      events: [
        {
          id: "wf_status_lookup_error",
          name: "course/rebuild",
          payload: { courseId: "course_1" },
          traceId: "trace_1",
          idempotencyKey: "idem_1",
          createdAt: "2026-08-27T00:00:00.000Z",
        },
      ],
    });

    const ambiguous = await handler.fetch(
      new Request("https://example.com/dispatch", { method: "POST", body }),
      {},
    );
    const immediateRetry = await handler.fetch(
      new Request("https://example.com/dispatch", { method: "POST", body }),
      {},
    );

    await expect(ambiguous.json()).resolves.toMatchObject({
      ids: [],
      errors: [
        {
          id: "wf_status_lookup_error",
          error: "Workflow instance status not found",
        },
      ],
    });
    await expect(immediateRetry.json()).resolves.toMatchObject({
      ids: [],
      errors: [
        {
          id: "wf_status_lookup_error",
          error: expect.stringContaining("being created by another request"),
        },
      ],
    });
    expect(createCalls).toBe(0);
  });

  test("accepts a batch omission only after named status proves creation", async () => {
    const workflow = defineWorkflow("course/rebuild", { run: () => undefined });
    const receipts = new MemoryWorkflowReceiptStore();
    const existing = new Set<string>();
    let batchCalls = 0;
    let statusCalls = 0;
    let owner = 0;
    const binding = {
      create: async () => ({}),
      createBatch: async (batch: Array<{ id: string }>) => {
        batchCalls += 1;
        for (const item of batch) existing.add(item.id);
        return [];
      },
      get: async (id: string) => {
        if (!existing.has(id)) throw new Error("instance.not_found");
        return {
          status: async () => {
            statusCalls += 1;
            return { status: "queued" };
          },
        };
      },
    };
    const makeHandler = () =>
      createCloudflareDispatchHandler({
        registry: defineWorkflowRegistry([workflow]),
        resolveWorkflow: () => binding,
        resolveWorkflowIdentity: () => "generic-callback-binding",
        resolveReceiptStore: () => receipts,
        receiptOwnerGenerator: () => `owner_${++owner}`,
        now: () => new Date("2026-08-27T00:00:00.000Z"),
      });
    const body = JSON.stringify({
      events: [
        {
          id: "wf_batch_omitted",
          name: "course/rebuild",
          payload: { courseId: "course_1" },
          traceId: "trace_1",
          idempotencyKey: "idem_1",
          createdAt: "2026-08-27T00:00:00.000Z",
        },
      ],
    });

    const first = await makeHandler().fetch(
      new Request("https://example.com/dispatch", { method: "POST", body }),
      {},
    );
    const retry = await makeHandler().fetch(
      new Request("https://example.com/dispatch", { method: "POST", body }),
      {},
    );

    await expect(first.json()).resolves.toMatchObject({
      ids: ["wf_batch_omitted"],
    });
    await expect(retry.json()).resolves.toMatchObject({
      ids: ["wf_batch_omitted"],
    });
    expect(batchCalls).toBe(1);
    expect(statusCalls).toBe(1);
  });

  test("accepts a batch error only after durable absence proof and named status reconciliation", async () => {
    const workflow = defineWorkflow("course/rebuild", { run: () => undefined });
    const receipts = new MemoryWorkflowReceiptStore();
    let exists = false;
    let batchCalls = 0;
    let statusCalls = 0;
    const binding = {
      create: async () => ({}),
      createBatch: async () => {
        batchCalls += 1;
        exists = true;
        throw new Error("connection closed after batch commit");
      },
      get: async () => {
        if (!exists) throw new Error("instance.not_found");
        return {
          status: async () => {
            statusCalls += 1;
            return { status: "running" };
          },
        };
      },
    };
    const handler = createCloudflareDispatchHandler({
      registry: defineWorkflowRegistry([workflow]),
      resolveWorkflow: () => binding,
      resolveWorkflowIdentity: () => "generic-callback-binding",
      resolveReceiptStore: () => receipts,
      receiptOwnerGenerator: () => "owner_1",
      now: () => new Date("2026-08-27T00:00:00.000Z"),
    });

    const response = await handler.fetch(
      new Request("https://example.com/dispatch", {
        method: "POST",
        body: JSON.stringify({
          events: [
            {
              id: "wf_batch_error",
              name: "course/rebuild",
              payload: { courseId: "course_1" },
              traceId: "trace_1",
              idempotencyKey: "idem_1",
              createdAt: "2026-08-27T00:00:00.000Z",
            },
          ],
        }),
      }),
      {},
    );

    await expect(response.json()).resolves.toMatchObject({
      ids: ["wf_batch_error"],
    });
    expect(batchCalls).toBe(1);
    expect(statusCalls).toBe(1);
    expect(
      receipts.records.get("generic-callback-binding\u0000wf_batch_error")
        ?.state,
    ).toBe("CREATED");
  });

  test("does not fan out any createBatch conflict into individual creates", async () => {
    const first = defineWorkflow("first", { run: () => undefined });
    const second = defineWorkflow("second", { run: () => undefined });
    let individualCalls = 0;
    const handler = createCloudflareDispatchHandler({
      registry: defineWorkflowRegistry([first, second]),
      resolveWorkflow() {
        return {
          create: async () => {
            individualCalls += 1;
            return {};
          },
          createBatch: async () => {
            throw new Error("duplicate id in batch");
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
      errors?: Array<{ id: string; error: string }>;
    };
    expect(individualCalls).toBe(0);
    expect(body.ids).toEqual([]);
    expect(body.errors).toEqual([
      { id: "wf_1", error: "duplicate id in batch" },
      { id: "wf_2", error: "duplicate id in batch" },
    ]);
  });

  test("does not fan out a transient createBatch failure into individual calls", async () => {
    const first = defineWorkflow("first", { run: () => undefined });
    const second = defineWorkflow("second", { run: () => undefined });
    let individualCalls = 0;
    const handler = createCloudflareDispatchHandler({
      registry: defineWorkflowRegistry([first, second]),
      resolveWorkflow() {
        return {
          create: async () => {
            individualCalls += 1;
            return {};
          },
          createBatch: async () => {
            throw new Error("Cloudflare service unavailable");
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
              id: "wf_1",
              name: "first",
              payload: {},
              traceId: "trace_1",
              idempotencyKey: "idem_1",
              createdAt: "2026-05-24T09:00:00.000Z",
            },
            {
              id: "wf_2",
              name: "second",
              payload: {},
              traceId: "trace_2",
              idempotencyKey: "idem_2",
              createdAt: "2026-05-24T09:00:00.000Z",
            },
          ],
        }),
      }),
      {},
    );

    expect(individualCalls).toBe(0);
    await expect(response.json()).resolves.toMatchObject({
      ids: [],
      errors: [
        { id: "wf_1", error: "Cloudflare service unavailable" },
        { id: "wf_2", error: "Cloudflare service unavailable" },
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
        {
          id: "unknown",
          error:
            "Invalid event structure at index 0: Workflow event must be a plain object",
        },
        {
          id: "wf_bad",
          error:
            "Invalid event structure at index 1: event.idempotencyKey must contain 1-512 characters",
        },
      ],
    });
  });

  test("rejects an oversized event without creating a Workflow instance", async () => {
    const workflow = defineWorkflow("course/rebuild", { run: () => undefined });
    let created = 0;
    const handler = createCloudflareDispatchHandler({
      registry: defineWorkflowRegistry([workflow]),
      maxRequestBytes: false,
      resolveWorkflow() {
        return {
          create: async () => {
            created += 1;
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
              id: "wf_too_large",
              name: "course/rebuild",
              payload: { content: "x".repeat(96_000) },
              traceId: "trace",
              idempotencyKey: "idem",
              createdAt: "2026-05-24T09:00:00.000Z",
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
          id: "wf_too_large",
          error:
            "Invalid event structure at index 0: Workflow event exceeds 96000 bytes",
        },
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

  test("rejects non-JSON schema transforms before hashing or creating receipts", async () => {
    class TransformedPayload {
      readonly value = "class-instance";
    }
    const workflow = defineWorkflow("payload/transform", {
      schema: {
        parse(value) {
          const kind = (value as { kind?: string }).kind;
          if (kind?.startsWith("date:")) {
            return { transformed: new Date(kind.slice("date:".length)) };
          }
          if (kind === "class") {
            return { transformed: new TransformedPayload() };
          }
          if (kind === "accessor") {
            const transformed: Record<string, unknown> = {};
            Object.defineProperty(transformed, "secret", {
              enumerable: true,
              get: () => "value",
            });
            return { transformed };
          }
          return { transformed: () => "non-json" };
        },
      },
      run: () => undefined,
    });
    const receipts = new MemoryWorkflowReceiptStore();
    let created = 0;
    const handler = createCloudflareDispatchHandler({
      registry: defineWorkflowRegistry([workflow]),
      resolveWorkflow: () => ({
        create: async () => {
          created += 1;
          return {};
        },
      }),
      resolveWorkflowIdentity: () => "transform-binding",
      resolveReceiptStore: () => receipts,
    });
    const base = {
      id: "wf_transform",
      name: "payload/transform",
      traceId: "trace",
      idempotencyKey: "idem",
      createdAt: "2026-08-27T00:00:00.000Z",
    };

    const response = await handler.fetch(
      new Request("https://example.com/dispatch", {
        method: "POST",
        body: JSON.stringify({
          events: [
            {
              ...base,
              payload: { kind: "date:2026-01-01T00:00:00.000Z" },
            },
            {
              ...base,
              payload: { kind: "date:2027-01-01T00:00:00.000Z" },
            },
            { ...base, id: "wf_class", payload: { kind: "class" } },
            { ...base, id: "wf_accessor", payload: { kind: "accessor" } },
            { ...base, id: "wf_function", payload: { kind: "function" } },
          ],
        }),
      }),
      {},
    );
    const result = (await response.json()) as {
      ids: string[];
      errors: Array<{ id: string; error: string }>;
    };

    expect(result.ids).toEqual([]);
    expect(result.errors).toHaveLength(5);
    expect(result.errors[0]?.error).toContain("non-plain object");
    expect(result.errors[1]?.error).toContain("non-plain object");
    expect(result.errors[2]?.error).toContain("non-plain object");
    expect(result.errors[3]?.error).toContain("accessor");
    expect(result.errors[4]?.error).toContain("non-JSON value function");
    expect(receipts.records.size).toBe(0);
    expect(created).toBe(0);
  });
});
