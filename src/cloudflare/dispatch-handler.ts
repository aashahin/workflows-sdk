import { collectDueCronRuns } from "../scheduler/cron";
import { createTraceId, createWorkflowId } from "../core/id";
import type { WorkflowRegistry } from "../core/registry";
import type {
  DispatchOptions,
  DispatchResult,
  WorkflowEventEnvelope,
  WorkflowInstance,
  WorkflowPayload,
} from "../core/types";
import { assertCloudflareWorkflowEnvelope } from "./serialization";

type CloudflareDurationLabel =
  | "second"
  | "minute"
  | "hour"
  | "day"
  | "week"
  | "month"
  | "year";

export type CloudflareWorkflowDuration =
  | number
  | `${number} ${CloudflareDurationLabel}${"" | "s"}`;

export interface CloudflareWorkflowRetention {
  successRetention: CloudflareWorkflowDuration;
  errorRetention: CloudflareWorkflowDuration;
}

export interface CloudflareWorkflowReceiptClaimInput {
  workflowIdentity: string;
  workflowName: string;
  instanceId: string;
  envelopeHash: string;
  owner: string;
  now: number;
  leaseExpiresAt: number;
  checkAfter: number;
}

export type CloudflareWorkflowReceiptClaim =
  | {
      outcome: "claimed";
      fence: number;
      previousState: "new" | "pending" | "absence_proven";
    }
  | { outcome: "created" }
  | { outcome: "busy"; retryAfter: number }
  | { outcome: "conflict" };

export interface CloudflareWorkflowReceiptRecord {
  workflowName: string;
  envelopeHash: string;
  state: "PENDING" | "ABSENCE_PROVEN" | "CREATED";
  owner: string | null;
  fence: number;
  leaseExpiresAt: number | null;
  checkAfter: number;
}

export interface CloudflareWorkflowReceiptCleanupCandidate
  extends CloudflareWorkflowReceiptRecord {
  workflowIdentity: string;
  instanceId: string;
}

export interface CloudflareWorkflowReceiptStore {
  claim(
    input: CloudflareWorkflowReceiptClaimInput,
  ): Promise<CloudflareWorkflowReceiptClaim>;
  get(input: {
    workflowIdentity: string;
    instanceId: string;
  }): Promise<CloudflareWorkflowReceiptRecord | null>;
  markAbsenceProven(input: {
    workflowIdentity: string;
    instanceId: string;
    envelopeHash: string;
    owner: string;
    fence: number;
    now: number;
  }): Promise<boolean>;
  markCreated(input: {
    workflowIdentity: string;
    instanceId: string;
    envelopeHash: string;
    owner: string;
    fence: number;
    now: number;
  }): Promise<boolean>;
  release(input: {
    workflowIdentity: string;
    instanceId: string;
    envelopeHash: string;
    owner: string;
    fence: number;
    now: number;
  }): Promise<boolean>;
  listCleanupCandidates(input: {
    checkBefore: number;
    limit: number;
  }): Promise<CloudflareWorkflowReceiptCleanupCandidate[]>;
  deleteCleanupCandidate(
    candidate: CloudflareWorkflowReceiptCleanupCandidate,
  ): Promise<boolean>;
  deferCleanupCandidate(input: {
    candidate: CloudflareWorkflowReceiptCleanupCandidate;
    now: number;
    checkAfter: number;
  }): Promise<boolean>;
}

/** Minimize successful-instance retention; errors remain inspectable longer. */
export const DEFAULT_CLOUDFLARE_WORKFLOW_RETENTION: CloudflareWorkflowRetention =
  Object.freeze({
    successRetention: "1 day",
    errorRetention: "3 days",
  });

export const DEFAULT_CLOUDFLARE_WORKFLOW_RECEIPT_LEASE_MS = 5 * 60 * 1_000;
export const DEFAULT_CLOUDFLARE_WORKFLOW_RECEIPT_CLEANUP_AFTER_MS =
  31 * 24 * 60 * 60 * 1_000;
/** @deprecated Use DEFAULT_CLOUDFLARE_WORKFLOW_RECEIPT_CLEANUP_AFTER_MS. */
export const DEFAULT_CLOUDFLARE_WORKFLOW_RECEIPT_RETENTION_MS =
  DEFAULT_CLOUDFLARE_WORKFLOW_RECEIPT_CLEANUP_AFTER_MS;
export const DEFAULT_CLOUDFLARE_WORKFLOW_RECEIPT_CLEANUP_BATCH_SIZE = 6;
/** @deprecated Use DEFAULT_CLOUDFLARE_WORKFLOW_RECEIPT_CLEANUP_BATCH_SIZE. */
export const DEFAULT_CLOUDFLARE_WORKFLOW_RECEIPT_CLEANUP_LIMIT =
  DEFAULT_CLOUDFLARE_WORKFLOW_RECEIPT_CLEANUP_BATCH_SIZE;
export const DEFAULT_CLOUDFLARE_WORKFLOW_RECEIPT_CLEANUP_MAX_BATCHES = 2;
export const DEFAULT_CLOUDFLARE_WORKFLOW_RECEIPT_CLEANUP_RECHECK_MS =
  6 * 60 * 60 * 1_000;
export const MAX_CLOUDFLARE_WORKFLOW_RECEIPT_CLEANUP_CANDIDATES = 12;
export const MAX_CLOUDFLARE_WORKFLOW_EVENTS_PER_REQUEST = 100;
export const RECOMMENDED_CLOUDFLARE_WORKFLOW_RECEIPT_CLEANUP_CRON =
  "*/5 * * * *";

export type CloudflareWorkflowBinding = {
  create(options: {
    id?: string;
    params?: unknown;
    retention: CloudflareWorkflowRetention;
  }): Promise<unknown>;
  createBatch?(batch: Array<{
    id: string;
    params?: unknown;
    retention: CloudflareWorkflowRetention;
  }>): Promise<unknown[]>;
  get?(id: string): Promise<{ status(): Promise<unknown> }>;
};

export interface CloudflareDispatchHandlerConfig<TEnv = unknown> {
  registry: WorkflowRegistry;
  auth?: {
    bearerToken?: string | ((env: TEnv) => string | undefined);
  };
  maxRequestBytes?: number | false;
  /**
   * Optional lower limit for one authenticated request. The hard Workflow RPC
   * and maintenance-admission ceiling remains 100 when this is false.
   */
  maxEventsPerRequest?: number | false;
  retention?: CloudflareWorkflowRetention;
  rateLimit?:
    | false
    | {
        max: number;
        windowMs: number;
      };
  resolveWorkflow(
    eventName: string,
    env: TEnv,
  ): CloudflareWorkflowBinding | null | undefined;
  /** Stable name for the concrete binding. Required when event names multiplex. */
  resolveWorkflowIdentity?(eventName: string, env: TEnv): string;
  resolveReceiptStore?(env: TEnv): CloudflareWorkflowReceiptStore;
  receiptLeaseMs?: number;
  /** Delay before the first status-backed cleanup check; this is not a TTL. */
  receiptCleanupAfterMs?: number;
  /** @deprecated Use receiptCleanupAfterMs. */
  receiptRetentionMs?: number;
  /** @deprecated Use receiptCleanupBatchSize. */
  receiptCleanupLimit?: number;
  receiptCleanupBatchSize?: number;
  receiptCleanupMaxBatches?: number;
  receiptCleanupRecheckMs?: number;
  receiptOwnerGenerator?: () => string;
  now?: () => Date;
}

export interface CloudflareWorkflowDispatchConfig<TEnv = unknown> {
  registry: WorkflowRegistry;
  resolveWorkflow(
    eventName: string,
    env: TEnv,
  ): CloudflareWorkflowBinding | null | undefined;
  resolveWorkflowIdentity?(eventName: string, env: TEnv): string;
  resolveReceiptStore?(env: TEnv): CloudflareWorkflowReceiptStore;
  receiptLeaseMs?: number;
  receiptCleanupAfterMs?: number;
  /** @deprecated Use receiptCleanupAfterMs. */
  receiptRetentionMs?: number;
  receiptOwnerGenerator?: () => string;
  now?: () => Date;
  idGenerator?: () => string;
  traceIdGenerator?: () => string;
  retention?: CloudflareWorkflowRetention;
}

export function createCloudflareWorkflowDispatch<TEnv = unknown>(
  config: CloudflareWorkflowDispatchConfig<TEnv>,
) {
  assertReceiptIdentityConfigured(config);
  const now = config.now ?? (() => new Date());
  const idGenerator = config.idGenerator ?? (() => createWorkflowId());
  const traceIdGenerator = config.traceIdGenerator ?? (() => createTraceId());
  const retention = config.retention ?? DEFAULT_CLOUDFLARE_WORKFLOW_RETENTION;
  const receiptSettings = resolveReceiptSettings(config);

  return async function dispatchCloudflareWorkflow<
    TPayload extends WorkflowPayload = WorkflowPayload,
  >(
    name: string,
    payload: TPayload,
    options: DispatchOptions | undefined,
    env: TEnv,
  ): Promise<DispatchResult> {
    const workflow = config.registry.get(name);
    const parsedPayload = config.registry.parsePayload(workflow, payload);

    const createdAt = resolveCreatedAt(now(), options);
    const scheduledAt = resolveScheduledAt(createdAt, options);
    const envelope: WorkflowEventEnvelope<string, WorkflowPayload> = {
      id: options?.id ?? idGenerator(),
      name,
      payload: parsedPayload,
      traceId: options?.traceId ?? traceIdGenerator(),
      idempotencyKey: options?.idempotencyKey ?? createWorkflowId("idem"),
      ...(scheduledAt === undefined ? {} : { scheduledAt }),
      createdAt: createdAt.toISOString(),
      ...(options?.metadata === undefined ? {} : { metadata: options.metadata }),
    };
    assertCloudflareWorkflowEnvelope(envelope);
    config.registry.validateEvent?.(workflow, envelope);

    const binding = config.resolveWorkflow(name, env);
    if (!binding) {
      throw new Error(`No Cloudflare Workflow binding for ${name}`);
    }

    const receiptStore = config.resolveReceiptStore?.(env);
    if (receiptStore) {
      await createCloudflareWorkflowInstanceWithReceipt({
        binding,
        envelope,
        workflowIdentity: resolveWorkflowIdentity(config, name, env),
        receiptStore,
        receiptSettings,
        now: now().getTime(),
      });
    } else {
      await createCloudflareWorkflowInstance(binding, envelope, retention);
    }

    const instance: WorkflowInstance = {
      id: envelope.id,
      name: envelope.name,
      status: envelope.scheduledAt ? "scheduled" : "queued",
      traceId: envelope.traceId,
      idempotencyKey: envelope.idempotencyKey,
      ...(envelope.scheduledAt === undefined
        ? {}
        : { scheduledAt: envelope.scheduledAt }),
      createdAt: envelope.createdAt,
      updatedAt: now().toISOString(),
    };

    return {
      ids: [envelope.id],
      envelopes: [envelope],
      instances: [instance],
    };
  };
}

export function createCloudflareDispatchHandler<TEnv = unknown>(
  config: CloudflareDispatchHandlerConfig<TEnv>,
) {
  assertReceiptIdentityConfigured(config);
  const now = config.now ?? (() => new Date());
  const rateLimiter = createRateLimiter(config.rateLimit);
  const retention = config.retention ?? DEFAULT_CLOUDFLARE_WORKFLOW_RETENTION;
  const receiptSettings = resolveReceiptSettings(config);
  const configuredMaxEventsPerRequest = config.maxEventsPerRequest;
  if (
    configuredMaxEventsPerRequest !== undefined &&
    configuredMaxEventsPerRequest !== false &&
    (!Number.isSafeInteger(configuredMaxEventsPerRequest) ||
      configuredMaxEventsPerRequest <= 0 ||
      configuredMaxEventsPerRequest >
        MAX_CLOUDFLARE_WORKFLOW_EVENTS_PER_REQUEST)
  ) {
    throw new Error(
      `maxEventsPerRequest must be a positive integer no greater than ${MAX_CLOUDFLARE_WORKFLOW_EVENTS_PER_REQUEST}, or false`,
    );
  }
  const maxEventsPerRequest =
    configuredMaxEventsPerRequest === undefined ||
    configuredMaxEventsPerRequest === false
      ? MAX_CLOUDFLARE_WORKFLOW_EVENTS_PER_REQUEST
      : configuredMaxEventsPerRequest;

  return {
    async fetch(request: Request, env: TEnv): Promise<Response> {
      const url = new URL(request.url);

      if (url.pathname === "/health") {
        return Response.json({ status: "ok" });
      }

      if (url.pathname.startsWith("/status/") && request.method === "GET") {
        const authError = verifyRequestAuth(request, env, config);
        if (authError) return authError;

        const id = url.pathname.slice("/status/".length);
        const name = url.searchParams.get("name");
        if (!name) {
          return Response.json(
            {
              error:
                "Workflow status lookup requires the workflow name query parameter",
            },
            { status: 400 },
          );
        }

        const target = await getStatusByName(id, name, env, config);
        if (!target) {
          return Response.json(
            { error: `No status binding for ${name}` },
            { status: 404 },
          );
        }

        return Response.json(
          toWorkflowInstance(id, target.workflowName, target.status),
        );
      }

      if (url.pathname === "/dispatch" && request.method === "POST") {
        const authError = verifyRequestAuth(request, env, config);
        if (authError) return authError;

        if (!rateLimiter()) {
          return Response.json(
            { error: "Rate limit exceeded" },
            { status: 429 },
          );
        }

        const parsed = await readJsonRequest<{
          events?: unknown[];
        }>(request, config.maxRequestBytes);
        if ("error" in parsed) return parsed.error;

        const body = parsed.body;

        if (!body?.events || !Array.isArray(body.events)) {
          return Response.json({ error: "Missing events array" }, { status: 400 });
        }
        if (body.events.length > maxEventsPerRequest) {
          return Response.json(
            {
              error: `Too many events; maximum is ${maxEventsPerRequest}`,
            },
            { status: 413 },
          );
        }

        const errors: Array<{ id: string; error: string }> = [];
        const prepared: Array<{
          envelope: WorkflowEventEnvelope;
          binding: CloudflareWorkflowBinding;
          workflowIdentity: string;
        }> = [];
        const seenByWorkflowIdentity = new Map<string, Map<string, string>>();

        for (const [index, candidate] of body.events.entries()) {
          try {
            const envelope = parseWorkflowEventEnvelope(candidate, index);

            const workflow = config.registry.get(envelope.name);
            const payload = config.registry.parsePayload(workflow, envelope.payload);

            const preparedEnvelope = { ...envelope, payload };
            assertCloudflareWorkflowEnvelope(preparedEnvelope);
            config.registry.validateEvent?.(workflow, preparedEnvelope);

            const binding = config.resolveWorkflow(envelope.name, env);
            if (!binding) {
              throw new Error(`No Cloudflare Workflow binding for ${envelope.name}`);
            }
            const workflowIdentity = resolveWorkflowIdentity(
              config,
              envelope.name,
              env,
            );
            const serialized = canonicalJson(preparedEnvelope);
            let seen = seenByWorkflowIdentity.get(workflowIdentity);
            if (!seen) {
              seen = new Map();
              seenByWorkflowIdentity.set(workflowIdentity, seen);
            }
            const previous = seen.get(preparedEnvelope.id);
            if (previous !== undefined) {
              if (previous !== serialized) {
                errors.push({
                  id: preparedEnvelope.id,
                  error:
                    "Conflicting duplicate Workflow instance id in one request",
                });
              }
              // Byte-identical retries collapse to one binding operation.
              continue;
            }
            seen.set(preparedEnvelope.id, serialized);
            prepared.push({
              envelope: preparedEnvelope,
              binding,
              workflowIdentity,
            });
          } catch (error) {
            errors.push({
              id: getCandidateEventId(candidate),
              error: boundedErrorMessage(error),
            });
          }
        }

        let receiptStore: CloudflareWorkflowReceiptStore | undefined;
        try {
          receiptStore = config.resolveReceiptStore?.(env);
          if (receiptStore && prepared.length > 0) {
            await cleanupCloudflareWorkflowReceipts({
              config,
              env,
              receiptStore,
              settings: receiptSettings,
              now: now().getTime(),
              candidateBudget: prepared.length,
            });
          }
        } catch (error) {
          return Response.json(
            {
              error: `Workflow receipt maintenance failed: ${boundedErrorMessage(error)}`,
            },
            { status: 503 },
          );
        }

        const accepted = await createCloudflareWorkflowInstances(
          prepared,
          errors,
          retention,
          receiptStore,
          receiptSettings,
          now().getTime(),
        );
        const instances = accepted.map((envelope) => ({
          id: envelope.id,
          name: envelope.name,
          status: envelope.scheduledAt ? ("scheduled" as const) : ("queued" as const),
          traceId: envelope.traceId,
          idempotencyKey: envelope.idempotencyKey,
          ...(envelope.scheduledAt === undefined
            ? {}
            : { scheduledAt: envelope.scheduledAt }),
          createdAt: envelope.createdAt,
          updatedAt: now().toISOString(),
        }));

        return Response.json({
          ids: instances.map((instance) => instance.id),
          instances,
          errors: errors.length ? errors : undefined,
        });
      }

      return new Response("Not Found", { status: 404 });
    },
    async scheduled(
      controller: { scheduledTime?: number } | unknown,
      env: TEnv,
    ): Promise<{ dispatched: number }> {
      let dispatched = 0;
      const scheduledTime =
        typeof (controller as { scheduledTime?: unknown } | null)?.scheduledTime === "number"
          ? new Date((controller as { scheduledTime: number }).scheduledTime)
          : now();
      const current = scheduledTime;
      const receiptStore = config.resolveReceiptStore?.(env);

      if (receiptStore) {
        await cleanupCloudflareWorkflowReceipts({
          config,
          env,
          receiptStore,
          settings: receiptSettings,
          now: current.getTime(),
        });
      }

      for (const workflow of config.registry.workflows) {
        for (const run of collectDueCronRuns(workflow, current)) {
          const binding = config.resolveWorkflow(run.workflowName, env);
          if (!binding) continue;
          const payload = config.registry.parsePayload(workflow, run.payload);

          const envelope: WorkflowEventEnvelope<string, WorkflowPayload> = {
            id: createCloudflareInstanceId(run.runKey),
            name: run.workflowName,
            payload,
            traceId: `trace_${createCloudflareInstanceId(run.runKey)}`,
            idempotencyKey: run.runKey,
            scheduledAt: run.scheduledAt.toISOString(),
            createdAt: current.toISOString(),
            ...(run.metadata === undefined ? {} : { metadata: run.metadata }),
          };
          assertCloudflareWorkflowEnvelope(envelope);
          config.registry.validateEvent?.(workflow, envelope);

          if (receiptStore) {
            const created = await createCloudflareWorkflowInstanceWithReceipt({
              binding,
              envelope,
              workflowIdentity: resolveWorkflowIdentity(
                config,
                run.workflowName,
                env,
              ),
              receiptStore,
              receiptSettings,
              now: current.getTime(),
            });
            if (created) dispatched++;
          } else {
            await createCloudflareWorkflowInstance(binding, envelope, retention);
            dispatched++;
          }
        }
      }

      return { dispatched };
    },
  };
}

type PreparedCloudflareWorkflowEvent = {
  envelope: WorkflowEventEnvelope;
  binding: CloudflareWorkflowBinding;
  workflowIdentity: string;
};

type ClaimedCloudflareWorkflowEvent = PreparedCloudflareWorkflowEvent & {
  envelopeHash: string;
  owner: string;
  fence: number;
};

type ReceiptSettings = {
  leaseMs: number;
  cleanupAfterMs: number;
  cleanupBatchSize: number;
  cleanupMaxBatches: number;
  cleanupRecheckMs: number;
  ownerGenerator: () => string;
  workflowRetention: CloudflareWorkflowRetention;
};

async function createCloudflareWorkflowInstances(
  events: PreparedCloudflareWorkflowEvent[],
  errors: Array<{ id: string; error: string }>,
  retention: CloudflareWorkflowRetention,
  receiptStore: CloudflareWorkflowReceiptStore | undefined,
  receiptSettings: ReceiptSettings,
  operationNow: number,
): Promise<WorkflowEventEnvelope[]> {
  const accepted: WorkflowEventEnvelope[] = [];
  const groups = new Map<
    CloudflareWorkflowBinding,
    PreparedCloudflareWorkflowEvent[]
  >();

  for (const event of events) {
    const group = groups.get(event.binding);
    if (group) {
      group.push(event);
    } else {
      groups.set(event.binding, [event]);
    }
  }

  for (const [binding, group] of groups) {
    for (const chunk of chunkPreparedEvents(group)) {
      accepted.push(
        ...(await createCloudflareWorkflowChunk(
          binding,
          chunk,
          errors,
          retention,
          receiptStore,
          receiptSettings,
          operationNow,
        )),
      );
    }
  }

  return accepted;
}

async function createCloudflareWorkflowChunk(
  binding: CloudflareWorkflowBinding,
  events: PreparedCloudflareWorkflowEvent[],
  errors: Array<{ id: string; error: string }>,
  retention: CloudflareWorkflowRetention,
  receiptStore: CloudflareWorkflowReceiptStore | undefined,
  receiptSettings: ReceiptSettings,
  operationNow: number,
): Promise<WorkflowEventEnvelope[]> {
  const envelopes = events.map((event) => event.envelope);
  if (envelopes.length === 0) return [];

  if (receiptStore) {
    return createCloudflareWorkflowChunkWithReceipts({
      binding,
      events,
      errors,
      receiptStore,
      receiptSettings,
      operationNow,
    });
  }

  if (binding.createBatch) {
    try {
      const created = await binding.createBatch(
        envelopes.map((envelope) => ({
          id: envelope.id,
          params: envelope,
          retention,
        })),
      );
      return matchCreatedBatchInstances(envelopes, created, errors);
    } catch (error) {
      // A failed binding batch remains one failed batch. Falling back to N
      // create() calls can amplify load and cannot safely distinguish a new
      // instance from an unverified cross-request id conflict.
      const message = boundedErrorMessage(error);
      for (const envelope of envelopes) {
        errors.push({ id: envelope.id, error: message });
      }
      return [];
    }
  }

  return createCloudflareWorkflowsIndividually(
    binding,
    envelopes,
    errors,
    retention,
  );
}

function matchCreatedBatchInstances(
  envelopes: WorkflowEventEnvelope[],
  created: unknown[],
  errors: Array<{ id: string; error: string }>,
): WorkflowEventEnvelope[] {
  const expectedIds = new Set(envelopes.map((envelope) => envelope.id));
  const confirmedIds = new Set<string>();
  let invalidResponse = false;

  for (const instance of created) {
    const id =
      typeof instance === "object" &&
      instance !== null &&
      typeof (instance as { id?: unknown }).id === "string"
        ? (instance as { id: string }).id
        : null;
    if (!id || !expectedIds.has(id) || confirmedIds.has(id)) {
      invalidResponse = true;
      break;
    }
    confirmedIds.add(id);
  }

  if (invalidResponse) {
    for (const envelope of envelopes) {
      errors.push({
        id: envelope.id,
        error:
          "Cloudflare Workflow createBatch returned an invalid instance list; creation was not verified",
      });
    }
    return [];
  }

  const accepted: WorkflowEventEnvelope[] = [];
  for (const envelope of envelopes) {
    if (confirmedIds.has(envelope.id)) {
      accepted.push(envelope);
      continue;
    }
    errors.push({
      id: envelope.id,
      error:
        "Cloudflare Workflow createBatch did not confirm this instance; refusing an unverified existing id",
    });
  }
  return accepted;
}

async function createCloudflareWorkflowChunkWithReceipts(options: {
  binding: CloudflareWorkflowBinding;
  events: PreparedCloudflareWorkflowEvent[];
  errors: Array<{ id: string; error: string }>;
  receiptStore: CloudflareWorkflowReceiptStore;
  receiptSettings: ReceiptSettings;
  operationNow: number;
}): Promise<WorkflowEventEnvelope[]> {
  const accepted: WorkflowEventEnvelope[] = [];
  const claimed: ClaimedCloudflareWorkflowEvent[] = [];

  const preparations = await mapWithConcurrency(
    options.events,
    8,
    async (event) => {
      try {
        return {
          event,
          prepared: await claimWorkflowReceipt(
            event,
            options.receiptStore,
            options.receiptSettings,
            options.operationNow,
          ),
        };
      } catch (error) {
        return { event, error };
      }
    },
  );
  for (const preparation of preparations) {
    if ("error" in preparation) {
      options.errors.push({
        id: preparation.event.envelope.id,
        error: boundedErrorMessage(preparation.error),
      });
    } else if (preparation.prepared.outcome === "accepted") {
      accepted.push(preparation.event.envelope);
    } else {
      claimed.push(preparation.prepared.event);
    }
  }

  if (claimed.length === 0) return accepted;

  if (!options.binding.createBatch) {
    for (const event of claimed) {
      const result = await createClaimedWorkflowInstance(
        options.binding,
        event,
        options.receiptStore,
        options.receiptSettings.workflowRetention,
        options.operationNow,
      ).catch((error) => ({ accepted: false as const, error }));
      if (result.accepted) {
        accepted.push(event.envelope);
      } else {
        options.errors.push({
          id: event.envelope.id,
          error: boundedErrorMessage(result.error),
        });
      }
    }
    return accepted;
  }

  let created: unknown[];
  try {
    created = await options.binding.createBatch(
      claimed.map((event) => ({
        id: event.envelope.id,
        params: event.envelope,
        retention: options.receiptSettings.workflowRetention,
      })),
    );
  } catch (error) {
    const resolved = await resolveAmbiguousClaimedCreates(
      options.binding,
      claimed,
      options.receiptStore,
      options.operationNow,
      error,
    );
    accepted.push(...resolved.accepted);
    options.errors.push(...resolved.errors);
    return accepted;
  }

  const confirmedIds = parseCreatedBatchIds(
    created,
    new Set(claimed.map((event) => event.envelope.id)),
  );
  if (confirmedIds === null) {
    const error = new Error(
      "Cloudflare Workflow createBatch returned an invalid instance list; creation requires status verification",
    );
    const resolved = await resolveAmbiguousClaimedCreates(
      options.binding,
      claimed,
      options.receiptStore,
      options.operationNow,
      error,
    );
    accepted.push(...resolved.accepted);
    options.errors.push(...resolved.errors);
    return accepted;
  }

  const omitted: ClaimedCloudflareWorkflowEvent[] = [];
  for (const event of claimed) {
    if (confirmedIds.has(event.envelope.id)) {
      try {
        await markWorkflowReceiptCreated(
          options.receiptStore,
          event,
          options.operationNow,
        );
        accepted.push(event.envelope);
      } catch (error) {
        options.errors.push({
          id: event.envelope.id,
          error: boundedErrorMessage(error),
        });
      }
      continue;
    }

    omitted.push(event);
  }
  if (omitted.length > 0) {
    const resolved = await resolveAmbiguousClaimedCreates(
      options.binding,
      omitted,
      options.receiptStore,
      options.operationNow,
      new Error(
        "Cloudflare Workflow createBatch omitted this instance; creation requires status verification",
      ),
    );
    accepted.push(...resolved.accepted);
    options.errors.push(...resolved.errors);
  }

  return accepted;
}

async function claimWorkflowReceipt(
  event: PreparedCloudflareWorkflowEvent,
  receiptStore: CloudflareWorkflowReceiptStore,
  settings: ReceiptSettings,
  operationNow: number,
): Promise<
  | { outcome: "accepted" }
  | { outcome: "claimed"; event: ClaimedCloudflareWorkflowEvent }
> {
  const envelopeHash = await hashCanonicalEnvelope(event.envelope);
  const owner = settings.ownerGenerator();
  if (owner.length === 0 || owner.length > 256) {
    throw new Error("Workflow receipt owner must contain 1-256 characters");
  }
  const claim = await receiptStore.claim({
    workflowIdentity: event.workflowIdentity,
    workflowName: event.envelope.name,
    instanceId: event.envelope.id,
    envelopeHash,
    owner,
    now: operationNow,
    leaseExpiresAt: operationNow + settings.leaseMs,
    checkAfter: operationNow + settings.cleanupAfterMs,
  });

  if (claim.outcome === "created") return { outcome: "accepted" };
  if (claim.outcome === "conflict") {
    throw nonRetryableReceiptError(
      `Cloudflare Workflow receipt conflict for ${event.envelope.id}: the instance id is reserved for a different canonical envelope`,
    );
  }
  if (claim.outcome === "busy") {
    throw new Error(
      `Cloudflare Workflow receipt for ${event.envelope.id} is being created by another request; retry after ${claim.retryAfter}`,
    );
  }

  const claimedEvent: ClaimedCloudflareWorkflowEvent = {
    ...event,
    envelopeHash,
    owner,
    fence: claim.fence,
  };
  const existence = await inspectCloudflareWorkflowInstance(
    event.binding,
    event.envelope.id,
  );
  if (claim.previousState === "absence_proven") {
    if (existence === "absent") {
      return { outcome: "claimed", event: claimedEvent };
    }
    await markWorkflowReceiptCreated(receiptStore, claimedEvent, operationNow);
    return { outcome: "accepted" };
  }

  if (existence === "exists") {
    await releaseWorkflowReceipt(receiptStore, claimedEvent, operationNow);
    throw nonRetryableReceiptError(
      `Cloudflare Workflow receipt conflict for ${event.envelope.id}: an existing instance has no matching durable absence proof`,
    );
  }

  const absenceProof = await markWorkflowReceiptAbsenceProven(
    receiptStore,
    claimedEvent,
    operationNow,
  );
  if (absenceProof === "created") return { outcome: "accepted" };
  return { outcome: "claimed", event: claimedEvent };
}

async function createClaimedWorkflowInstance(
  binding: CloudflareWorkflowBinding,
  event: ClaimedCloudflareWorkflowEvent,
  receiptStore: CloudflareWorkflowReceiptStore,
  retention: CloudflareWorkflowRetention,
  operationNow: number,
): Promise<{ accepted: true }> {
  try {
    await createCloudflareWorkflowInstance(binding, event.envelope, retention);
  } catch (error) {
    const existence = await inspectCloudflareWorkflowInstance(
      binding,
      event.envelope.id,
    );
    if (existence === "exists") {
      await markWorkflowReceiptCreated(receiptStore, event, operationNow);
      return { accepted: true };
    }
    await releaseWorkflowReceipt(receiptStore, event, operationNow);
    throw error;
  }

  await markWorkflowReceiptCreated(receiptStore, event, operationNow);
  return { accepted: true };
}

async function resolveAmbiguousClaimedCreate(
  binding: CloudflareWorkflowBinding,
  event: ClaimedCloudflareWorkflowEvent,
  receiptStore: CloudflareWorkflowReceiptStore,
  operationNow: number,
  createError: unknown,
  accepted: WorkflowEventEnvelope[],
  errors: Array<{ id: string; error: string }>,
): Promise<void> {
  try {
    const existence = await inspectCloudflareWorkflowInstance(
      binding,
      event.envelope.id,
    );
    if (existence === "exists") {
      await markWorkflowReceiptCreated(receiptStore, event, operationNow);
      accepted.push(event.envelope);
      return;
    }
    await releaseWorkflowReceipt(receiptStore, event, operationNow);
    errors.push({
      id: event.envelope.id,
      error: boundedErrorMessage(createError),
    });
  } catch (statusError) {
    errors.push({
      id: event.envelope.id,
      error: boundedErrorMessage(
        new Error(
          `${boundedErrorMessage(createError)}; status verification failed: ${boundedErrorMessage(statusError)}`,
        ),
      ),
    });
  }
}

async function resolveAmbiguousClaimedCreates(
  binding: CloudflareWorkflowBinding,
  events: ClaimedCloudflareWorkflowEvent[],
  receiptStore: CloudflareWorkflowReceiptStore,
  operationNow: number,
  createError: unknown,
): Promise<{
  accepted: WorkflowEventEnvelope[];
  errors: Array<{ id: string; error: string }>;
}> {
  const outcomes = await mapWithConcurrency(events, 8, async (event) => {
    const accepted: WorkflowEventEnvelope[] = [];
    const errors: Array<{ id: string; error: string }> = [];
    await resolveAmbiguousClaimedCreate(
      binding,
      event,
      receiptStore,
      operationNow,
      createError,
      accepted,
      errors,
    );
    return { accepted, errors };
  });
  return {
    accepted: outcomes.flatMap((outcome) => outcome.accepted),
    errors: outcomes.flatMap((outcome) => outcome.errors),
  };
}

async function cleanupCloudflareWorkflowReceipts<TEnv>(options: {
  config: CloudflareDispatchHandlerConfig<TEnv>;
  env: TEnv;
  receiptStore: CloudflareWorkflowReceiptStore;
  settings: ReceiptSettings;
  now: number;
  /** HTTP admission supplies one bounded maintenance attempt per unique event. */
  candidateBudget?: number;
}): Promise<void> {
  if (
    options.candidateBudget !== undefined &&
    (!Number.isSafeInteger(options.candidateBudget) ||
      options.candidateBudget <= 0 ||
      options.candidateBudget >
        MAX_CLOUDFLARE_WORKFLOW_EVENTS_PER_REQUEST)
  ) {
    throw new Error(
      `Workflow receipt cleanup admission budget must be between 1 and ${MAX_CLOUDFLARE_WORKFLOW_EVENTS_PER_REQUEST}`,
    );
  }

  const batchSize =
    options.candidateBudget ?? options.settings.cleanupBatchSize;
  const maxBatches =
    options.candidateBudget === undefined
      ? options.settings.cleanupMaxBatches
      : 1;
  const maxCandidates = options.candidateBudget ?? batchSize * maxBatches;
  const seen = new Set<string>();
  let inspectedCandidates = 0;

  for (
    let batchIndex = 0;
    batchIndex < maxBatches && inspectedCandidates < maxCandidates;
    batchIndex += 1
  ) {
    const limit = Math.min(batchSize, maxCandidates - inspectedCandidates);
    const candidates = await options.receiptStore.listCleanupCandidates({
      checkBefore: options.now,
      limit,
    });
    if (candidates.length > limit) {
      throw new Error(
        "Workflow receipt cleanup store exceeded the requested candidate limit",
      );
    }
    if (candidates.length === 0) return;
    inspectedCandidates += candidates.length;

    let repeatedSnapshot = false;
    const freshCandidates = candidates.filter((candidate) => {
      const key = [
        candidate.workflowIdentity,
        candidate.instanceId,
        candidate.envelopeHash,
        candidate.fence,
        candidate.checkAfter,
      ].join("\u0000");
      if (seen.has(key)) {
        repeatedSnapshot = true;
        return false;
      }
      seen.add(key);
      return true;
    });

    await mapWithConcurrency(freshCandidates, 2, async (candidate) => {
      let absenceProven = false;
      try {
        const binding = options.config.resolveWorkflow(
          candidate.workflowName,
          options.env,
        );
        const identity = resolveWorkflowIdentity(
          options.config,
          candidate.workflowName,
          options.env,
        );
        if (binding && identity === candidate.workflowIdentity) {
          absenceProven =
            (await inspectCloudflareWorkflowInstance(
              binding,
              candidate.instanceId,
            )) === "absent";
        }
      } catch {
        // A missing binding, identity drift, or transient status error cannot
        // prove absence. Defer the exact fenced snapshot for a later check.
      }

      if (absenceProven) {
        await options.receiptStore.deleteCleanupCandidate(candidate);
        return;
      }
      await options.receiptStore.deferCleanupCandidate({
        candidate,
        now: options.now,
        checkAfter: options.now + options.settings.cleanupRecheckMs,
      });
    });

    if (repeatedSnapshot || candidates.length < limit) {
      return;
    }
  }
}

function parseCreatedBatchIds(
  created: unknown[],
  expectedIds: Set<string>,
): Set<string> | null {
  if (!Array.isArray(created)) return null;
  const confirmedIds = new Set<string>();
  for (const instance of created) {
    const id =
      typeof instance === "object" &&
      instance !== null &&
      typeof (instance as { id?: unknown }).id === "string"
        ? (instance as { id: string }).id
        : null;
    if (!id || !expectedIds.has(id) || confirmedIds.has(id)) return null;
    confirmedIds.add(id);
  }
  return confirmedIds;
}

async function inspectCloudflareWorkflowInstance(
  binding: CloudflareWorkflowBinding,
  instanceId: string,
): Promise<"exists" | "absent"> {
  if (!binding.get) {
    throw new Error(
      "Cloudflare Workflow receipt recovery requires an explicitly named binding with get()",
    );
  }

  try {
    const instance = await binding.get(instanceId);
    await instance.status();
    // A successful lookup/status call proves the instance exists even when
    // Cloudflare adds a status that this SDK does not recognize yet. Only an
    // explicit missing-instance error below may prove absence.
    return "exists";
  } catch (error) {
    if (isMissingCloudflareInstanceError(error)) return "absent";
    throw error;
  }
}

async function markWorkflowReceiptAbsenceProven(
  receiptStore: CloudflareWorkflowReceiptStore,
  event: ClaimedCloudflareWorkflowEvent,
  operationNow: number,
): Promise<"absence_proven" | "created"> {
  const marked = await receiptStore.markAbsenceProven({
    workflowIdentity: event.workflowIdentity,
    instanceId: event.envelope.id,
    envelopeHash: event.envelopeHash,
    owner: event.owner,
    fence: event.fence,
    now: operationNow,
  });
  if (marked) return "absence_proven";

  const receipt = await receiptStore.get({
    workflowIdentity: event.workflowIdentity,
    instanceId: event.envelope.id,
  });
  if (
    receipt?.state === "CREATED" &&
    receipt.envelopeHash === event.envelopeHash
  ) {
    return "created";
  }
  throw new Error(
    `Cloudflare Workflow receipt fence was lost for ${event.envelope.id}; absence proof was not persisted`,
  );
}

async function markWorkflowReceiptCreated(
  receiptStore: CloudflareWorkflowReceiptStore,
  event: ClaimedCloudflareWorkflowEvent,
  operationNow: number,
): Promise<void> {
  const marked = await receiptStore.markCreated({
    workflowIdentity: event.workflowIdentity,
    instanceId: event.envelope.id,
    envelopeHash: event.envelopeHash,
    owner: event.owner,
    fence: event.fence,
    now: operationNow,
  });
  if (marked) return;

  const receipt = await receiptStore.get({
    workflowIdentity: event.workflowIdentity,
    instanceId: event.envelope.id,
  });
  if (
    receipt?.state === "CREATED" &&
    receipt.envelopeHash === event.envelopeHash
  ) {
    return;
  }
  throw new Error(
    `Cloudflare Workflow receipt fence was lost for ${event.envelope.id}; creation remains pending verification`,
  );
}

async function releaseWorkflowReceipt(
  receiptStore: CloudflareWorkflowReceiptStore,
  event: ClaimedCloudflareWorkflowEvent,
  operationNow: number,
): Promise<void> {
  const released = await receiptStore.release({
    workflowIdentity: event.workflowIdentity,
    instanceId: event.envelope.id,
    envelopeHash: event.envelopeHash,
    owner: event.owner,
    fence: event.fence,
    now: operationNow,
  });
  if (!released) {
    throw new Error(
      `Cloudflare Workflow receipt fence was lost for ${event.envelope.id}; retry before creating again`,
    );
  }
}

function nonRetryableReceiptError(message: string): Error {
  const error = new Error(message);
  (error as Error & { nonRetryable?: boolean }).nonRetryable = true;
  return error;
}

async function createCloudflareWorkflowsIndividually(
  binding: CloudflareWorkflowBinding,
  envelopes: WorkflowEventEnvelope[],
  errors: Array<{ id: string; error: string }>,
  retention: CloudflareWorkflowRetention,
): Promise<WorkflowEventEnvelope[]> {
  const accepted: WorkflowEventEnvelope[] = [];
  for (const envelope of envelopes) {
    try {
      await createCloudflareWorkflowInstance(binding, envelope, retention);
      accepted.push(envelope);
    } catch (error) {
      errors.push({
        id: envelope.id,
        error: boundedErrorMessage(error),
      });
    }
  }

  return accepted;
}

function chunkPreparedEvents(
  events: PreparedCloudflareWorkflowEvent[],
): PreparedCloudflareWorkflowEvent[][] {
  const chunks: PreparedCloudflareWorkflowEvent[][] = [];
  let current: PreparedCloudflareWorkflowEvent[] = [];
  let currentBytes = 0;

  for (const event of events) {
    const envelope = event.envelope;
    const envelopeBytes = new TextEncoder().encode(JSON.stringify(envelope)).byteLength;
    if (
      current.length > 0 &&
      (current.length >= 100 || currentBytes + envelopeBytes > 900_000)
    ) {
      chunks.push(current);
      current = [];
      currentBytes = 0;
    }
    current.push(event);
    currentBytes += envelopeBytes;
  }
  if (current.length > 0) chunks.push(current);
  return chunks;
}

async function mapWithConcurrency<T, TResult>(
  values: T[],
  concurrency: number,
  operation: (value: T) => Promise<TResult>,
): Promise<TResult[]> {
  const results = new Array<TResult>(values.length);
  let next = 0;
  const workers = Array.from(
    { length: Math.min(concurrency, values.length) },
    async () => {
      while (true) {
        const index = next++;
        if (index >= values.length) return;
        results[index] = await operation(values[index]!);
      }
    },
  );
  await Promise.all(workers);
  return results;
}

function verifyRequestSize(
  request: Request,
  maxRequestBytes: number | false | undefined,
): Response | null {
  if (maxRequestBytes === false) return null;

  const maxBytes = maxRequestBytes ?? 1_048_576;
  const contentLength = request.headers.get("content-length");
  if (contentLength && Number(contentLength) > maxBytes) {
    return Response.json({ error: "Payload too large" }, { status: 413 });
  }

  return null;
}

async function readJsonRequest<T>(
  request: Request,
  maxRequestBytes: number | false | undefined,
): Promise<{ body: T | null } | { error: Response }> {
  if (maxRequestBytes === false) {
    return { body: (await request.json().catch(() => null)) as T | null };
  }

  const sizeError = verifyRequestSize(request, maxRequestBytes);
  if (sizeError) return { error: sizeError };

  const maxBytes = maxRequestBytes ?? 1_048_576;
  if (!request.body) return { body: null };

  const reader = request.body.getReader();
  const decoder = new TextDecoder();
  let total = 0;
  let text = "";

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > maxBytes) {
        await reader.cancel().catch(() => undefined);
        return { error: Response.json({ error: "Payload too large" }, { status: 413 }) };
      }
      text += decoder.decode(value, { stream: true });
    }
    text += decoder.decode();
  } catch {
    return { body: null };
  }

  if (!text) return { body: null };

  try {
    return { body: JSON.parse(text) as T };
  } catch {
    return { body: null };
  }
}

function parseWorkflowEventEnvelope(
  value: unknown,
  index: number,
): WorkflowEventEnvelope {
  try {
    assertCloudflareWorkflowEnvelope(value);
    return value;
  } catch (error) {
    throw new Error(
      `Invalid event structure at index ${index}: ${boundedErrorMessage(error)}`,
    );
  }
}

function getCandidateEventId(value: unknown): string {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return "unknown";
  }

  const id = (value as { id?: unknown }).id;
  return typeof id === "string" && id.length > 0 ? id : "unknown";
}

function createRateLimiter(
  rateLimit: CloudflareDispatchHandlerConfig["rateLimit"],
): () => boolean {
  if (!rateLimit) return () => true;

  let count = 0;
  let windowStart = Date.now();

  return () => {
    const now = Date.now();
    if (now - windowStart > rateLimit.windowMs) {
      count = 0;
      windowStart = now;
    }

    count += 1;
    return count <= rateLimit.max;
  };
}

function createCloudflareInstanceId(runKey: string): string {
  const sanitized = runKey.replace(/[^a-zA-Z0-9_-]/g, "_");
  const hash = hashString(runKey);
  return `cron_${sanitized.slice(0, 86)}_${hash}`.slice(0, 100);
}

function resolveScheduledAt(
  now: Date,
  options: DispatchOptions | undefined,
): string | undefined {
  if (options?.scheduledAt instanceof Date) {
    return options.scheduledAt.toISOString();
  }

  if (typeof options?.scheduledAt === "string") {
    return new Date(options.scheduledAt).toISOString();
  }

  if (typeof options?.delayMs === "number" && options.delayMs > 0) {
    return new Date(now.getTime() + options.delayMs).toISOString();
  }

  return undefined;
}

function resolveCreatedAt(
  now: Date,
  options: DispatchOptions | undefined,
): Date {
  if (options?.createdAt instanceof Date) return options.createdAt;
  if (typeof options?.createdAt === "string") {
    return new Date(options.createdAt);
  }
  return now;
}

async function getStatusByName<TEnv>(
  id: string,
  name: string,
  env: TEnv,
  config: CloudflareDispatchHandlerConfig<TEnv>,
): Promise<{ workflowName: string; status: unknown } | null> {
  const binding = config.resolveWorkflow(name, env);
  if (!binding?.get) return null;

  try {
    const instance = await binding.get(id);
    return { workflowName: name, status: await instance.status() };
  } catch (error) {
    if (isMissingCloudflareInstanceError(error)) return null;
    throw error;
  }
}

function toWorkflowInstance(
  id: string,
  name: string,
  status: unknown,
): WorkflowInstance {
  const statusObject =
    typeof status === "object" && status !== null
      ? (status as { status?: string; output?: unknown; error?: unknown })
      : null;
  const error = statusObject?.error;

  return {
    id,
    name,
    status:
      typeof status === "string"
        ? normalizeCloudflareStatus(status)
        : normalizeCloudflareStatus(statusObject?.status),
    output: statusObject ? statusObject.output : undefined,
    error: error ? normalizeCloudflareError(error) : undefined,
  };
}

function normalizeCloudflareStatus(status: unknown): WorkflowInstance["status"] {
  switch (String(status ?? "").toLowerCase()) {
    case "complete":
    case "completed":
    case "success":
      return "complete";
    case "errored":
      return "errored";
    case "failed":
      return "failed";
    case "terminated":
      return "terminated";
    case "canceled":
    case "cancelled":
      return "cancelled";
    case "running":
      return "running";
    case "waiting":
      return "waiting";
    case "waitingforpause":
    case "waiting for pause":
    case "waiting_for_pause":
      return "waitingForPause";
    case "paused":
      return "paused";
    case "queued":
      return "queued";
    case "unknown":
      return "unknown";
    default:
      return "unknown";
  }
}

function normalizeCloudflareError(error: unknown): { name: string; message: string } {
  if (error instanceof Error) {
    return { name: error.name, message: boundedErrorMessage(error) };
  }

  if (typeof error === "object" && error !== null) {
    const value = error as { name?: unknown; message?: unknown };
    return {
      name: typeof value.name === "string" ? value.name : "WorkflowError",
      message: boundedErrorMessage(
        typeof value.message === "string" ? value.message : JSON.stringify(error),
      ),
    };
  }

  return { name: "WorkflowError", message: boundedErrorMessage(error) };
}

async function createCloudflareWorkflowInstance(
  binding: CloudflareWorkflowBinding,
  envelope: WorkflowEventEnvelope,
  retention: CloudflareWorkflowRetention = DEFAULT_CLOUDFLARE_WORKFLOW_RETENTION,
): Promise<void> {
  await binding.create({
    id: envelope.id,
    params: envelope,
    retention,
  });
}

async function createCloudflareWorkflowInstanceWithReceipt(options: {
  binding: CloudflareWorkflowBinding;
  envelope: WorkflowEventEnvelope;
  workflowIdentity: string;
  receiptStore: CloudflareWorkflowReceiptStore;
  receiptSettings: ReceiptSettings;
  now: number;
}): Promise<boolean> {
  const prepared = await claimWorkflowReceipt(
    {
      binding: options.binding,
      envelope: options.envelope,
      workflowIdentity: options.workflowIdentity,
    },
    options.receiptStore,
    options.receiptSettings,
    options.now,
  );
  if (prepared.outcome === "accepted") return false;

  await createClaimedWorkflowInstance(
    options.binding,
    prepared.event,
    options.receiptStore,
    options.receiptSettings.workflowRetention,
    options.now,
  );
  return true;
}

function resolveReceiptSettings(config: {
  retention?: CloudflareWorkflowRetention;
  receiptLeaseMs?: number;
  receiptCleanupAfterMs?: number;
  receiptRetentionMs?: number;
  receiptCleanupLimit?: number;
  receiptCleanupBatchSize?: number;
  receiptCleanupMaxBatches?: number;
  receiptCleanupRecheckMs?: number;
  receiptOwnerGenerator?: () => string;
}): ReceiptSettings {
  const leaseMs =
    config.receiptLeaseMs ?? DEFAULT_CLOUDFLARE_WORKFLOW_RECEIPT_LEASE_MS;
  const cleanupAfterMs =
    config.receiptCleanupAfterMs ??
    config.receiptRetentionMs ??
    DEFAULT_CLOUDFLARE_WORKFLOW_RECEIPT_CLEANUP_AFTER_MS;
  const cleanupBatchSize =
    config.receiptCleanupBatchSize ??
    config.receiptCleanupLimit ??
    DEFAULT_CLOUDFLARE_WORKFLOW_RECEIPT_CLEANUP_BATCH_SIZE;
  const cleanupMaxBatches =
    config.receiptCleanupMaxBatches ??
    DEFAULT_CLOUDFLARE_WORKFLOW_RECEIPT_CLEANUP_MAX_BATCHES;
  const cleanupRecheckMs =
    config.receiptCleanupRecheckMs ??
    DEFAULT_CLOUDFLARE_WORKFLOW_RECEIPT_CLEANUP_RECHECK_MS;
  if (!Number.isSafeInteger(leaseMs) || leaseMs <= 0) {
    throw new Error("receiptLeaseMs must be a positive safe integer");
  }
  if (
    !Number.isSafeInteger(cleanupAfterMs) ||
    cleanupAfterMs < leaseMs
  ) {
    throw new Error(
      "receiptCleanupAfterMs must be a safe integer at least as large as receiptLeaseMs",
    );
  }
  if (
    !Number.isSafeInteger(cleanupBatchSize) ||
    cleanupBatchSize <= 0
  ) {
    throw new Error("receiptCleanupBatchSize must be a positive safe integer");
  }
  if (
    !Number.isSafeInteger(cleanupMaxBatches) ||
    cleanupMaxBatches <= 0
  ) {
    throw new Error("receiptCleanupMaxBatches must be a positive safe integer");
  }
  if (
    cleanupBatchSize * cleanupMaxBatches >
    MAX_CLOUDFLARE_WORKFLOW_RECEIPT_CLEANUP_CANDIDATES
  ) {
    throw new Error(
      `Workflow receipt cleanup is limited to ${MAX_CLOUDFLARE_WORKFLOW_RECEIPT_CLEANUP_CANDIDATES} candidates per invocation`,
    );
  }
  if (!Number.isSafeInteger(cleanupRecheckMs) || cleanupRecheckMs <= 0) {
    throw new Error("receiptCleanupRecheckMs must be a positive safe integer");
  }

  return {
    leaseMs,
    cleanupAfterMs,
    cleanupBatchSize,
    cleanupMaxBatches,
    cleanupRecheckMs,
    ownerGenerator:
      config.receiptOwnerGenerator ?? (() => crypto.randomUUID()),
    workflowRetention:
      config.retention ?? DEFAULT_CLOUDFLARE_WORKFLOW_RETENTION,
  };
}

function resolveWorkflowIdentity<TEnv>(
  config: {
    resolveWorkflowIdentity?(eventName: string, env: TEnv): string;
  },
  eventName: string,
  env: TEnv,
): string {
  const identity = config.resolveWorkflowIdentity?.(eventName, env) ?? eventName;
  if (identity.length === 0 || identity.length > 256) {
    throw new Error(
      "Cloudflare Workflow binding identity must contain 1-256 characters",
    );
  }
  return identity;
}

function assertReceiptIdentityConfigured(config: {
  resolveReceiptStore?: unknown;
  resolveWorkflowIdentity?: unknown;
}): void {
  if (config.resolveReceiptStore && !config.resolveWorkflowIdentity) {
    throw new Error(
      "resolveWorkflowIdentity is required when durable Workflow receipts are enabled",
    );
  }
}

async function hashCanonicalEnvelope(
  envelope: WorkflowEventEnvelope,
): Promise<string> {
  const bytes = new TextEncoder().encode(canonicalJson(envelope));
  const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));
  return [...digest]
    .map((value) => value.toString(16).padStart(2, "0"))
    .join("");
}

function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map((item) => canonicalJson(item)).join(",")}]`;
  }
  const record = value as Record<string, unknown>;
  return `{${Object.keys(record)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${canonicalJson(record[key])}`)
    .join(",")}}`;
}

function isMissingCloudflareInstanceError(error: unknown): boolean {
  if (typeof error === "object" && error !== null) {
    const candidate = error as { code?: unknown; errorCode?: unknown };
    for (const code of [candidate.code, candidate.errorCode]) {
      if (
        code === 10400 ||
        code === "10400" ||
        code === "instance.not_found" ||
        code === "workflows.api.error.instance.not_found"
      ) {
        return true;
      }
    }
  }

  const rawMessage =
    error instanceof Error
      ? error.message
      : typeof error === "object" &&
          error !== null &&
          typeof (error as { message?: unknown }).message === "string"
        ? (error as { message: string }).message
        : String(error);
  const message = rawMessage
    .trim()
    .replace(/^(?:(?:Error|WorkflowError):\s*)+/i, "")
    .trim();
  return (
    /^(?:\(?(?:workflows\.api\.error\.)?instance\.not_found\)?)(?:\s*[:\-]?\s*instance does not exist)?$/i.test(
      message,
    ) ||
    /^(?:workflow\s+)?instance does not exist$/i.test(message) ||
    /^(?:workflow\s+)?instance\s+(?:was\s+)?not found$/i.test(message) ||
    /^no such (?:workflow\s+)?instance$/i.test(message)
  );
}

function boundedErrorMessage(error: unknown): string {
  const raw = error instanceof Error ? error.message : String(error);
  const sanitized = raw
    .replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  return (sanitized || "Cloudflare Workflow operation failed").slice(0, 512);
}

function hashString(value: string): string {
  let hash = 5381;
  for (let index = 0; index < value.length; index++) {
    hash = (hash * 33) ^ value.charCodeAt(index);
  }
  return (hash >>> 0).toString(36);
}

function verifyRequestAuth<TEnv>(
  request: Request,
  env: TEnv,
  config: CloudflareDispatchHandlerConfig<TEnv>,
): Response | null {
  const bearerToken = config.auth?.bearerToken;
  if (bearerToken === undefined) return null;

  const expected =
    typeof bearerToken === "function" ? bearerToken(env) : bearerToken;

  if (!expected) {
    return new Response("Workflow dispatcher auth is not configured", {
      status: 500,
    });
  }

  const header = request.headers.get("Authorization");
  if (!header?.startsWith("Bearer ")) {
    return new Response("Unauthorized", { status: 401 });
  }

  return timingSafeEqual(header.slice(7), expected)
    ? null
    : new Response("Unauthorized", { status: 401 });
}

function timingSafeEqual(a: string, b: string): boolean {
  const encoder = new TextEncoder();
  const left = encoder.encode(a);
  const right = encoder.encode(b);
  if (left.byteLength !== right.byteLength) return false;

  let result = 0;
  for (let index = 0; index < left.byteLength; index++) {
    result |= left[index]! ^ right[index]!;
  }

  return result === 0;
}
