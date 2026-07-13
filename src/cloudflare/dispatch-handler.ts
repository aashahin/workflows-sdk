import { collectDueCronRuns } from "../scheduler/cron";
import { createTraceId, createWorkflowId } from "../core/id";
import { WorkflowValidationError } from "../core/errors";
import type { WorkflowRegistry } from "../core/registry";
import type {
  DispatchOptions,
  DispatchResult,
  WorkflowEventEnvelope,
  WorkflowInstance,
  WorkflowPayload,
} from "../core/types";

export type CloudflareWorkflowBinding = {
  create(options: {
    id?: string;
    params?: unknown;
  }): Promise<unknown>;
  createBatch?(batch: Array<{
    id: string;
    params?: unknown;
  }>): Promise<unknown[]>;
  get?(id: string): Promise<{ status(): Promise<unknown> }>;
};

export interface CloudflareDispatchHandlerConfig<TEnv = unknown> {
  registry: WorkflowRegistry;
  auth?: {
    bearerToken?: string | ((env: TEnv) => string | undefined);
  };
  maxRequestBytes?: number | false;
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
  now?: () => Date;
}

export interface CloudflareWorkflowDispatchConfig<TEnv = unknown> {
  registry: WorkflowRegistry;
  resolveWorkflow(
    eventName: string,
    env: TEnv,
  ): CloudflareWorkflowBinding | null | undefined;
  now?: () => Date;
  idGenerator?: () => string;
  traceIdGenerator?: () => string;
}

export function createCloudflareWorkflowDispatch<TEnv = unknown>(
  config: CloudflareWorkflowDispatchConfig<TEnv>,
) {
  const now = config.now ?? (() => new Date());
  const idGenerator = config.idGenerator ?? (() => createWorkflowId());
  const traceIdGenerator = config.traceIdGenerator ?? (() => createTraceId());

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
    const binding = config.resolveWorkflow(name, env);
    if (!binding) {
      throw new Error(`No Cloudflare Workflow binding for ${name}`);
    }

    const createdAt = now();
    const envelope: WorkflowEventEnvelope<string, WorkflowPayload> = {
      id: options?.id ?? idGenerator(),
      name,
      payload: parsedPayload,
      traceId: options?.traceId ?? traceIdGenerator(),
      idempotencyKey: options?.idempotencyKey ?? createWorkflowId("idem"),
      scheduledAt: resolveScheduledAt(createdAt, options),
      createdAt: createdAt.toISOString(),
      metadata: options?.metadata,
    };

    await createCloudflareWorkflowInstance(binding, envelope, {
      allowExisting: true,
    });

    const instance: WorkflowInstance = {
      id: envelope.id,
      name: envelope.name,
      status: envelope.scheduledAt ? "scheduled" : "queued",
      traceId: envelope.traceId,
      idempotencyKey: envelope.idempotencyKey,
      scheduledAt: envelope.scheduledAt,
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
  const now = config.now ?? (() => new Date());
  const rateLimiter = createRateLimiter(config.rateLimit);

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
        let target: { workflowName: string; status: unknown } | null;
        try {
          target = name
            ? await getStatusByName(id, name, env, config)
            : await findStatusById(id, env, config);
        } catch (error) {
          // Unexpected binding.get()/status() failures (transient Cloudflare API
          // error, network failure, or an error whose message does not match the
          // "not found" heuristic) are surfaced as a controlled JSON 502 rather
          // than crashing fetch() with an unhandled rejection.
          return Response.json(
            { error: error instanceof Error ? error.message : String(error) },
            { status: 502 },
          );
        }
        if (!target) {
          return Response.json(
            { error: `No status binding for ${name ?? id}` },
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

        const errors: Array<{ id: string; error: string }> = [];
        const prepared: Array<{
          envelope: WorkflowEventEnvelope;
          binding: CloudflareWorkflowBinding;
        }> = [];

        for (const [index, candidate] of body.events.entries()) {
          try {
            if (!isWorkflowEventEnvelope(candidate)) {
              throw new Error(`Invalid event structure at index ${index}`);
            }

            const envelope = candidate;
            if (!config.registry.has(envelope.name)) {
              throw new Error(`Unknown workflow: ${envelope.name}`);
            }

            const workflow = config.registry.get(envelope.name);
            const payload = config.registry.parsePayload(workflow, envelope.payload);

            const binding = config.resolveWorkflow(envelope.name, env);
            if (!binding) {
              throw new Error(`No Cloudflare Workflow binding for ${envelope.name}`);
            }

            prepared.push({ envelope: { ...envelope, payload }, binding });
          } catch (error) {
            errors.push({
              id: getCandidateEventId(candidate),
              error: error instanceof Error ? error.message : String(error),
            });
          }
        }

        const accepted = await createCloudflareWorkflowInstances(prepared, errors);
        const instances = accepted.map((envelope) => ({
          id: envelope.id,
          name: envelope.name,
          status: envelope.scheduledAt ? ("scheduled" as const) : ("queued" as const),
          traceId: envelope.traceId,
          idempotencyKey: envelope.idempotencyKey,
          scheduledAt: envelope.scheduledAt,
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
    ): Promise<{ dispatched: number; errors: number }> {
      let dispatched = 0;
      let errors = 0;
      const scheduledTime =
        typeof (controller as { scheduledTime?: unknown } | null)?.scheduledTime === "number"
          ? new Date((controller as { scheduledTime: number }).scheduledTime)
          : now();
      const current = scheduledTime;

      for (const workflow of config.registry.workflows) {
        // Per-workflow isolation: an unsupported cron string or a bug in
        // collectDueCronRuns must not abort the whole sweep for other workflows.
        let dueRuns;
        try {
          dueRuns = collectDueCronRuns(workflow, current);
        } catch (error) {
          errors++;
          console.error("workflow.cron_collect_failed", {
            workflow: workflow.name,
            error: error instanceof Error ? error.message : String(error),
          });
          continue;
        }

        for (const run of dueRuns) {
          // Per-run isolation mirrors the POST /dispatch handler: one bad run
          // (payload parse failure, create failure) is logged and skipped so it
          // cannot block the remaining due runs.
          try {
            const binding = config.resolveWorkflow(run.workflowName, env);
            if (!binding) continue;
            const payload = config.registry.parsePayload(workflow, run.payload);

            const envelope: WorkflowEventEnvelope<string, WorkflowPayload> = {
              id: createCloudflareInstanceId(run.runKey),
              name: run.workflowName,
              payload,
              traceId: createWorkflowId("trace"),
              idempotencyKey: run.runKey,
              scheduledAt: run.scheduledAt.toISOString(),
              createdAt: current.toISOString(),
              metadata: run.metadata,
            };

            const created = await createCloudflareWorkflowInstance(binding, envelope, {
              allowExisting: true,
            });
            if (created) dispatched++;
          } catch (error) {
            errors++;
            console.error("workflow.cron_dispatch_failed", {
              workflow: run.workflowName,
              runKey: run.runKey,
              error: error instanceof Error ? error.message : String(error),
            });
          }
        }
      }

      return { dispatched, errors };
    },
  };
}

async function createCloudflareWorkflowInstances(
  events: Array<{
    envelope: WorkflowEventEnvelope;
    binding: CloudflareWorkflowBinding;
  }>,
  errors: Array<{ id: string; error: string }>,
): Promise<WorkflowEventEnvelope[]> {
  const accepted: WorkflowEventEnvelope[] = [];
  const groups = new Map<
    CloudflareWorkflowBinding,
    WorkflowEventEnvelope[]
  >();

  for (const event of events) {
    const group = groups.get(event.binding);
    if (group) {
      group.push(event.envelope);
    } else {
      groups.set(event.binding, [event.envelope]);
    }
  }

  for (const [binding, group] of groups) {
    for (const chunk of chunkEnvelopes(group, 100)) {
      accepted.push(...(await createCloudflareWorkflowChunk(binding, chunk, errors)));
    }
  }

  return accepted;
}

async function createCloudflareWorkflowChunk(
  binding: CloudflareWorkflowBinding,
  envelopes: WorkflowEventEnvelope[],
  errors: Array<{ id: string; error: string }>,
): Promise<WorkflowEventEnvelope[]> {
  if (envelopes.length === 0) return [];

  if (binding.createBatch) {
    try {
      // createBatch throws only when the whole batch fails; on success it has
      // created every new id and silently skipped any that already exist
      // (within retention). A skipped id is an idempotent re-dispatch — e.g. a
      // retry after the worker created the instance but the response was lost —
      // so the entire batch is accepted, not partially errored.
      await binding.createBatch(
        envelopes.map((envelope) => ({
          id: envelope.id,
          params: envelope,
        })),
      );
      return envelopes;
    } catch {
      // Fall back to per-instance creation so one bad instance does not reject
      // the whole batch response.
    }
  }

  return createCloudflareWorkflowsIndividually(binding, envelopes, errors);
}

async function createCloudflareWorkflowsIndividually(
  binding: CloudflareWorkflowBinding,
  envelopes: WorkflowEventEnvelope[],
  errors: Array<{ id: string; error: string }>,
): Promise<WorkflowEventEnvelope[]> {
  const accepted: WorkflowEventEnvelope[] = [];
  for (const envelope of envelopes) {
    try {
      // allowExisting: a duplicate instance id means the event was already
      // dispatched (e.g. a retried dispatch after the worker created the
      // instance but the response was lost) and is an idempotent success,
      // not a failure to retry.
      await createCloudflareWorkflowInstance(binding, envelope, {
        allowExisting: true,
      });
      accepted.push(envelope);
    } catch (error) {
      errors.push({
        id: envelope.id,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  return accepted;
}

function chunkEnvelopes<T>(envelopes: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let index = 0; index < envelopes.length; index += size) {
    chunks.push(envelopes.slice(index, index + size));
  }
  return chunks;
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

function isWorkflowEventEnvelope(value: unknown): value is WorkflowEventEnvelope {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return false;
  }

  const envelope = value as Partial<WorkflowEventEnvelope>;
  return (
    typeof envelope.id === "string" &&
    envelope.id.length > 0 &&
    typeof envelope.name === "string" &&
    envelope.name.length > 0 &&
    typeof envelope.traceId === "string" &&
    envelope.traceId.length > 0 &&
    typeof envelope.idempotencyKey === "string" &&
    envelope.idempotencyKey.length > 0 &&
    typeof envelope.createdAt === "string" &&
    typeof envelope.payload === "object" &&
    envelope.payload !== null &&
    !Array.isArray(envelope.payload)
  );
}

function getCandidateEventId(value: unknown): string {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return "unknown";
  }

  const id = (value as { id?: unknown }).id;
  return typeof id === "string" && id.length > 0 ? id : "unknown";
}

// NOTE: This fixed-window limiter keeps its counter in per-isolate closure
// state. Cloudflare Workers run many isolates across PoPs, each with its own
// window, so the effective accepted rate is `max * <isolate count>` and a
// client spreading requests can bypass it; fixed windows also permit ~2x bursts
// at window boundaries. Treat it as best-effort in-process throttling only —
// for true global enforcement back it with a Cloudflare Rate Limiting binding or
// a Durable Object counter.
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
    const date = new Date(options.scheduledAt);
    if (Number.isNaN(date.getTime())) {
      throw new WorkflowValidationError(
        `Invalid scheduledAt: ${options.scheduledAt}`,
      );
    }
    return date.toISOString();
  }

  if (typeof options?.delayMs === "number" && options.delayMs > 0) {
    return new Date(now.getTime() + options.delayMs).toISOString();
  }

  return undefined;
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

async function findStatusById<TEnv>(
  id: string,
  env: TEnv,
  config: CloudflareDispatchHandlerConfig<TEnv>,
): Promise<{ workflowName: string; status: unknown } | null> {
  let unknown: { workflowName: string; status: unknown } | null = null;

  for (const workflow of config.registry.workflows) {
    const target = await getStatusByName(id, workflow.name, env, config);
    if (!target) continue;

    const status =
      typeof target.status === "string"
        ? target.status
        : (target.status as { status?: unknown } | null)?.status;
    if (normalizeCloudflareStatus(status) !== "unknown") return target;
    unknown ??= target;
  }

  return unknown;
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
    return { name: error.name, message: error.message };
  }

  if (typeof error === "object" && error !== null) {
    const value = error as { name?: unknown; message?: unknown };
    return {
      name: typeof value.name === "string" ? value.name : "WorkflowError",
      message:
        typeof value.message === "string" ? value.message : JSON.stringify(error),
    };
  }

  return { name: "WorkflowError", message: String(error) };
}

async function createCloudflareWorkflowInstance(
  binding: CloudflareWorkflowBinding,
  envelope: WorkflowEventEnvelope,
  options: { allowExisting?: boolean } = {},
): Promise<boolean> {
  // createBatch is idempotent (it skips ids that already exist instead of
  // throwing), so prefer it when re-dispatch must tolerate duplicates. An empty
  // result means the instance already existed and was skipped.
  if (options.allowExisting && binding.createBatch) {
    const created = await binding.createBatch([{ id: envelope.id, params: envelope }]);
    return created.length > 0;
  }

  try {
    await binding.create({
      id: envelope.id,
      params: envelope,
    });
    return true;
  } catch (error) {
    // create() throws on a duplicate id; when re-dispatch is allowed that is an
    // idempotent no-op (the instance already exists and will run), not an error.
    if (options.allowExisting && isDuplicateCloudflareInstanceError(error)) {
      return false;
    }
    throw error;
  }
}

function isMissingCloudflareInstanceError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return /not\s*found|does\s*not\s*exist|no\s*such\s*instance|unknown\s*instance/i.test(message);
}

function isDuplicateCloudflareInstanceError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return /already\s*(exists|used)|duplicate|conflict|in\s*use/i.test(message);
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
