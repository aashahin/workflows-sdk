import { collectDueCronRuns } from "../scheduler/cron";
import { createWorkflowId } from "../core/id";
import type { WorkflowRegistry } from "../core/registry";
import type {
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
        const target = name
          ? await getStatusByName(id, name, env, config)
          : await findStatusById(id, env, config);
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
    ): Promise<{ dispatched: number }> {
      let dispatched = 0;
      const scheduledTime =
        typeof (controller as { scheduledTime?: unknown } | null)?.scheduledTime === "number"
          ? new Date((controller as { scheduledTime: number }).scheduledTime)
          : now();
      const current = scheduledTime;

      for (const workflow of config.registry.workflows) {
        for (const run of collectDueCronRuns(workflow, current)) {
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
        }
      }

      return { dispatched };
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
      const created = await binding.createBatch(
        envelopes.map((envelope) => ({
          id: envelope.id,
          params: envelope,
        })),
      );
      const createdIds = getCreatedBatchIds(created);
      if (createdIds) {
        const accepted = envelopes.filter((envelope) => createdIds.has(envelope.id));
        for (const envelope of envelopes) {
          if (!createdIds.has(envelope.id)) {
            errors.push({
              id: envelope.id,
              error: "Cloudflare Workflow instance was not created by createBatch",
            });
          }
        }
        return accepted;
      }
      if (created.length === envelopes.length) return envelopes;
    } catch {
      // Fall back to individual creates so one bad instance does not reject the
      // whole batch response when the platform provides per-instance errors.
    }
  }

  const accepted: WorkflowEventEnvelope[] = [];
  for (const envelope of envelopes) {
    try {
      await createCloudflareWorkflowInstance(binding, envelope);
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
    if (options.allowExisting && isDuplicateCloudflareInstanceError(error)) {
      return false;
    }
    throw error;
  }
}

function getCreatedBatchIds(created: unknown[]): Set<string> | null {
  const ids = new Set<string>();

  for (const item of created) {
    if (typeof item !== "object" || item === null) return null;

    const id = (item as { id?: unknown }).id;
    if (typeof id !== "string" || id.length === 0) return null;
    ids.add(id);
  }

  return ids;
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
