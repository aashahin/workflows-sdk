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

        const body = (await request.json().catch(() => null)) as {
          events?: WorkflowEventEnvelope[];
        } | null;

        if (!body?.events || !Array.isArray(body.events)) {
          return Response.json({ error: "Missing events array" }, { status: 400 });
        }

        const instances: WorkflowInstance[] = [];
        const errors: Array<{ id: string; error: string }> = [];

        for (const envelope of body.events) {
          try {
            if (!config.registry.has(envelope.name)) {
              throw new Error(`Unknown workflow: ${envelope.name}`);
            }

            const binding = config.resolveWorkflow(envelope.name, env);
            if (!binding) {
              throw new Error(`No Cloudflare Workflow binding for ${envelope.name}`);
            }

            await createCloudflareWorkflowInstance(binding, envelope);

            instances.push({
              id: envelope.id,
              name: envelope.name,
              status: envelope.scheduledAt ? "scheduled" : "queued",
              traceId: envelope.traceId,
              idempotencyKey: envelope.idempotencyKey,
              scheduledAt: envelope.scheduledAt,
              createdAt: envelope.createdAt,
              updatedAt: now().toISOString(),
            });
          } catch (error) {
            errors.push({
              id: envelope.id ?? "unknown",
              error: error instanceof Error ? error.message : String(error),
            });
          }
        }

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

          const envelope: WorkflowEventEnvelope<string, WorkflowPayload> = {
            id: createCloudflareInstanceId(run.runKey),
            name: run.workflowName,
            payload: run.payload,
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
  const expected =
    typeof config.auth?.bearerToken === "function"
      ? config.auth.bearerToken(env)
      : config.auth?.bearerToken;

  if (!expected) return null;

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
