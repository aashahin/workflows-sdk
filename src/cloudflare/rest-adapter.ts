import { WorkflowSendError } from "../core/errors";
import type {
  WorkflowAdapter,
  WorkflowEventEnvelope,
  WorkflowInstance,
  WorkflowStatus,
} from "../core/types";

export interface CloudflareRestWorkflowAdapterConfig {
  accountId: string;
  apiToken: string;
  workflowName: string | ((eventName: string) => string);
  baseUrl?: string;
  timeoutMs?: number;
  fetch?: FetchLike;
}

export class CloudflareRestWorkflowAdapter implements WorkflowAdapter {
  private readonly baseUrl: string;
  private readonly timeoutMs: number;
  private readonly fetchImpl: FetchLike;
  private readonly instanceNames = new Map<string, string>();

  constructor(private readonly config: CloudflareRestWorkflowAdapterConfig) {
    this.baseUrl = (config.baseUrl ?? "https://api.cloudflare.com/client/v4").replace(
      /\/+$/,
      "",
    );
    this.timeoutMs = config.timeoutMs ?? 10_000;
    this.fetchImpl = config.fetch ?? fetch;
  }

  async dispatch(envelope: WorkflowEventEnvelope): Promise<WorkflowInstance> {
    const workflowName = this.resolveWorkflowName(envelope.name);
    const response = await this.request(
      `/accounts/${encodeURIComponent(this.config.accountId)}/workflows/${encodeURIComponent(workflowName)}/instances`,
      {
        method: "POST",
        body: JSON.stringify({
          instance_id: envelope.id,
          params: envelope,
        }),
      },
    );

    const result = parseCloudflareResult(response);
    this.instanceNames.set(envelope.id, workflowName);
    return {
      id: String(result.id ?? envelope.id),
      name: envelope.name,
      status: normalizeCloudflareStatus(result.status),
      traceId: envelope.traceId,
      idempotencyKey: envelope.idempotencyKey,
      scheduledAt: envelope.scheduledAt,
      createdAt: envelope.createdAt,
      updatedAt: new Date().toISOString(),
    };
  }

  async dispatchBatch(
    envelopes: WorkflowEventEnvelope[],
  ): Promise<WorkflowInstance[]> {
    const instances: WorkflowInstance[] = [];
    for (const envelope of envelopes) {
      instances.push(await this.dispatch(envelope));
    }
    return instances;
  }

  async getInstance(
    id: string,
    options?: { name?: string },
  ): Promise<WorkflowInstance | null> {
    const workflowName = options?.name
      ? this.resolveWorkflowName(options.name)
      : this.instanceNames.get(id);
    if (!workflowName) {
      throw new WorkflowSendError(
        "Cloudflare REST status lookup requires options.name or a prior dispatch from this adapter",
      );
    }

    const path = `/accounts/${encodeURIComponent(this.config.accountId)}/workflows/${encodeURIComponent(workflowName)}/instances/${encodeURIComponent(id)}`;
    const response = await this.request(path, { method: "GET" }, { allowNotFound: true });
    if (response === null) return null;

    const result = parseCloudflareResult(response);
    return {
      id: String(result.id ?? id),
      name: options?.name ?? workflowName,
      status: normalizeCloudflareStatus(result.status),
      createdAt: stringOrUndefined(result.created_on ?? result.createdAt ?? result.start),
      updatedAt: stringOrUndefined(result.modified_on ?? result.updatedAt ?? result.end),
      output: result.output,
      error: normalizeCloudflareError(result.error),
    };
  }

  private resolveWorkflowName(eventName: string): string {
    return typeof this.config.workflowName === "function"
      ? this.config.workflowName(eventName)
      : this.config.workflowName;
  }

  private async request(
    path: string,
    init: RequestInit,
    options: { allowNotFound?: boolean } = {},
  ): Promise<unknown | null> {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.timeoutMs);

    try {
      const response = await this.fetchImpl(`${this.baseUrl}${path}`, {
        ...init,
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${this.config.apiToken}`,
          ...init.headers,
        },
        signal: controller.signal,
      });

      if (options.allowNotFound && response.status === 404) return null;

      const body = await response.text();
      const json = body ? JSON.parse(body) : {};
      if (!response.ok || (isCloudflareEnvelope(json) && json.success === false)) {
        throw new WorkflowSendError(
          `Cloudflare Workflows API responded with ${response.status}: ${body}`,
        );
      }
      return json;
    } catch (error) {
      if (error instanceof WorkflowSendError) throw error;
      throw new WorkflowSendError(
        `Failed to call Cloudflare Workflows API: ${error instanceof Error ? error.message : String(error)}`,
        error,
      );
    } finally {
      clearTimeout(timeout);
    }
  }
}

type FetchLike = (input: string | URL | Request, init?: RequestInit) => Promise<Response>;

function parseCloudflareResult(response: unknown): Record<string, unknown> {
  if (isCloudflareEnvelope(response) && response.result && typeof response.result === "object") {
    return response.result as Record<string, unknown>;
  }
  if (response && typeof response === "object") return response as Record<string, unknown>;
  return {};
}

function isCloudflareEnvelope(value: unknown): value is {
  success?: boolean;
  result?: unknown;
} {
  return typeof value === "object" && value !== null && "result" in value;
}

function normalizeCloudflareStatus(status: unknown): WorkflowStatus {
  switch (String(status ?? "").toLowerCase()) {
    case "queued":
      return "queued";
    case "running":
      return "running";
    case "paused":
      return "paused";
    case "errored":
      return "errored";
    case "failed":
      return "failed";
    case "terminated":
      return "terminated";
    case "complete":
    case "completed":
    case "success":
      return "complete";
    case "canceled":
    case "cancelled":
      return "cancelled";
    case "waiting":
      return "waiting";
    case "waitingforpause":
    case "waiting_for_pause":
    case "waiting for pause":
      return "waitingForPause";
    default:
      return "unknown";
  }
}

function normalizeCloudflareError(
  error: unknown,
): { name: string; message: string } | undefined {
  if (!error) return undefined;
  if (error instanceof Error) return { name: error.name, message: error.message };
  if (typeof error === "object") {
    const value = error as { name?: unknown; message?: unknown };
    return {
      name: typeof value.name === "string" ? value.name : "WorkflowError",
      message: typeof value.message === "string" ? value.message : JSON.stringify(error),
    };
  }
  return { name: "WorkflowError", message: String(error) };
}

function stringOrUndefined(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}
