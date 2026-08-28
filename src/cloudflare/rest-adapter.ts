import { WorkflowSendError } from "../core/errors";
import { InstanceNameCache } from "../core/instance-name-cache";
import {
  MAX_WORKFLOW_HTTP_ERROR_BYTES,
  MAX_WORKFLOW_HTTP_SUCCESS_BYTES,
  readBoundedResponseText,
  summarizeResponseText,
} from "../http/bounded-response";
import type {
  WorkflowAdapter,
  WorkflowEventEnvelope,
  WorkflowInstance,
  WorkflowStatus,
} from "../core/types";
import {
  DEFAULT_CLOUDFLARE_WORKFLOW_RETENTION,
  type CloudflareWorkflowRetention,
} from "./dispatch-handler";
import { assertCloudflareWorkflowEnvelope } from "./serialization";

export interface CloudflareRestWorkflowAdapterConfig {
  accountId: string;
  apiToken: string;
  workflowName: string | ((eventName: string) => string);
  baseUrl?: string;
  timeoutMs?: number;
  /** Process-local status-name hints retained in LRU order; set 0 to disable. */
  instanceNameCacheSize?: number;
  retention?: CloudflareWorkflowRetention;
  fetch?: FetchLike;
}

export class CloudflareRestWorkflowAdapter implements WorkflowAdapter {
  private readonly baseUrl: string;
  private readonly timeoutMs: number;
  private readonly fetchImpl: FetchLike;
  private readonly retention: CloudflareWorkflowRetention;
  private readonly instanceNames: InstanceNameCache;

  constructor(private readonly config: CloudflareRestWorkflowAdapterConfig) {
    this.baseUrl = (config.baseUrl ?? "https://api.cloudflare.com/client/v4").replace(
      /\/+$/,
      "",
    );
    this.timeoutMs = config.timeoutMs ?? 10_000;
    this.fetchImpl = config.fetch ?? fetch;
    this.instanceNames = new InstanceNameCache(config.instanceNameCacheSize);
    this.retention =
      config.retention ?? DEFAULT_CLOUDFLARE_WORKFLOW_RETENTION;
  }

  async dispatch(envelope: WorkflowEventEnvelope): Promise<WorkflowInstance> {
    assertCloudflareWorkflowEnvelope(envelope);
    const workflowName = this.resolveWorkflowName(envelope.name);
    const response = await this.request(
      `/accounts/${encodeURIComponent(this.config.accountId)}/workflows/${encodeURIComponent(workflowName)}/instances`,
      {
        method: "POST",
        body: JSON.stringify({
          instance_id: envelope.id,
          params: JSON.stringify(envelope),
          instance_retention: toRestRetention(this.retention),
        }),
      },
    );

    const result = parseCloudflareResult(response);
    this.instanceNames.set(envelope.id, workflowName);
    return toWorkflowInstance(envelope, result);
  }

  async dispatchBatch(
    envelopes: WorkflowEventEnvelope[],
  ): Promise<WorkflowInstance[]> {
    if (envelopes.length === 0) return [];
    const groups = new Map<
      string,
      Array<{ envelope: WorkflowEventEnvelope; index: number }>
    >();
    const seen = new Map<string, string>();
    for (const [index, envelope] of envelopes.entries()) {
      assertCloudflareWorkflowEnvelope(envelope);
      const workflowName = this.resolveWorkflowName(envelope.name);
      const identity = `${workflowName}\u0000${envelope.id}`;
      const serialized = JSON.stringify(envelope);
      const previous = seen.get(identity);
      if (previous !== undefined) {
        throw new WorkflowSendError(
          `${previous === serialized ? "Duplicate" : "Conflicting duplicate"} Workflow instance id ${envelope.id}`,
        );
      }
      seen.set(identity, serialized);
      const group = groups.get(workflowName);
      const item = { envelope, index };
      if (group) group.push(item);
      else groups.set(workflowName, [item]);
    }

    const instances = new Array<WorkflowInstance>(envelopes.length);
    for (const [workflowName, group] of groups) {
      for (const chunk of chunkRestBatch(group, this.retention)) {
        const response = await this.request(
          `/accounts/${encodeURIComponent(this.config.accountId)}/workflows/${encodeURIComponent(workflowName)}/instances/batch`,
          {
            method: "POST",
            body: JSON.stringify(
              chunk.map(({ envelope }) =>
                toRestCreateBody(envelope, this.retention),
              ),
            ),
          },
        );
        const expectedIds = new Set(
          chunk.map(({ envelope }) => envelope.id),
        );
        const byId = new Map<string, Record<string, unknown>>();
        for (const result of parseCloudflareResults(response)) {
          const resultId = typeof result.id === "string" ? result.id : "";
          if (
            resultId.length === 0 ||
            !expectedIds.has(resultId) ||
            byId.has(resultId)
          ) {
            throw batchResponseMismatch(
              `Cloudflare Workflows batch response contained ${
                resultId.length === 0
                  ? "an item without an id"
                  : byId.has(resultId)
                    ? `duplicate id ${resultId}`
                    : `unexpected id ${resultId}`
              }`,
              instances,
              chunk.map(({ envelope }) => envelope.id),
            );
          }
          byId.set(resultId, result);
        }

        const missing = chunk.filter(
          ({ envelope }) => !byId.has(envelope.id),
        );
        for (const { envelope, index } of chunk) {
          const result = byId.get(envelope.id);
          if (!result) continue;
          const instance = toWorkflowInstance(envelope, result);
          instances[index] = instance;
          this.instanceNames.set(envelope.id, workflowName);
        }

        if (missing.length > 0) {
          throw batchResponseMismatch(
            `Cloudflare Workflows batch response omitted ${missing.length} instance(s); refusing unverified existing ids`,
            instances,
            missing.map(({ envelope }) => envelope.id),
          );
        }
      }
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
    this.instanceNames.set(id, workflowName);
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

      if (options.allowNotFound && response.status === 404) {
        await response.body?.cancel();
        return null;
      }

      const bounded = await readBoundedResponseText(
        response,
        response.ok
          ? MAX_WORKFLOW_HTTP_SUCCESS_BYTES
          : MAX_WORKFLOW_HTTP_ERROR_BYTES,
      );
      if (!response.ok) {
        const responseText = summarizeResponseText(
          bounded.text,
          bounded.truncated,
        );
        const error = new WorkflowSendError(
          `Cloudflare Workflows API responded with ${response.status}: ${summarizeResponseText(bounded.text, bounded.truncated)}`,
        );
        const details = error as CloudflareRequestErrorDetails;
        details.status = response.status;
        details.responseText = responseText;
        if (
          !isAlwaysRetryableCloudflareStatus(response.status) &&
          (isPermanentCloudflareStatus(response.status) ||
            isPermanentCloudflareApiFailure(responseText))
        ) {
          details.nonRetryable = true;
        }
        throw error;
      }
      if (bounded.truncated) {
        throw new WorkflowSendError(
          `Cloudflare Workflows API response exceeds ${MAX_WORKFLOW_HTTP_SUCCESS_BYTES} bytes`,
        );
      }
      const json = bounded.text ? JSON.parse(bounded.text) : {};
      if (isCloudflareEnvelope(json) && json.success === false) {
        const responseText = summarizeCloudflareErrors(json);
        const error = new WorkflowSendError(
          `Cloudflare Workflows API reported failure: ${responseText}`,
        );
        const details = error as CloudflareRequestErrorDetails;
        details.status = response.status;
        details.responseText = responseText;
        if (isPermanentCloudflareApiFailure(responseText)) {
          details.nonRetryable = true;
        }
        throw error;
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

function batchResponseMismatch(
  message: string,
  instances: Array<WorkflowInstance | undefined>,
  failedIds: string[],
  cause?: unknown,
): WorkflowSendError {
  const error = new WorkflowSendError(message, cause);
  const details = error as WorkflowSendError & {
    dispatchedIds?: string[];
    failedIds?: string[];
  };
  details.dispatchedIds = instances.flatMap((instance) =>
    instance === undefined ? [] : [instance.id],
  );
  details.failedIds = failedIds;
  return error;
}

type FetchLike = (input: string | URL | Request, init?: RequestInit) => Promise<Response>;

type CloudflareRequestErrorDetails = WorkflowSendError & {
  status?: number;
  responseText?: string;
  nonRetryable?: boolean;
};

function isPermanentCloudflareStatus(status: number): boolean {
  return (
    status === 400 ||
    status === 401 ||
    status === 403 ||
    status === 404 ||
    status === 409 ||
    status === 422
  );
}

function isAlwaysRetryableCloudflareStatus(status: number): boolean {
  return status === 429 || status >= 500;
}

function isPermanentCloudflareApiFailure(responseText: string): boolean {
  if (
    /rate.?limit|too many requests|temporar(?:y|ily)|timed?\s*out|unavailable|overload|internal (?:error|server)/i.test(
      responseText,
    )
  ) {
    return false;
  }

  return /auth(?:entication|orization)?|unauthori[sz]ed|forbidden|permission|access token|validation|malformed|invalid (?:request|parameter|argument|payload|token)|missing (?:required|parameter)|unknown workflow|workflow.*(?:not found|does not exist)|not configured|configuration/i.test(
    responseText,
  );
}

function parseCloudflareResult(response: unknown): Record<string, unknown> {
  if (isCloudflareEnvelope(response) && response.result && typeof response.result === "object") {
    return response.result as Record<string, unknown>;
  }
  if (response && typeof response === "object") return response as Record<string, unknown>;
  return {};
}

function parseCloudflareResults(response: unknown): Record<string, unknown>[] {
  const result = isCloudflareEnvelope(response) ? response.result : response;
  if (Array.isArray(result)) {
    return result.filter(isRecord);
  }
  return isRecord(result) ? [result] : [];
}

function isCloudflareEnvelope(value: unknown): value is {
  success?: boolean;
  result?: unknown;
  errors?: unknown;
  messages?: unknown;
} {
  return (
    typeof value === "object" &&
    value !== null &&
    ("result" in value || "success" in value)
  );
}

function toRestRetention(retention: CloudflareWorkflowRetention) {
  return {
    success_retention: retention.successRetention,
    error_retention: retention.errorRetention,
  };
}

function toRestCreateBody(
  envelope: WorkflowEventEnvelope,
  retention: CloudflareWorkflowRetention,
) {
  return {
    instance_id: envelope.id,
    params: JSON.stringify(envelope),
    instance_retention: toRestRetention(retention),
  };
}

function chunkRestBatch(
  items: Array<{ envelope: WorkflowEventEnvelope; index: number }>,
  retention: CloudflareWorkflowRetention,
) {
  const chunks: typeof items[] = [];
  let current: typeof items = [];
  let currentBytes = 2;
  for (const item of items) {
    const bytes = new TextEncoder().encode(
      JSON.stringify(toRestCreateBody(item.envelope, retention)),
    ).byteLength;
    if (
      current.length > 0 &&
      (current.length >= 100 || currentBytes + bytes + 1 > 900_000)
    ) {
      chunks.push(current);
      current = [];
      currentBytes = 2;
    }
    current.push(item);
    currentBytes += bytes + (current.length > 1 ? 1 : 0);
  }
  if (current.length > 0) chunks.push(current);
  return chunks;
}

function toWorkflowInstance(
  envelope: WorkflowEventEnvelope,
  result: Record<string, unknown>,
): WorkflowInstance {
  return {
    id: String(result.id ?? envelope.id),
    name: envelope.name,
    status: normalizeCloudflareStatus(result.status ?? "queued"),
    traceId: envelope.traceId,
    idempotencyKey: envelope.idempotencyKey,
    scheduledAt: envelope.scheduledAt,
    createdAt: envelope.createdAt,
    updatedAt: new Date().toISOString(),
  };
}

function summarizeCloudflareErrors(value: {
  errors?: unknown;
  messages?: unknown;
}): string {
  return summarizeResponseText(
    JSON.stringify({ errors: value.errors ?? [], messages: value.messages ?? [] }).slice(
      0,
      MAX_WORKFLOW_HTTP_ERROR_BYTES,
    ),
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
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
