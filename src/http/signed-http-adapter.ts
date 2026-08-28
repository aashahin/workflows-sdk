import { WorkflowSendError } from "../core/errors";
import { InstanceNameCache } from "../core/instance-name-cache";
import { assertCloudflareWorkflowEnvelope } from "../cloudflare/serialization";
import {
  MAX_WORKFLOW_HTTP_ERROR_BYTES,
  MAX_WORKFLOW_HTTP_SUCCESS_BYTES,
  readBoundedResponseText,
  summarizeResponseText,
} from "./bounded-response";
import type {
  WorkflowAdapter,
  WorkflowEventEnvelope,
  WorkflowInstance,
} from "../core/types";

export interface SignedHttpAdapterConfig {
  baseUrl: string;
  authToken: string;
  timeoutMs?: number;
  /** Process-local status-name hints retained in LRU order; set 0 to disable. */
  instanceNameCacheSize?: number;
  fetch?: typeof fetch;
}

export class SignedHttpAdapter implements WorkflowAdapter {
  private readonly baseUrl: string;
  private readonly timeoutMs: number;
  private readonly fetchImpl: typeof fetch;
  private readonly instanceNames: InstanceNameCache;

  constructor(private readonly config: SignedHttpAdapterConfig) {
    this.baseUrl = config.baseUrl.replace(/\/+$/, "");
    this.timeoutMs = config.timeoutMs ?? 10_000;
    this.fetchImpl = config.fetch ?? fetch;
    this.instanceNames = new InstanceNameCache(config.instanceNameCacheSize);
  }

  async dispatch(envelope: WorkflowEventEnvelope): Promise<WorkflowInstance> {
    const [instance] = await this.dispatchBatch([envelope]);
    if (!instance) {
      throw new WorkflowSendError("HTTP dispatch did not return an instance");
    }
    return instance;
  }

  async dispatchBatch(
    envelopes: WorkflowEventEnvelope[],
  ): Promise<WorkflowInstance[]> {
    const seenIds = new Set<string>();
    for (const envelope of envelopes) {
      try {
        assertCloudflareWorkflowEnvelope(envelope);
      } catch (cause) {
        const error = new WorkflowSendError(
          `Invalid Cloudflare Workflow envelope: ${cause instanceof Error ? cause.message : String(cause)}`,
          cause,
        );
        (error as { nonRetryable?: boolean }).nonRetryable = true;
        throw error;
      }
      if (seenIds.has(envelope.id)) {
        const error = new WorkflowSendError(
          `Duplicate Workflow instance id in dispatch batch: ${envelope.id}`,
        );
        (error as { nonRetryable?: boolean }).nonRetryable = true;
        throw error;
      }
      seenIds.add(envelope.id);
    }

    const instances: WorkflowInstance[] = [];
    for (const chunk of chunkDispatchEnvelopes(envelopes)) {
      try {
        instances.push(...(await this.dispatchChunk(chunk)));
      } catch (error) {
        if (error instanceof WorkflowSendError) {
          const dispatched = new Set([
            ...instances.map((instance) => instance.id),
            ...((error as { dispatchedIds?: string[] }).dispatchedIds ?? []),
          ]);
          (error as { dispatchedIds?: string[] }).dispatchedIds = [...dispatched];
          (error as { failedIds?: string[] }).failedIds = envelopes
            .map((envelope) => envelope.id)
            .filter((id) => !dispatched.has(id));
        }
        throw error;
      }
    }
    return instances;
  }

  private async dispatchChunk(
    envelopes: WorkflowEventEnvelope[],
  ): Promise<WorkflowInstance[]> {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.timeoutMs);

    try {
      const response = await this.fetchImpl(`${this.baseUrl}/dispatch`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${this.config.authToken}`,
        },
        body: JSON.stringify({ events: envelopes }),
        signal: controller.signal,
      });

      if (!response.ok) {
        const body = await readBoundedResponseText(
          response,
          MAX_WORKFLOW_HTTP_ERROR_BYTES,
        ).catch(() => ({ text: "", truncated: false }));
        const error = new WorkflowSendError(
          `Workflow dispatcher responded with ${response.status}: ${summarizeResponseText(body.text, body.truncated)}`,
        );
        if (isPermanentDispatcherStatus(response.status)) {
          (error as { nonRetryable?: boolean }).nonRetryable = true;
        }
        throw error;
      }

      const result = await readSuccessfulJson<{
        instances?: WorkflowInstance[];
        ids?: string[];
        errors?: Array<{ id: string; error: string }>;
      }>(response, "Workflow dispatcher");

      if (result.errors?.length) {
        const failedIds = new Set(result.errors.map((item) => item.id));
        const dispatchedIds = (result.instances?.map((item) => item.id) ?? result.ids ?? [])
          .filter((id) => !failedIds.has(id));
        const error = new WorkflowSendError(
          `Partial workflow dispatch failure: ${result.errors
            .map((error) => `${error.id}: ${error.error}`)
            .join("; ")}`,
        );
        (error as { dispatchedIds?: string[]; failedIds?: string[] }).dispatchedIds =
          dispatchedIds;
        (error as { dispatchedIds?: string[]; failedIds?: string[] }).failedIds =
          [...failedIds];
        if (result.errors.every((item) => isNonRetryableDispatchError(item.error))) {
          (error as { nonRetryable?: boolean }).nonRetryable = true;
        }
        throw error;
      }

      if (result.instances) {
        if (result.instances.length !== envelopes.length) {
          const error = new WorkflowSendError(
            `Workflow dispatcher returned ${result.instances.length}/${envelopes.length} instances`,
          );
          (error as { dispatchedIds?: string[] }).dispatchedIds =
            result.instances.map((instance) => instance.id);
          throw error;
        }
        for (const instance of result.instances) {
          this.instanceNames.set(instance.id, instance.name);
        }
        return result.instances;
      }

      const instances = (result.ids ?? []).map((id) => ({
        id,
        name: envelopes.find((event) => event.id === id)?.name ?? "unknown",
        status: "queued" as const,
      }));
      for (const instance of instances) {
        this.instanceNames.set(instance.id, instance.name);
      }
      if (instances.length !== envelopes.length) {
        const error = new WorkflowSendError(
          `Workflow dispatcher returned ${instances.length}/${envelopes.length} ids`,
        );
        (error as { dispatchedIds?: string[] }).dispatchedIds = instances.map(
          (instance) => instance.id,
        );
        throw error;
      }
      return instances;
    } catch (error) {
      if (error instanceof WorkflowSendError) throw error;

      throw new WorkflowSendError(
        `Failed to dispatch workflow events: ${error instanceof Error ? error.message : String(error)}`,
        error,
      );
    } finally {
      clearTimeout(timeout);
    }
  }

  async getInstance(
    id: string,
    options?: { name?: string },
  ): Promise<WorkflowInstance | null> {
    const name = options?.name ?? this.instanceNames.get(id);
    if (!name) {
      const error = new WorkflowSendError(
        "Workflow HTTP status lookup requires options.name or a prior dispatch from this adapter",
      );
      (error as { nonRetryable?: boolean }).nonRetryable = true;
      throw error;
    }
    const url = new URL(
      `${this.baseUrl}/status/${encodeURIComponent(id)}`,
    );
    url.searchParams.set("name", name);

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.timeoutMs);

    try {
      const response = await this.fetchImpl(url, {
        headers: {
          Authorization: `Bearer ${this.config.authToken}`,
        },
        signal: controller.signal,
      });

      if (response.status === 404) {
        await response.body?.cancel();
        return null;
      }
      if (!response.ok) {
        const body = await readBoundedResponseText(
          response,
          MAX_WORKFLOW_HTTP_ERROR_BYTES,
        ).catch(() => ({ text: "", truncated: false }));
        const error = new WorkflowSendError(
          `Workflow status responded with ${response.status}: ${summarizeResponseText(body.text, body.truncated)}`,
        );
        if (isPermanentDispatcherStatus(response.status)) {
          (error as { nonRetryable?: boolean }).nonRetryable = true;
        }
        throw error;
      }

      const instance = await readSuccessfulJson<WorkflowInstance>(
        response,
        "Workflow status",
      );
      const resolvedName = options?.name ?? instance.name;
      if (resolvedName) this.instanceNames.set(id, resolvedName);
      return instance;
    } catch (error) {
      if (error instanceof WorkflowSendError) throw error;
      throw new WorkflowSendError(
        `Failed to get workflow status: ${error instanceof Error ? error.message : String(error)}`,
        error,
      );
    } finally {
      clearTimeout(timeout);
    }
  }
}

async function readSuccessfulJson<T>(
  response: Response,
  label: string,
): Promise<T> {
  const bounded = await readBoundedResponseText(
    response,
    MAX_WORKFLOW_HTTP_SUCCESS_BYTES,
  );
  if (bounded.truncated) {
    throw new WorkflowSendError(
      `${label} response exceeds ${MAX_WORKFLOW_HTTP_SUCCESS_BYTES} bytes`,
    );
  }

  try {
    return JSON.parse(bounded.text) as T;
  } catch (cause) {
    throw new WorkflowSendError(`${label} returned invalid JSON`, cause);
  }
}

function isNonRetryableDispatchError(message: string): boolean {
  return /invalid event structure|missing events array|workflow payload validation failed|unsupported workflow callback|unknown (event|workflow)|no cloudflare workflow binding|cloudflare workflow receipt conflict|duplicate (?:workflow )?(?:instance )?id|workflow instance already exists|refusing (?:an )?unverified existing id|creation was not verified/i.test(
    message,
  );
}

function isPermanentDispatcherStatus(status: number): boolean {
  return (
    status === 400 ||
    status === 401 ||
    status === 403 ||
    status === 404 ||
    status === 409 ||
    status === 413 ||
    status === 422
  );
}

function chunkDispatchEnvelopes(
  envelopes: WorkflowEventEnvelope[],
): WorkflowEventEnvelope[][] {
  const chunks: WorkflowEventEnvelope[][] = [];
  let current: WorkflowEventEnvelope[] = [];
  let currentBytes = new TextEncoder().encode('{"events":[]}').byteLength;

  for (const envelope of envelopes) {
    const bytes = new TextEncoder().encode(JSON.stringify(envelope)).byteLength;
    if (
      current.length > 0 &&
      (current.length >= 100 || currentBytes + bytes + 1 > 900_000)
    ) {
      chunks.push(current);
      current = [];
      currentBytes = new TextEncoder().encode('{"events":[]}').byteLength;
    }
    current.push(envelope);
    currentBytes += bytes + (current.length > 1 ? 1 : 0);
  }
  if (current.length > 0) chunks.push(current);
  return chunks;
}
