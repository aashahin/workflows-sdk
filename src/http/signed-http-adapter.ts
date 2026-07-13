import { WorkflowSendError } from "../core/errors";
import type {
  WorkflowAdapter,
  WorkflowEventEnvelope,
  WorkflowInstance,
} from "../core/types";

export interface SignedHttpAdapterConfig {
  baseUrl: string;
  authToken: string;
  timeoutMs?: number;
  fetch?: typeof fetch;
}

const MAX_INSTANCE_NAMES = 10_000;

export class SignedHttpAdapter implements WorkflowAdapter {
  private readonly baseUrl: string;
  private readonly timeoutMs: number;
  private readonly fetchImpl: typeof fetch;
  private readonly instanceNames = new Map<string, string>();

  constructor(private readonly config: SignedHttpAdapterConfig) {
    this.baseUrl = config.baseUrl.replace(/\/+$/, "");
    this.timeoutMs = config.timeoutMs ?? 10_000;
    this.fetchImpl = config.fetch ?? fetch;
  }

  private rememberInstanceName(id: string, name: string): void {
    this.instanceNames.set(id, name);
    if (this.instanceNames.size > MAX_INSTANCE_NAMES) {
      const oldest = this.instanceNames.keys().next().value;
      if (oldest !== undefined) this.instanceNames.delete(oldest);
    }
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
        const body = await response.text().catch(() => "");
        const error = new WorkflowSendError(
          `Workflow dispatcher responded with ${response.status}: ${body}`,
        );
        if (isPermanentDispatcherStatus(response.status)) {
          (error as { nonRetryable?: boolean }).nonRetryable = true;
        }
        throw error;
      }

      const result = (await response.json()) as {
        instances?: WorkflowInstance[];
        ids?: string[];
        errors?: Array<{ id: string; error: string }>;
      };

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
          throw new WorkflowSendError(
            `Workflow dispatcher returned ${result.instances.length}/${envelopes.length} instances`,
          );
        }
        for (const instance of result.instances) {
          this.rememberInstanceName(instance.id, instance.name);
        }
        return result.instances;
      }

      if (result.ids && result.ids.length !== envelopes.length) {
        throw new WorkflowSendError(
          `Workflow dispatcher returned ${result.ids.length}/${envelopes.length} instances`,
        );
      }

      const instances = (result.ids ?? []).map((id) => ({
        id,
        name: envelopes.find((event) => event.id === id)?.name ?? "unknown",
        status: "queued" as const,
      }));
      for (const instance of instances) {
        this.rememberInstanceName(instance.id, instance.name);
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
    const url = new URL(`${this.baseUrl}/status/${id}`);
    if (name) url.searchParams.set("name", name);

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.timeoutMs);

    try {
      const response = await this.fetchImpl(url, {
        headers: {
          Authorization: `Bearer ${this.config.authToken}`,
        },
        signal: controller.signal,
      });

      if (response.status === 404) return null;
      if (!response.ok) {
        throw new WorkflowSendError(
          `Workflow status responded with ${response.status}: ${await response
            .text()
            .catch(() => "")}`,
        );
      }

      return (await response.json()) as WorkflowInstance;
    } catch (error) {
      if (error instanceof WorkflowSendError) throw error;

      throw new WorkflowSendError(
        `Failed to fetch workflow status: ${error instanceof Error ? error.message : String(error)}`,
        error,
      );
    } finally {
      clearTimeout(timeout);
    }
  }
}

function isNonRetryableDispatchError(message: string): boolean {
  return /invalid event structure|missing events array|workflow payload validation failed|unknown (event|workflow)|no cloudflare workflow binding/i.test(
    message,
  );
}

function isPermanentDispatcherStatus(status: number): boolean {
  return status === 400 || status === 404 || status === 409 || status === 422;
}
