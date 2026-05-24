import { createTraceId, createWorkflowId } from "./id";
import type {
  DispatchOptions,
  DispatchResult,
  WorkflowClientConfig,
  WorkflowEventEnvelope,
  WorkflowInstance,
  WorkflowPayload,
} from "./types";

export class WorkflowClient {
  private readonly idGenerator: () => string;
  private readonly now: () => Date;

  constructor(private readonly config: WorkflowClientConfig) {
    this.idGenerator = config.idGenerator ?? (() => createWorkflowId());
    this.now = config.now ?? (() => new Date());
  }

  async dispatch<TPayload extends WorkflowPayload = WorkflowPayload>(
    name: string,
    payload: TPayload,
    options: DispatchOptions = {},
  ): Promise<DispatchResult> {
    return this.dispatchBatch([{ name, payload, options }]);
  }

  async dispatchBatch(
    events: Array<{
      name: string;
      payload: WorkflowPayload;
      options?: DispatchOptions;
    }>,
  ): Promise<DispatchResult> {
    const envelopes = events.map((event) =>
      this.createEnvelope(event.name, event.payload, event.options),
    );

    try {
      const dispatch = this.config.adapter.dispatchBatch
        ? this.config.adapter.dispatchBatch(envelopes)
        : Promise.all(
            envelopes.map((envelope) => this.config.adapter.dispatch(envelope)),
          );

      const instances = await dispatch;

      if (instances.length !== envelopes.length) {
        throw new Error(
          `Workflow adapter dispatched ${instances.length}/${envelopes.length} event(s)`,
        );
      }

      for (const envelope of envelopes) {
        await this.config.hooks?.onDispatch?.(envelope);
      }

      return {
        ids: instances.map((instance) => instance.id),
        envelopes,
        instances,
      };
    } catch (error) {
      const err = error instanceof Error ? error : new Error(String(error));
      for (const envelope of envelopes) {
        await this.config.hooks?.onDispatchFailed?.(envelope, err);
      }
      throw error;
    }
  }

  getInstance(
    id: string,
    options?: { name?: string },
  ): Promise<WorkflowInstance | null> {
    return this.config.adapter.getInstance(id, options);
  }

  private createEnvelope<TPayload extends WorkflowPayload>(
    name: string,
    payload: TPayload,
    options: DispatchOptions = {},
  ): WorkflowEventEnvelope<string, TPayload> {
    const now = this.now();
    const scheduledAt = resolveScheduledAt(now, options);

    return {
      id: options.id ?? this.idGenerator(),
      name,
      payload,
      traceId: options.traceId ?? createTraceId(),
      idempotencyKey: options.idempotencyKey ?? createWorkflowId("idem"),
      scheduledAt,
      createdAt: now.toISOString(),
      metadata: options.metadata,
    };
  }
}

export function createWorkflowClient(
  config: WorkflowClientConfig,
): WorkflowClient {
  return new WorkflowClient(config);
}

function resolveScheduledAt(
  now: Date,
  options: DispatchOptions,
): string | undefined {
  if (options.scheduledAt instanceof Date) {
    return options.scheduledAt.toISOString();
  }

  if (typeof options.scheduledAt === "string") {
    return new Date(options.scheduledAt).toISOString();
  }

  if (typeof options.delayMs === "number" && options.delayMs > 0) {
    return new Date(now.getTime() + options.delayMs).toISOString();
  }

  return undefined;
}
