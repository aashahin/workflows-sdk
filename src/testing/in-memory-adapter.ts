import type {
  WorkflowAdapter,
  WorkflowEventEnvelope,
  WorkflowInstance,
} from "../core/types";

export class InMemoryWorkflowAdapter implements WorkflowAdapter {
  readonly instances = new Map<string, WorkflowInstance>();
  readonly envelopes = new Map<string, WorkflowEventEnvelope>();
  readonly cronRuns = new Set<string>();
  readonly stepResults = new Map<string, string>();

  async dispatch(envelope: WorkflowEventEnvelope): Promise<WorkflowInstance> {
    const status = envelope.scheduledAt ? "scheduled" : "queued";
    const instance: WorkflowInstance = {
      id: envelope.id,
      name: envelope.name,
      status,
      traceId: envelope.traceId,
      idempotencyKey: envelope.idempotencyKey,
      scheduledAt: envelope.scheduledAt,
      createdAt: envelope.createdAt,
      updatedAt: envelope.createdAt,
    };

    this.envelopes.set(envelope.id, envelope);
    this.instances.set(envelope.id, instance);
    return instance;
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

  async dispatchCronRun(
    runKey: string,
    envelope: WorkflowEventEnvelope,
  ): Promise<WorkflowInstance | null> {
    if (this.cronRuns.has(runKey)) return null;
    this.cronRuns.add(runKey);
    return this.dispatch(envelope);
  }

  async getInstance(id: string): Promise<WorkflowInstance | null> {
    return this.instances.get(id) ?? null;
  }

  async requeue(
    envelope: WorkflowEventEnvelope,
    options: {
      scheduledAt?: Date | string;
      error?: { name: string; message: string };
    } = {},
  ): Promise<void> {
    const scheduledAt =
      options.scheduledAt instanceof Date
        ? options.scheduledAt.toISOString()
        : options.scheduledAt
          ? new Date(options.scheduledAt).toISOString()
          : undefined;
    const instance = this.instances.get(envelope.id);
    this.envelopes.set(envelope.id, { ...envelope, scheduledAt });
    this.instances.set(envelope.id, {
      id: envelope.id,
      name: envelope.name,
      status: scheduledAt ? "scheduled" : "queued",
      traceId: envelope.traceId,
      idempotencyKey: envelope.idempotencyKey,
      scheduledAt,
      createdAt: envelope.createdAt,
      updatedAt: new Date().toISOString(),
      output: instance?.output,
      error: options.error ?? instance?.error,
    });
  }

  async claimCronRun(
    runKey: string,
    _envelope: WorkflowEventEnvelope,
  ): Promise<boolean> {
    if (this.cronRuns.has(runKey)) return false;
    this.cronRuns.add(runKey);
    return true;
  }

  async releaseCronRun(runKey: string): Promise<void> {
    this.cronRuns.delete(runKey);
  }

  setStatus(id: string, status: WorkflowInstance["status"]): void {
    const instance = this.instances.get(id);
    if (!instance) return;
    this.instances.set(id, {
      ...instance,
      status,
      updatedAt: new Date().toISOString(),
    });
  }

  async getStepResult(
    instanceId: string,
    stepName: string,
  ): Promise<unknown | undefined> {
    const raw = this.stepResults.get(stepResultKey(instanceId, stepName));
    if (!raw) return undefined;
    const parsed = JSON.parse(raw) as { hasValue?: boolean; value?: unknown };
    return parsed.hasValue === true ? parsed.value : parsed;
  }

  async hasStepResult(instanceId: string, stepName: string): Promise<boolean> {
    return this.stepResults.has(stepResultKey(instanceId, stepName));
  }

  async saveStepResult(
    instanceId: string,
    stepName: string,
    result: unknown,
  ): Promise<void> {
    this.stepResults.set(
      stepResultKey(instanceId, stepName),
      JSON.stringify({ hasValue: true, value: result }),
    );
  }
}

function stepResultKey(instanceId: string, stepName: string): string {
  return `${instanceId}:${stepName}`;
}
