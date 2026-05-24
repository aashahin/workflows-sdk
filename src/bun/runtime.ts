import {
  collectDueCronRuns,
  createCronRun,
  getNextCronDate,
  normalizeCronDefinitions,
} from "../scheduler/cron";
import type { WorkflowRegistry } from "../core/registry";
import { WorkflowClient } from "../core/client";
import { createWorkflowId } from "../core/id";
import { runWorkflowEnvelope } from "../core/runtime";
import { DEFAULT_RETRY_POLICY, getBackoffDelay } from "../helpers/retry";
import type {
  WorkflowAdapter,
  WorkflowEventEnvelope,
  WorkflowLogger,
} from "../core/types";

export type BunSchedulerMode = "in-process" | "os" | "redis" | "external";

export interface BunWorkflowRuntimeConfig {
  registry: WorkflowRegistry;
  adapter: WorkflowAdapter;
  scheduler?: {
    mode: BunSchedulerMode;
    scriptPath?: string;
    titlePrefix?: string;
    tickIntervalMs?: number;
  };
  logger?: WorkflowLogger;
  concurrency?: number;
  stalledTimeoutMs?: number;
  now?: () => Date;
}

export class BunWorkflowRuntime {
  readonly client: WorkflowClient;
  private readonly logger: WorkflowLogger;
  private readonly now: () => Date;
  private timers: Array<{ stop?: () => void } | ReturnType<typeof setInterval>> = [];
  private stopped = true;
  private processing = false;
  private ticking = false;

  constructor(private readonly config: BunWorkflowRuntimeConfig) {
    this.client = new WorkflowClient({
      adapter: config.adapter,
      logger: config.logger,
      now: config.now,
    });
    this.logger = config.logger ?? console;
    this.now = config.now ?? (() => new Date());
  }

  start(): void {
    if (!this.stopped) return;
    this.stopped = false;

    const mode = this.config.scheduler?.mode ?? "external";
    if (mode === "in-process" || mode === "redis") {
      const cronRegistered = this.startInProcessCron();
      const interval = setInterval(
        () => {
          const work = cronRegistered ? this.processReady() : this.tickAndProcess();
          void work.catch((error) => this.logSchedulerError(error));
        },
        this.config.scheduler?.tickIntervalMs ?? 30_000,
      );
      this.timers.push(interval);
    }

    if (mode === "os") {
      void this.registerOsCron().catch((error) => this.logSchedulerError(error));
    }

    void this.processReady().catch((error) => this.logSchedulerError(error));
  }

  async stop(): Promise<void> {
    this.stopped = true;
    for (const timer of this.timers) {
      if ("stop" in Object(timer) && typeof (timer as { stop?: unknown }).stop === "function") {
        (timer as { stop: () => void }).stop();
      } else {
        clearInterval(timer as ReturnType<typeof setInterval>);
      }
    }
    this.timers = [];
  }

  async tick(now = this.now()): Promise<number> {
    return this.tickWithOptions(now);
  }

  private async tickWithOptions(
    now = this.now(),
    options: { defaultTimezone?: string; schedule?: string } = {},
  ): Promise<number> {
    let claimed = 0;

    for (const workflow of this.config.registry.workflows) {
      const runs = options.schedule
        ? normalizeCronDefinitions(workflow)
            .filter((cron) => cron.schedule === options.schedule)
            .map((cron) => createCronRun(workflow, cron, now))
        : collectDueCronRuns(workflow, now, options.defaultTimezone);

      for (const run of runs) {
        const envelope: WorkflowEventEnvelope = {
          id: createWorkflowId("cron"),
          name: run.workflowName,
          payload: run.payload,
          traceId: createWorkflowId("trace"),
          idempotencyKey: run.runKey,
          scheduledAt: run.scheduledAt.toISOString(),
          createdAt: now.toISOString(),
          metadata: run.metadata,
        };

        if (this.config.adapter.dispatchCronRun) {
          const instance = await this.config.adapter.dispatchCronRun(run.runKey, envelope);
          if (!instance) continue;
          claimed++;
          continue;
        }

        const reserved =
          (await this.config.adapter.claimCronRun?.(run.runKey, envelope)) ??
          true;
        if (!reserved) continue;

        try {
          await this.client.dispatch(run.workflowName, run.payload, {
            id: envelope.id,
            idempotencyKey: run.runKey,
            scheduledAt: run.scheduledAt,
            traceId: envelope.traceId,
            metadata: run.metadata,
          });
        } catch (error) {
          const err = error instanceof Error ? error : new Error(String(error));
          await this.config.adapter.releaseCronRun?.(run.runKey, envelope, err);
          throw error;
        }
        claimed++;
      }
    }

    return claimed;
  }

  async runEnvelope(envelope: WorkflowEventEnvelope): Promise<unknown> {
    return runWorkflowEnvelope(envelope, {
      registry: this.config.registry,
      client: this.client,
      logger: this.logger,
      getStepResult: this.config.adapter.getStepResult?.bind(this.config.adapter),
      hasStepResult: this.config.adapter.hasStepResult?.bind(this.config.adapter),
      saveStepResult: this.config.adapter.saveStepResult?.bind(this.config.adapter),
    });
  }

  async processReady(now = this.now()): Promise<number> {
    if (this.processing) return 0;
    this.processing = true;

    const concurrency = Math.max(1, this.config.concurrency ?? 1);
    const stalledTimeoutMs = Math.max(
      1,
      this.config.stalledTimeoutMs ?? 5 * 60_000,
    );
    let processed = 0;

    try {
      await this.config.adapter.recoverStalled?.(
        new Date(now.getTime() - stalledTimeoutMs),
      );

      while (true) {
        const batch = await Promise.all(
          Array.from({ length: concurrency }, () =>
            this.config.adapter.claimNext?.(now) ?? Promise.resolve(null),
          ),
        );
        const envelopes = batch.filter(
          (item): item is WorkflowEventEnvelope => item !== null,
        );
        if (envelopes.length === 0) break;

        await Promise.all(
          envelopes.map(async (envelope) => {
            try {
              const output = await this.runEnvelope(envelope);
              await this.config.adapter.updateInstance?.(envelope.id, "complete", {
                output,
              });
            } catch (error) {
              const err = error instanceof Error ? error : new Error(String(error));
              const retry = this.config.registry.get(envelope.name).retry ?? DEFAULT_RETRY_POLICY;
              const attempt = getWorkflowAttempt(envelope);

              if (retry !== false && attempt < retry.maxAttempts && this.config.adapter.requeue) {
                const nextEnvelope = withWorkflowAttempt(envelope, attempt + 1);
                const scheduledAt = new Date(
                  this.now().getTime() + getBackoffDelay(attempt, retry),
                );
                await this.config.adapter.requeue(nextEnvelope, {
                  scheduledAt,
                  error: { name: err.name, message: err.message },
                });
                this.logger.warn?.("workflow.requeued", {
                  id: envelope.id,
                  name: envelope.name,
                  attempt: attempt + 1,
                  scheduledAt: scheduledAt.toISOString(),
                  error: err.message,
                });
              } else {
                await this.config.adapter.updateInstance?.(envelope.id, "dead", {
                  error: { name: err.name, message: err.message },
                });
                await this.config.adapter.recordDeadLetter?.(envelope, err);
                this.logger.error?.("workflow.dead", {
                  id: envelope.id,
                  name: envelope.name,
                  error: err.message,
                });
              }
            } finally {
              processed++;
            }
          }),
        );
      }
    } finally {
      this.processing = false;
    }

    return processed;
  }

  async scheduled(controller?: { cron?: string; scheduledTime?: number }): Promise<{
    claimed: number;
    processed: number;
  }> {
    const scheduledTime =
      typeof controller?.scheduledTime === "number"
        ? new Date(controller.scheduledTime)
        : this.now();
    const defaultTimezone =
      this.config.scheduler?.mode === "os" || controller?.cron
        ? getLocalTimeZone()
        : undefined;
    const claimed = await this.tickWithOptions(scheduledTime, {
      defaultTimezone,
      schedule: controller?.cron,
    });
    const processed = await this.processReady(this.now());
    return { claimed, processed };
  }

  private startInProcessCron(): boolean {
    const bun = (globalThis as { Bun?: { cron?: (...args: unknown[]) => { stop?: () => void } } }).Bun;
    if (!bun?.cron) {
      this.logger.warn?.("Bun.cron unavailable; falling back to interval tick");
      return false;
    }

    let registered = false;
    for (const workflow of this.config.registry.workflows) {
      for (const cron of normalizeCronDefinitions(workflow)) {
        const handle = bun.cron(cron.schedule, async () => {
          await this.tickAndProcess(new Date());
        });
        this.timers.push(handle);
        registered = true;
      }
    }
    return registered;
  }

  private async registerOsCron(): Promise<void> {
    const bun = (globalThis as { Bun?: { cron?: (...args: unknown[]) => unknown } }).Bun;
    const scriptPath = this.config.scheduler?.scriptPath;

    if (!bun?.cron || !scriptPath) {
      this.logger.warn?.(
        "OS cron registration requires Bun.cron and scheduler.scriptPath",
      );
      return;
    }

    for (const workflow of this.config.registry.workflows) {
      for (const cron of normalizeCronDefinitions(workflow)) {
        const title = sanitizeCronTitle(
          `${this.config.scheduler?.titlePrefix ?? "workflows"}_${workflow.name}_${cron.name}`,
        );
        await bun.cron(scriptPath, cron.schedule, title);
      }
    }
  }

  nextCronDates(from = this.now()): Array<{
    workflowName: string;
    cronName: string;
    nextAt: Date;
  }> {
    return this.config.registry.workflows.flatMap((workflow) =>
      normalizeCronDefinitions(workflow).map((cron) => ({
        workflowName: workflow.name,
        cronName: cron.name ?? workflow.name,
        nextAt: getNextCronDate(cron.schedule, from),
      })),
    );
  }

  private async tickAndProcess(now = this.now()): Promise<{
    claimed: number;
    processed: number;
  }> {
    if (this.ticking) return { claimed: 0, processed: 0 };
    this.ticking = true;

    try {
      const claimed = await this.tick(now);
      const processed = await this.processReady(this.now());
      return { claimed, processed };
    } finally {
      this.ticking = false;
    }
  }

  private logSchedulerError(error: unknown): void {
    this.logger.error?.("workflow.scheduler.failed", {
      error: error instanceof Error ? error.message : String(error),
    });
  }
}

const WORKFLOW_ATTEMPT_METADATA_KEY = "__workflowSdkAttempt";

function getWorkflowAttempt(envelope: WorkflowEventEnvelope): number {
  const value = envelope.metadata?.[WORKFLOW_ATTEMPT_METADATA_KEY];
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

function withWorkflowAttempt(
  envelope: WorkflowEventEnvelope,
  attempt: number,
): WorkflowEventEnvelope {
  return {
    ...envelope,
    metadata: {
      ...envelope.metadata,
      [WORKFLOW_ATTEMPT_METADATA_KEY]: attempt,
    },
  };
}

function getLocalTimeZone(): string {
  return Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC";
}

function sanitizeCronTitle(title: string): string {
  return title.replace(/[^a-zA-Z0-9_-]/g, "_").slice(0, 120);
}

export function createBunWorkflowRuntime(
  config: BunWorkflowRuntimeConfig,
): BunWorkflowRuntime {
  return new BunWorkflowRuntime(config);
}

export function createBunWorkflowScheduledHandler(
  config: BunWorkflowRuntimeConfig,
): {
  scheduled(controller?: { cron?: string; scheduledTime?: number }): Promise<{
    claimed: number;
    processed: number;
  }>;
} {
  const runtime = createBunWorkflowRuntime(config);
  return {
    scheduled(controller) {
      return runtime.scheduled(controller);
    },
  };
}
