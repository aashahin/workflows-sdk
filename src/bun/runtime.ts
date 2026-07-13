import {
  collectDueCronRuns,
  createCronRun,
  getNextCronDate,
  getNextCronDateInTimezone,
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

export interface BunWorkflowRuntimeConfig<TServices = unknown> {
  registry: WorkflowRegistry;
  adapter: WorkflowAdapter;
  scheduler?: {
    mode: BunSchedulerMode;
    scriptPath?: string;
    titlePrefix?: string;
    tickIntervalMs?: number;
  };
  logger?: WorkflowLogger;
  services?: TServices;
  concurrency?: number;
  stalledTimeoutMs?: number;
  now?: () => Date;
}

export class BunWorkflowRuntime<TServices = unknown> {
  readonly client: WorkflowClient;
  private readonly logger: WorkflowLogger;
  private readonly now: () => Date;
  private timers: Array<{ stop?: () => void } | ReturnType<typeof setInterval>> = [];
  private stopped = true;
  private stopping = false;
  private processing = false;
  private ticking = false;
  private activeDrain: Promise<number> | null = null;

  constructor(private readonly config: BunWorkflowRuntimeConfig<TServices>) {
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
    this.stopping = false;

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
      throw new Error(
        'scheduler.mode "os" is not supported: Bun has no OS cron registration API. Use "in-process" or an external scheduler that calls runtime.scheduled().',
      );
    }

    void this.processReady().catch((error) => this.logSchedulerError(error));
  }

  async stop(): Promise<void> {
    this.stopped = true;
    this.stopping = true;
    for (const timer of this.timers) {
      if ("stop" in Object(timer) && typeof (timer as { stop?: unknown }).stop === "function") {
        (timer as { stop: () => void }).stop();
      } else {
        clearInterval(timer as ReturnType<typeof setInterval>);
      }
    }
    this.timers = [];

    // Graceful shutdown: wait for the in-flight drain to settle so callers can
    // safely process.exit() after `await stop()` without killing a running job.
    if (this.activeDrain) {
      try {
        await this.activeDrain;
      } catch (error) {
        this.logSchedulerError(error);
      }
    }
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

        // Isolate each cron run: a single dispatch failure must not abort the
        // remaining due runs in this tick (mirrors processReady's per-job
        // isolation).
        try {
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
            throw err;
          }
          claimed++;
        } catch (error) {
          const err = error instanceof Error ? error : new Error(String(error));
          this.logger.error?.("workflow.cron_dispatch_failed", {
            workflow: run.workflowName,
            runKey: run.runKey,
            error: err.message,
          });
        }
      }
    }

    return claimed;
  }

  async runEnvelope(envelope: WorkflowEventEnvelope): Promise<unknown> {
    return runWorkflowEnvelope(envelope, {
      registry: this.config.registry,
      client: this.client,
      logger: this.logger,
      services: this.config.services,
      getStepResult: this.config.adapter.getStepResult?.bind(this.config.adapter),
      hasStepResult: this.config.adapter.hasStepResult?.bind(this.config.adapter),
      saveStepResult: this.config.adapter.saveStepResult?.bind(this.config.adapter),
    });
  }

  async processReady(now = this.now()): Promise<number> {
    if (this.processing) return 0;
    this.processing = true;
    const drain = this.drainReady(now);
    this.activeDrain = drain;

    try {
      return await drain;
    } finally {
      this.processing = false;
      this.activeDrain = null;
    }
  }

  private async drainReady(now: Date): Promise<number> {
    const concurrency = Math.max(1, this.config.concurrency ?? 1);
    const stalledTimeoutMs = Math.max(
      1,
      this.config.stalledTimeoutMs ?? 5 * 60_000,
    );
    let processed = 0;

    await this.config.adapter.recoverStalled?.(
      new Date(now.getTime() - stalledTimeoutMs),
    );

    while (true) {
      // Stop claiming more work once shutdown has been requested; in-flight jobs
      // in the current batch are still awaited below.
      if (this.stopping) break;

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
            let output: unknown;
            try {
              output = await this.runEnvelope(envelope);
            } catch (error) {
              await this.handleRunFailure(envelope, error);
              return;
            }
            await this.persistCompletion(envelope, output);
          } finally {
            processed++;
          }
        }),
      );
    }

    return processed;
  }

  // Persists a successful run's completion. A failure to write the terminal
  // status is a bookkeeping error, not a workflow failure, so it must NOT feed
  // the retry/dead-letter pipeline (which would re-run an already-succeeded
  // body). Stalled recovery + step caching are the at-least-once fallback.
  private async persistCompletion(
    envelope: WorkflowEventEnvelope,
    output: unknown,
  ): Promise<void> {
    try {
      await this.config.adapter.updateInstance?.(envelope.id, "complete", { output });
    } catch {
      try {
        await this.config.adapter.updateInstance?.(envelope.id, "complete", { output });
      } catch (retryError) {
        const err =
          retryError instanceof Error ? retryError : new Error(String(retryError));
        this.logger.error?.("workflow.complete_persist_failed", {
          id: envelope.id,
          name: envelope.name,
          error: err.message,
        });
      }
    }
  }

  private async handleRunFailure(
    envelope: WorkflowEventEnvelope,
    error: unknown,
  ): Promise<void> {
    const err = error instanceof Error ? error : new Error(String(error));
    // Guard the registry lookup: an unregistered/renamed workflow name throws
    // here, so route a missing definition through the dead-letter path instead
    // of throwing a second time and aborting the whole batch.
    const workflow = this.config.registry.has(envelope.name)
      ? this.config.registry.get(envelope.name)
      : undefined;
    const retry = workflow?.retry ?? DEFAULT_RETRY_POLICY;
    const attempt = getWorkflowAttempt(envelope);

    if (
      workflow &&
      retry !== false &&
      attempt < retry.maxAttempts &&
      this.config.adapter.requeue
    ) {
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

  // Drives in-process cron via chained setTimeout timers computed from the cron
  // parser (there is no real Bun.cron API). Each fire recomputes the next
  // occurrence, honoring the cron's timezone.
  private startInProcessCron(): boolean {
    let registered = false;
    for (const workflow of this.config.registry.workflows) {
      for (const cron of normalizeCronDefinitions(workflow)) {
        this.timers.push(this.scheduleCron(cron.schedule, cron.timezone));
        registered = true;
      }
    }
    return registered;
  }

  private scheduleCron(
    schedule: string,
    timezone?: string,
  ): { stop: () => void } {
    // setTimeout delays are bounded to a signed 32-bit int; long-horizon crons
    // (monthly/yearly) are re-armed in chunks until the fire time is reached.
    const MAX_TIMEOUT = 2_147_483_647;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let stopped = false;

    const scheduleNext = (): void => {
      if (stopped || this.stopped) return;
      const from = this.now();
      const next = getNextCronDateInTimezone(schedule, from, timezone);
      const delay = Math.max(0, next.getTime() - from.getTime());
      if (delay > MAX_TIMEOUT) {
        timer = setTimeout(scheduleNext, MAX_TIMEOUT);
        return;
      }
      timer = setTimeout(() => {
        if (stopped || this.stopped) return;
        void this.tickAndProcess(this.now())
          .catch((error) => this.logSchedulerError(error))
          .finally(() => scheduleNext());
      }, delay);
    };

    scheduleNext();
    return {
      stop: () => {
        stopped = true;
        if (timer) clearTimeout(timer);
      },
    };
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

export function createBunWorkflowRuntime<TServices = unknown>(
  config: BunWorkflowRuntimeConfig<TServices>,
): BunWorkflowRuntime<TServices> {
  return new BunWorkflowRuntime(config);
}

export function createBunWorkflowScheduledHandler<TServices = unknown>(
  config: BunWorkflowRuntimeConfig<TServices>,
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
