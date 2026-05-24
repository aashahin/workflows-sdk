import { DEFAULT_RETRY_POLICY, withRetry } from "../helpers/retry";
import type { WorkflowRegistry } from "./registry";
import type {
  DispatchOptions,
  DispatchResult,
  RegisteredWorkflow,
  WorkflowClientConfig,
  WorkflowEventEnvelope,
  WorkflowLogger,
  WorkflowPayload,
  WorkflowRunContext,
  WorkflowStepOptions,
} from "./types";
import { WorkflowClient } from "./client";

export interface WorkflowRuntimeOptions {
  registry: WorkflowRegistry;
  client?: WorkflowClient;
  clientConfig?: WorkflowClientConfig;
  logger?: WorkflowLogger;
  sleep?: (ms: number) => Promise<void>;
  getStepResult?: (instanceId: string, stepName: string) => Promise<unknown | undefined>;
  hasStepResult?: (instanceId: string, stepName: string) => Promise<boolean>;
  saveStepResult?: (instanceId: string, stepName: string, result: unknown) => Promise<void>;
}

export async function runWorkflowEnvelope(
  envelope: WorkflowEventEnvelope,
  options: WorkflowRuntimeOptions,
): Promise<unknown> {
  const workflow = options.registry.get(envelope.name);
  const payload = options.registry.parsePayload(
    workflow as RegisteredWorkflow<WorkflowPayload>,
    envelope.payload,
  );

  const client =
    options.client ??
    (options.clientConfig ? new WorkflowClient(options.clientConfig) : undefined);

  const ctx = createRunContext(envelope, workflow, options, client);
  return workflow.run(ctx, payload);
}

export function createRunContext(
  envelope: WorkflowEventEnvelope,
  workflow: RegisteredWorkflow,
  options: WorkflowRuntimeOptions,
  client?: WorkflowClient,
): WorkflowRunContext {
  const logger = options.logger ?? consoleLogger;
  const sleep = options.sleep ?? ((ms) => new Promise((resolve) => setTimeout(resolve, ms)));

  return {
    event: envelope,
    traceId: envelope.traceId,
    idempotencyKey: envelope.idempotencyKey,
    logger,
    async step<T>(
      name: string,
      fn: () => Promise<T> | T,
      stepOptions?: WorkflowStepOptions,
    ): Promise<T> {
      const retry = stepOptions?.retry ?? workflow.retry ?? DEFAULT_RETRY_POLICY;
      const operation = async () => fn();
      logger.debug?.("workflow.step.start", { workflow: workflow.name, step: name });

      try {
        const cached = await options.getStepResult?.(envelope.id, name);
        const hasCached =
          cached !== undefined ||
          (await options.hasStepResult?.(envelope.id, name)) === true;
        if (hasCached) {
          logger.debug?.("workflow.step.cached", {
            workflow: workflow.name,
            step: name,
          });
          return cached as T;
        }

        const timeoutMs = stepOptions?.timeoutMs ?? workflow.timeoutMs;
        const run = () =>
          timeoutMs
            ? withTimeout(operation, timeoutMs)
            : operation();
        const value =
          retry === false ? await run() : await withRetry(run, retry);
        await options.saveStepResult?.(envelope.id, name, value);
        logger.debug?.("workflow.step.complete", {
          workflow: workflow.name,
          step: name,
        });
        return value;
      } catch (error) {
        logger.error?.("workflow.step.failed", {
          workflow: workflow.name,
          step: name,
          error: error instanceof Error ? error.message : String(error),
        });
        throw error;
      }
    },
    async sleep(_name: string, durationOrDate: number | Date | string): Promise<void> {
      const delayMs = durationToMs(durationOrDate);
      if (delayMs > 0) await sleep(delayMs);
    },
    dispatch<TPayload extends WorkflowPayload = WorkflowPayload>(
      name: string,
      payload: TPayload,
      dispatchOptions?: DispatchOptions,
    ): Promise<DispatchResult> {
      if (!client) {
        throw new Error("ctx.dispatch requires a workflow client");
      }
      return client.dispatch(name, payload, dispatchOptions);
    },
  };
}

export function durationToMs(durationOrDate: number | Date | string): number {
  if (typeof durationOrDate === "number") return Math.max(0, durationOrDate);

  if (durationOrDate instanceof Date) {
    return Math.max(0, durationOrDate.getTime() - Date.now());
  }

  const date = Date.parse(durationOrDate);
  if (!Number.isNaN(date)) return Math.max(0, date - Date.now());

  const match = durationOrDate
    .trim()
    .match(
      /^(\d+(?:\.\d+)?)\s*(ms|millisecond|milliseconds|s|sec|secs|second|seconds|m|min|mins|minute|minutes|h|hr|hrs|hour|hours|d|day|days|w|week|weeks|mo|month|months|y|yr|yrs|year|years)$/i,
    );
  if (!match) throw new Error(`Unsupported duration "${durationOrDate}"`);

  const value = Number(match[1]);
  const unit = match[2]?.toLowerCase();
  const multiplier = getDurationMultiplier(unit);

  return Math.max(0, value * multiplier);
}

function getDurationMultiplier(unit: string | undefined): number {
  switch (unit) {
    case "ms":
    case "millisecond":
    case "milliseconds":
      return 1;
    case "s":
    case "sec":
    case "secs":
    case "second":
    case "seconds":
      return 1_000;
    case "m":
    case "min":
    case "mins":
    case "minute":
    case "minutes":
      return 60_000;
    case "h":
    case "hr":
    case "hrs":
    case "hour":
    case "hours":
      return 3_600_000;
    case "d":
    case "day":
    case "days":
      return 86_400_000;
    case "w":
    case "week":
    case "weeks":
      return 7 * 86_400_000;
    case "mo":
    case "month":
    case "months":
      return 30 * 86_400_000;
    case "y":
    case "yr":
    case "yrs":
    case "year":
    case "years":
      return 365 * 86_400_000;
    default:
      return 86_400_000;
  }
}

function withTimeout<T>(
  operation: () => Promise<T>,
  timeoutMs: number,
): Promise<T> {
  let timeout: ReturnType<typeof setTimeout> | undefined;

  return Promise.race([
    operation(),
    new Promise<T>((_, reject) => {
      timeout = setTimeout(() => {
        reject(new Error(`Workflow step timed out after ${timeoutMs}ms`));
      }, timeoutMs);
    }),
  ]).finally(() => {
    if (timeout) clearTimeout(timeout);
  });
}

const consoleLogger: WorkflowLogger = {
  debug: (message, context) => console.debug(message, context),
  info: (message, context) => console.info(message, context),
  warn: (message, context) => console.warn(message, context),
  error: (message, context) => console.error(message, context),
};
