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

export interface WorkflowRuntimeOptions<TServices = unknown> {
  registry: WorkflowRegistry;
  client?: WorkflowClient;
  clientConfig?: WorkflowClientConfig;
  logger?: WorkflowLogger;
  services?: TServices;
  sleep?: (ms: number) => Promise<void>;
  getStepResult?: (instanceId: string, stepName: string) => Promise<unknown | undefined>;
  hasStepResult?: (instanceId: string, stepName: string) => Promise<boolean>;
  saveStepResult?: (instanceId: string, stepName: string, result: unknown) => Promise<void>;
}

export async function runWorkflowEnvelope<TServices = unknown>(
  envelope: WorkflowEventEnvelope,
  options: WorkflowRuntimeOptions<TServices>,
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

export function createRunContext<TServices = unknown>(
  envelope: WorkflowEventEnvelope,
  workflow: RegisteredWorkflow<WorkflowPayload, unknown, TServices>,
  options: WorkflowRuntimeOptions<TServices>,
  client?: WorkflowClient,
): WorkflowRunContext<TServices> {
  const logger = options.logger ?? consoleLogger;
  const sleep = options.sleep ?? ((ms) => new Promise((resolve) => setTimeout(resolve, ms)));
  // Derive the dispatch client the same way runWorkflowEnvelope does, so direct
  // callers of createRunContext get a working ctx.dispatch() from clientConfig.
  const resolvedClient =
    client ??
    options.client ??
    (options.clientConfig ? new WorkflowClient(options.clientConfig) : undefined);

  return {
    event: envelope,
    traceId: envelope.traceId,
    idempotencyKey: envelope.idempotencyKey,
    logger,
    services: options.services as TServices,
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
      if (!resolvedClient) {
        throw new Error("ctx.dispatch requires a workflow client");
      }
      return resolvedClient.dispatch(name, payload, dispatchOptions);
    },
  };
}

export function durationToMs(durationOrDate: number | Date | string): number {
  if (typeof durationOrDate === "number") return Math.max(0, durationOrDate);

  if (durationOrDate instanceof Date) {
    return Math.max(0, durationOrDate.getTime() - Date.now());
  }

  const trimmed = durationOrDate.trim();

  // 1) Unit-suffixed duration string ("5s", "10 minutes").
  const match = trimmed.match(
    /^(\d+(?:\.\d+)?)\s*(ms|millisecond|milliseconds|s|sec|secs|second|seconds|m|min|mins|minute|minutes|h|hr|hrs|hour|hours|d|day|days|w|week|weeks|mo|month|months|y|yr|yrs|year|years)$/i,
  );
  if (match) {
    const value = Number(match[1]);
    const unit = match[2]?.toLowerCase();
    const multiplier = getDurationMultiplier(unit);
    return Math.max(0, value * multiplier);
  }

  // 2) Bare numeric string ("500") is treated as milliseconds, matching the
  // numeric overload — never routed to Date.parse (which would misread it as a
  // calendar year and silently collapse the sleep to zero).
  if (/^\d+(?:\.\d+)?$/.test(trimmed)) {
    return Math.max(0, Number(trimmed));
  }

  // 3) Only clearly date-like strings (those carrying a date/time separator)
  // fall through to Date.parse. Date.parse is permissive enough to coerce cron
  // expressions and other free text into a (usually past) calendar date, so we
  // gate it behind a separator check and otherwise reject the input outright.
  if (/[-/:]/.test(trimmed)) {
    const date = Date.parse(trimmed);
    if (!Number.isNaN(date)) return Math.max(0, date - Date.now());
  }

  throw new Error(`Unsupported duration "${durationOrDate}"`);
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

/**
 * Race an operation against a timeout. A JavaScript promise cannot be
 * cancelled, so the underlying operation keeps running after a timeout fires.
 * We track a single `settled` flag so that only the winning branch is ever
 * observed: a timed-out attempt that completes (or rejects) late is swallowed
 * and can neither surface as an unhandled rejection nor clobber the result of
 * the retry that replaced it. (See WorkflowStepOptions.timeoutMs for the
 * concurrent-execution hazard this does NOT solve.)
 */
function withTimeout<T>(
  operation: () => Promise<T>,
  timeoutMs: number,
): Promise<T> {
  let timeout: ReturnType<typeof setTimeout> | undefined;
  let settled = false;

  const attempt = Promise.resolve()
    .then(operation)
    .then(
      (value) => {
        if (settled) return undefined as never; // lost the race — drop the value
        settled = true;
        return value;
      },
      (error) => {
        if (settled) return undefined as never; // lost the race — swallow late failure
        settled = true;
        throw error;
      },
    );

  const timer = new Promise<T>((_, reject) => {
    timeout = setTimeout(() => {
      if (settled) return;
      settled = true;
      reject(new Error(`Workflow step timed out after ${timeoutMs}ms`));
    }, timeoutMs);
  });

  return Promise.race([attempt, timer]).finally(() => {
    if (timeout) clearTimeout(timeout);
  });
}

const consoleLogger: WorkflowLogger = {
  debug: (message, context) => console.debug(message, context),
  info: (message, context) => console.info(message, context),
  warn: (message, context) => console.warn(message, context),
  error: (message, context) => console.error(message, context),
};
