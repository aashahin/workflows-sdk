import { DEFAULT_RETRY_POLICY } from "../helpers/retry";
import { createWorkflowId } from "../core/id";
import type { WorkflowRegistry } from "../core/registry";
import type {
  DispatchOptions,
  DispatchResult,
  WorkflowEventEnvelope,
  WorkflowLogger,
  WorkflowPayload,
  WorkflowStepOptions,
} from "../core/types";

export interface CloudflareRunnerConfig<TEnv = unknown, TServices = unknown> {
  registry: WorkflowRegistry | ((env: TEnv) => WorkflowRegistry);
  logger?: WorkflowLogger | ((env: TEnv) => WorkflowLogger);
  services?: TServices | ((env: TEnv) => TServices);
  dispatch?<TPayload extends WorkflowPayload = WorkflowPayload>(
    name: string,
    payload: TPayload,
    options: DispatchOptions | undefined,
    env: TEnv,
  ): Promise<DispatchResult>;
}

type WorkflowEntrypointConstructor = abstract new (...args: any[]) => object;

export function createCloudflareWorkflowEntrypoint<TEnv = unknown, TServices = unknown>(
  Base: WorkflowEntrypointConstructor,
  config: CloudflareRunnerConfig<TEnv, TServices>,
) {
  return class GenericCloudflareWorkflowEntrypoint extends Base {
    async run(
      event: {
        payload: WorkflowEventEnvelope | LegacyWorkflowPayload;
        // Cloudflare supplies a stable per-instance id and the enqueue timestamp
        // on the WorkflowEvent; both are used to keep the legacy normalization
        // path deterministic across replays.
        instanceId?: string;
        timestamp?: Date | number;
      },
      step: {
        do<T>(
          name: string,
          optionsOrFn: unknown,
          fn?: () => Promise<T> | T,
        ): Promise<T>;
        sleep(name: string, duration: string | number): Promise<void>;
        sleepUntil?(name: string, timestamp: Date | number): Promise<void>;
      },
    ): Promise<unknown> {
      const env = (this as unknown as { env: TEnv }).env;
      const logger =
        typeof config.logger === "function"
          ? config.logger(env)
          : config.logger;
      const registry =
        typeof config.registry === "function"
          ? config.registry(env)
          : config.registry;
      const services =
        typeof config.services === "function"
          ? (config.services as (env: TEnv) => TServices)(env)
          : config.services;

      const envelope = normalizeWorkflowEnvelope(event);
      const workflow = registry.get(envelope.name);
      const payload = registry.parsePayload(
        workflow,
        envelope.payload,
      );
      await sleepUntilScheduledAt(envelope, step);

      // Counter used to give each nested ctx.dispatch a stable, unique step name
      // so its result is memoized across replays without colliding with sibling
      // dispatches to the same workflow name.
      let dispatchCount = 0;

      return workflow.run(
        {
          event: envelope,
          traceId: envelope.traceId,
          idempotencyKey: envelope.idempotencyKey,
          logger: logger ?? console,
          services,
          step<T>(
            name: string,
            fn: () => Promise<T> | T,
            options?: WorkflowStepOptions,
          ): Promise<T> {
            const stepConfig = toCloudflareStepConfig({
              retry: options?.retry ?? workflow.retry ?? DEFAULT_RETRY_POLICY,
              timeoutMs: options?.timeoutMs ?? workflow.timeoutMs,
            });
            // Translate SDK-convention non-retryable markers into Cloudflare's
            // native NonRetryableError so its retry engine stops early, matching
            // the fail-fast behaviour of withRetry() on Bun.
            const wrapped = wrapNonRetryable(fn);
            return stepConfig
              ? step.do(name, stepConfig, wrapped)
              : step.do(name, wrapped);
          },
          async sleep(
            name: string,
            durationOrDate: number | Date | string,
          ): Promise<void> {
            await step.sleep(name, toCloudflareDuration(durationOrDate));
          },
          dispatch<TPayload extends WorkflowPayload = WorkflowPayload>(
            name: string,
            payload: TPayload,
            options?: DispatchOptions,
          ): Promise<DispatchResult> {
            const dispatchFn = config.dispatch;
            if (!dispatchFn) {
              throw new Error(
                "ctx.dispatch requires CloudflareRunnerConfig.dispatch.",
              );
            }
            // Wrap the dispatch in step.do so the created instance (and its
            // generated id) is memoized. Cloudflare replays run() from the top
            // after a sleep, a retried step, or crash recovery; an unwrapped
            // dispatch would re-fire and start a duplicate workflow instance.
            // DispatchResult is JSON-serializable, so it survives step storage.
            return step.do(`dispatch:${name}:${dispatchCount++}`, () =>
              dispatchFn(name, payload, options, env),
            );
          },
        },
        payload,
      );
    }
  };
}

export interface LegacyWorkflowPayload {
  eventId?: string;
  idempotencyKey?: string;
  traceId?: string;
  eventName?: string;
  data?: WorkflowPayload;
  delayMs?: number;
}

function normalizeWorkflowEnvelope(event: {
  payload: WorkflowEventEnvelope | LegacyWorkflowPayload;
  instanceId?: string;
  timestamp?: Date | number;
}): WorkflowEventEnvelope {
  const payload = event.payload;
  if ("name" in payload && "payload" in payload) return payload;

  // Legacy path: run() re-executes on every Cloudflare replay, so any generated
  // id/timestamp must be derived from stable inputs (the CF instance id and
  // enqueue timestamp) rather than fresh randomness/`Date.now()`. Only when the
  // platform provides neither do we fall back to a random id.
  const base =
    event.timestamp !== undefined ? new Date(event.timestamp) : new Date();
  const id = payload.eventId ?? event.instanceId ?? createWorkflowId();
  const delayMs =
    typeof payload.delayMs === "number" && payload.delayMs > 0
      ? payload.delayMs
      : undefined;

  return {
    id,
    name: payload.eventName ?? "",
    payload: payload.data ?? {},
    traceId: payload.traceId ?? `trace_${id}`,
    idempotencyKey: payload.idempotencyKey ?? id,
    scheduledAt: delayMs
      ? new Date(base.getTime() + delayMs).toISOString()
      : undefined,
    createdAt: base.toISOString(),
  };
}

async function sleepUntilScheduledAt(
  envelope: WorkflowEventEnvelope,
  step: {
    sleep(name: string, duration: string | number): Promise<void>;
    sleepUntil?(name: string, timestamp: Date | number): Promise<void>;
  },
): Promise<void> {
  if (!envelope.scheduledAt) return;

  const scheduledAt = Date.parse(envelope.scheduledAt);
  if (Number.isNaN(scheduledAt) || scheduledAt <= Date.now()) return;

  if (step.sleepUntil) {
    await step.sleepUntil("sdk scheduledAt", scheduledAt);
    return;
  }

  await step.sleep("sdk scheduledAt", toCloudflareDuration(scheduledAt - Date.now()));
}

function toCloudflareStepConfig(
  options?: WorkflowStepOptions,
): Record<string, unknown> | null {
  if (options?.retry === undefined && !options?.timeoutMs) return null;

  const config: Record<string, unknown> = {};
  if (options.timeoutMs) {
    config.timeout = options.timeoutMs;
  }
  if (options.retry === false) {
    config.retries = { limit: 1, delay: 0, backoff: "constant" };
  } else if (options.retry) {
    const { maxAttempts, initialIntervalMs, multiplier, maxIntervalMs } =
      options.retry;
    const limit = maxAttempts + 1;
    const uncappedMaxDelay =
      initialIntervalMs * multiplier ** Math.max(0, limit - 1);

    if (multiplier > 1 && uncappedMaxDelay > maxIntervalMs) {
      // Cloudflare's static retries config has no max-delay field, so
      // exponential growth would be uncapped for the full limit — unlike Bun,
      // where getBackoffDelay() applies Math.min(delay, maxIntervalMs). Supply a
      // delay *function* that reproduces that ceiling. When `delay` is a
      // function Cloudflare ignores the `backoff` field, so it is omitted.
      config.retries = {
        limit,
        delay: ({ ctx }: { ctx: { attempt: number } }) =>
          Math.min(
            initialIntervalMs * multiplier ** Math.max(0, ctx.attempt - 1),
            maxIntervalMs,
          ),
      };
    } else {
      config.retries = {
        limit,
        delay: initialIntervalMs,
        backoff: multiplier > 1 ? "exponential" : "constant",
      };
    }
  }

  return config;
}

function toCloudflareDuration(durationOrDate: number | Date | string): string | number {
  if (typeof durationOrDate === "string") return durationOrDate;

  // Cloudflare's step.sleep accepts a plain number of milliseconds directly. Its
  // string duration grammar has no millisecond unit, so building a string is
  // both unnecessary and wrong for sub-second durations — always pass the number.
  return durationOrDate instanceof Date
    ? Math.max(0, durationOrDate.getTime() - Date.now())
    : Math.max(0, durationOrDate);
}

type NonRetryableErrorConstructor = new (message: string) => Error;

// `cloudflare:workflows` only exists inside the Workers runtime; importing it
// eagerly (or by a literal specifier) would break the SDK's dependency-free
// promise on Bun/Node. Resolve it lazily via an indirected specifier and cache
// the (possibly failed) lookup so translation is best-effort.
const CLOUDFLARE_WORKFLOWS_MODULE: string = "cloudflare:workflows";
let cloudflareNonRetryableErrorPromise:
  | Promise<NonRetryableErrorConstructor | null>
  | undefined;

function loadCloudflareNonRetryableError(): Promise<NonRetryableErrorConstructor | null> {
  if (!cloudflareNonRetryableErrorPromise) {
    cloudflareNonRetryableErrorPromise = import(CLOUDFLARE_WORKFLOWS_MODULE)
      .then(
        (module: { NonRetryableError?: NonRetryableErrorConstructor }) =>
          module.NonRetryableError ?? null,
      )
      .catch(() => null);
  }
  return cloudflareNonRetryableErrorPromise;
}

function isSdkNonRetryableError(error: unknown): boolean {
  if (typeof error !== "object" || error === null) return false;
  const candidate = error as { nonRetryable?: unknown; name?: unknown };
  return candidate.nonRetryable === true || candidate.name === "NonRetryableError";
}

function wrapNonRetryable<T>(fn: () => Promise<T> | T): () => Promise<T> {
  return async () => {
    try {
      return await fn();
    } catch (error) {
      if (isSdkNonRetryableError(error)) {
        const NonRetryableError = await loadCloudflareNonRetryableError();
        if (NonRetryableError) {
          const message = error instanceof Error ? error.message : String(error);
          throw new NonRetryableError(message);
        }
      }
      throw error;
    }
  };
}
