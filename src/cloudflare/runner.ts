import { DEFAULT_RETRY_POLICY } from "../helpers/retry";
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
      event: { payload: WorkflowEventEnvelope },
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

      const workflow = registry.get(event.payload.name);
      const payload = registry.parsePayload(
        workflow,
        event.payload.payload,
      );
      await sleepUntilScheduledAt(event.payload, step);

      return workflow.run(
        {
          event: event.payload,
          traceId: event.payload.traceId,
          idempotencyKey: event.payload.idempotencyKey,
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
            return stepConfig ? step.do(name, stepConfig, fn) : step.do(name, fn);
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
            if (config.dispatch) {
              return config.dispatch(name, payload, options, env);
            }
            throw new Error(
              "ctx.dispatch requires CloudflareRunnerConfig.dispatch.",
            );
          },
        },
        payload,
      );
    }
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
    config.retries = {
      limit: options.retry.maxAttempts + 1,
      delay: options.retry.initialIntervalMs,
      backoff: options.retry.multiplier > 1 ? "exponential" : "constant",
    };
  }

  return config;
}

function toCloudflareDuration(durationOrDate: number | Date | string): string {
  if (typeof durationOrDate === "string") return durationOrDate;

  const ms =
    durationOrDate instanceof Date
      ? Math.max(0, durationOrDate.getTime() - Date.now())
      : Math.max(0, durationOrDate);

  if (ms < 1_000) return `${Math.max(1, Math.ceil(ms))} milliseconds`;
  if (ms < 60_000) return `${Math.ceil(ms / 1_000)} seconds`;
  if (ms < 3_600_000) return `${Math.ceil(ms / 60_000)} minutes`;
  if (ms < 86_400_000) return `${Math.ceil(ms / 3_600_000)} hours`;
  return `${Math.ceil(ms / 86_400_000)} days`;
}
