import { DEFAULT_RETRY_POLICY } from "../helpers/retry";
import { createTraceId, createWorkflowId } from "../core/id";
import { durationToMs } from "../core/runtime";
import type { WorkflowRegistry } from "../core/registry";
import type {
  DispatchOptions,
  DispatchResult,
  WorkflowEventEnvelope,
  WorkflowLogger,
  WorkflowPayload,
  WorkflowStepOptions,
} from "../core/types";
import {
  MAX_CLOUDFLARE_SCHEDULE_AHEAD_MS,
  assertCloudflareJsonSerializable,
  assertCloudflareWorkflowEnvelope,
} from "./serialization";

const CHILD_DISPATCH_STEP_OPTIONS: WorkflowStepOptions = {
  retry: {
    maxAttempts: 1,
    initialIntervalMs: 1_000,
    multiplier: 1,
    maxIntervalMs: 1_000,
  },
  timeoutMs: 30_000,
};

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
      event: { payload: WorkflowEventEnvelope | LegacyWorkflowPayload },
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

      const envelope = normalizeWorkflowEnvelope(event.payload);
      assertCloudflareWorkflowEnvelope(envelope);
      const workflow = registry.get(envelope.name);
      const payload = registry.parsePayload(
        workflow,
        envelope.payload,
      );
      registry.validateEvent?.(workflow, { ...envelope, payload });
      await sleepUntilScheduledAt(envelope, step);
      const unnamedChildTargets = new Set<string>();

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
            return stepConfig ? step.do(name, stepConfig, fn) : step.do(name, fn);
          },
          async sleep(
            name: string,
            durationOrDate: number | Date | string,
          ): Promise<void> {
            const absoluteTimestamp = parseAbsoluteSleepTimestamp(durationOrDate);
            if (absoluteTimestamp !== null) {
              if (!step.sleepUntil) {
                throw new Error("Cloudflare Workflow runtime requires step.sleepUntil for absolute dates");
              }
              const workflowCreatedAt = Date.parse(envelope.createdAt);
              if (
                absoluteTimestamp - workflowCreatedAt >
                MAX_CLOUDFLARE_SCHEDULE_AHEAD_MS
              ) {
                throw new Error("Cloudflare Workflow sleep exceeds 365 days");
              }
              await step.sleepUntil(name, absoluteTimestamp);
              return;
            }
            const duration = toCloudflareDuration(durationOrDate);
            if (duration <= 0) return;
            await step.sleep(name, duration);
          },
          async dispatch<TPayload extends WorkflowPayload = WorkflowPayload>(
            name: string,
            payload: TPayload,
            options?: DispatchOptions,
          ): Promise<DispatchResult> {
            if (!config.dispatch) {
              throw new Error(
                "ctx.dispatch requires CloudflareRunnerConfig.dispatch.",
              );
            }

            assertCloudflareJsonSerializable(
              payload,
              `Child Workflow payload for ${name}`,
            );
            const plan = await createChildDispatchPlan(
              envelope,
              name,
              options,
              unnamedChildTargets,
            );
            const stepConfig = toCloudflareStepConfig(
              CHILD_DISPATCH_STEP_OPTIONS,
            );

            return step.do(
              plan.stepName,
              stepConfig,
              () => config.dispatch!(name, payload, plan.options, env),
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

function normalizeWorkflowEnvelope(payload: unknown): WorkflowEventEnvelope {
  if (!isRecord(payload)) {
    throw new Error("Workflow event payload must be an object");
  }
  if ("name" in payload && "payload" in payload) {
    assertCloudflareWorkflowEnvelope(payload);
    return payload;
  }

  const now = new Date();
  const delayMs =
    typeof payload.delayMs === "number" && payload.delayMs > 0
      ? payload.delayMs
      : undefined;

  const envelope: WorkflowEventEnvelope = {
    id: stringOrUndefined(payload.eventId) ?? createWorkflowId(),
    name: stringOrUndefined(payload.eventName) ?? "",
    payload: isRecord(payload.data) ? payload.data : {},
    traceId: stringOrUndefined(payload.traceId) ?? createTraceId(),
    idempotencyKey:
      stringOrUndefined(payload.idempotencyKey) ?? createWorkflowId("idem"),
    ...(delayMs
      ? { scheduledAt: new Date(now.getTime() + delayMs).toISOString() }
      : {}),
    createdAt: now.toISOString(),
  };
  return envelope;
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
  if (Number.isNaN(scheduledAt)) {
    throw new Error("Cloudflare Workflow scheduledAt is invalid");
  }

  if (step.sleepUntil) {
    await step.sleepUntil("sdk scheduledAt", scheduledAt);
    return;
  }

  throw new Error("Cloudflare Workflow runtime requires step.sleepUntil");
}

function toCloudflareStepConfig(
  options?: WorkflowStepOptions,
): CloudflareStepConfig | null {
  if (options?.retry === undefined && options?.timeoutMs === undefined) {
    return null;
  }

  const config: CloudflareStepConfig = {};
  if (options?.timeoutMs !== undefined) {
    if (
      !Number.isSafeInteger(options.timeoutMs) ||
      options.timeoutMs <= 0
    ) {
      throw new Error(
        "Cloudflare Workflow timeoutMs must be a positive integer",
      );
    }
    config.timeout = options.timeoutMs;
  }
  if (options.retry === false) {
    // Cloudflare's limit is total attempts, including the first execution.
    config.retries = { limit: 1, delay: 0, backoff: "constant" };
  } else if (options.retry) {
    if (
      !Number.isSafeInteger(options.retry.maxAttempts) ||
      options.retry.maxAttempts < 0 ||
      options.retry.maxAttempts > 9_999
    ) {
      throw new Error(
        "Cloudflare Workflow retry maxAttempts must be an integer from 0 to 9999",
      );
    }
    if (
      !Number.isSafeInteger(options.retry.initialIntervalMs) ||
      options.retry.initialIntervalMs < 0 ||
      !Number.isFinite(options.retry.multiplier) ||
      options.retry.multiplier < 1 ||
      !Number.isSafeInteger(options.retry.maxIntervalMs) ||
      options.retry.maxIntervalMs < 0
    ) {
      throw new Error(
        "Cloudflare Workflow retry delays require non-negative integer intervals and a multiplier of at least 1",
      );
    }
    const retry = options.retry;
    config.retries = {
      // SDK maxAttempts means retries after the initial execution; Cloudflare
      // counts that initial execution in retries.limit.
      limit: retry.maxAttempts + 1,
      // Cloudflare's built-in exponential multiplier and uncapped delay do not
      // preserve the SDK policy. A dynamic delay keeps both the configured
      // multiplier and maxIntervalMs exact.
      delay: ({ ctx }) =>
        Math.min(
          retry.initialIntervalMs *
            Math.pow(retry.multiplier, Math.max(0, ctx.attempt - 1)),
          retry.maxIntervalMs,
        ),
    };
  }

  return config;
}

function toCloudflareDuration(
  durationOrDate: number | Date | string,
): number {
  const ms = durationToMs(durationOrDate);
  if (!Number.isFinite(ms)) {
    throw new Error("Cloudflare Workflow sleep duration must be finite");
  }
  const rounded = Math.max(0, Math.ceil(ms));
  if (rounded > 365 * 86_400_000) {
    throw new Error("Cloudflare Workflow sleep duration exceeds 365 days");
  }
  // Cloudflare accepts numeric milliseconds. Using one representation avoids
  // passing SDK abbreviations such as `250ms`/`5s` into its stricter grammar.
  return rounded;
}

/**
 * Preserve absolute dates as absolute durable timers. Converting one through
 * `durationToMs()` subtracts `Date.now()`, so a Workflow replay would produce a
 * different duration and therefore different durable control flow.
 */
function parseAbsoluteSleepTimestamp(
  durationOrDate: number | Date | string,
): number | null {
  if (typeof durationOrDate === "number") return null;
  const timestamp =
    durationOrDate instanceof Date
      ? durationOrDate.getTime()
      : Date.parse(durationOrDate);
  if (Number.isNaN(timestamp)) return null;
  if (!Number.isFinite(timestamp)) {
    throw new Error("Cloudflare Workflow sleep date is invalid");
  }
  return timestamp;
}

interface CloudflareStepConfig {
  retries?: {
    limit: number;
    delay:
      | string
      | number
      | ((input: {
          ctx: { attempt: number };
          error: Error;
        }) => string | number | Promise<string | number>);
    backoff?: "constant" | "linear" | "exponential";
  };
  timeout?: number;
}

async function createChildDispatchPlan(
  parent: WorkflowEventEnvelope,
  childName: string,
  options: DispatchOptions | undefined,
  unnamedChildTargets: Set<string>,
): Promise<{ stepName: string; options: DispatchOptions }> {
  if (!childName.trim()) {
    throw new Error("Child Workflow name must not be empty");
  }

  const childKey = options?.childKey;
  if (
    childKey !== undefined &&
    (childKey.trim().length === 0 || childKey.length > 128)
  ) {
    throw new Error("Child Workflow options.childKey must contain 1-128 characters");
  }
  if (
    options?.id !== undefined &&
    (options.id.length === 0 || options.id.length > 100)
  ) {
    throw new Error("Child Workflow options.id must contain 1-100 characters");
  }

  if (options?.id === undefined && childKey === undefined) {
    if (unnamedChildTargets.has(childName)) {
      throw new Error(
        `Ambiguous duplicate child Workflow "${childName}"; provide a distinct options.childKey`,
      );
    }
    unnamedChildTargets.add(childName);
  }

  const identityKey = options?.id ?? childKey ?? "default";
  const digest = await sha256Hex(
    [
      parent.id,
      parent.name,
      parent.idempotencyKey,
      childName,
      identityKey,
    ].join("\u0000"),
  );
  const id =
    options?.id ??
    `child_${sanitizeIdentifier(childName).slice(0, 48)}_${digest.slice(0, 32)}`.slice(
      0,
      100,
    );
  const { childKey: _childKey, ...rest } = options ?? {};
  void _childKey;

  return {
    stepName: `sdk dispatch ${sanitizeIdentifier(childName).slice(0, 80)} ${digest.slice(0, 24)}`,
    options: {
      ...rest,
      id,
      createdAt: options?.createdAt ?? parent.createdAt,
      idempotencyKey: options?.idempotencyKey ?? `child:${digest}`,
      traceId: options?.traceId ?? parent.traceId,
    },
  };
}

async function sha256Hex(value: string): Promise<string> {
  const digest = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(value),
  );
  return [...new Uint8Array(digest)]
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}

function sanitizeIdentifier(value: string): string {
  return value.replace(/[^a-zA-Z0-9_-]+/g, "-").replace(/^-+|-+$/g, "") || "workflow";
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function stringOrUndefined(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}
