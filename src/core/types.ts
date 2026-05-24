// ─── Core Types ──────────────────────────────────────────────────────────────
// Runtime-agnostic v1 types for the unified workflows SDK.

import type { CronDefinition } from "../scheduler/types";

export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };
export type WorkflowPayload = Record<string, unknown>;

export type WorkflowStatus =
  | "queued"
  | "scheduled"
  | "running"
  | "waiting"
  | "waitingForPause"
  | "paused"
  | "complete"
  | "failed"
  | "errored"
  | "dead"
  | "cancelled"
  | "terminated"
  | "unknown";

export interface WorkflowLogger {
  debug?(message: string, context?: Record<string, unknown>): void;
  info?(message: string, context?: Record<string, unknown>): void;
  warn?(message: string, context?: Record<string, unknown>): void;
  error?(message: string, context?: Record<string, unknown>): void;
}

export interface WorkflowStepOptions {
  retry?: RetryPolicy | false;
  timeoutMs?: number;
}

export interface WorkflowRunContext<TServices = unknown> {
  readonly event: WorkflowEventEnvelope;
  readonly traceId: string;
  readonly idempotencyKey: string;
  readonly logger: WorkflowLogger;
  readonly services: TServices;
  step<T>(name: string, fn: () => Promise<T> | T, options?: WorkflowStepOptions): Promise<T>;
  sleep(name: string, durationOrDate: number | Date | string): Promise<void>;
  dispatch<TPayload extends WorkflowPayload = WorkflowPayload>(
    name: string,
    payload: TPayload,
    options?: DispatchOptions,
  ): Promise<DispatchResult>;
}

export interface WorkflowDefinition<
  TPayload extends WorkflowPayload = WorkflowPayload,
  TResult = unknown,
  TServices = unknown,
> {
  schema?: WorkflowSchema<TPayload>;
  cron?: string | CronDefinition[];
  retry?: RetryPolicy | false;
  timeoutMs?: number;
  run(ctx: WorkflowRunContext<TServices>, payload: TPayload): Promise<TResult> | TResult;
}

export type WorkflowSchema<TPayload extends WorkflowPayload> =
  | {
      parse(value: unknown): TPayload;
    }
  | {
      safeParse(value: unknown): { success: true; data: TPayload } | { success: false; error: unknown };
    }
  | ((value: unknown) => TPayload);

export interface RegisteredWorkflow<
  TPayload extends WorkflowPayload = WorkflowPayload,
  TResult = unknown,
  TServices = unknown,
> extends WorkflowDefinition<TPayload, TResult, TServices> {
  name: string;
}

export interface WorkflowEventEnvelope<
  TName extends string = string,
  TPayload extends WorkflowPayload = WorkflowPayload,
> {
  id: string;
  name: TName;
  payload: TPayload;
  traceId: string;
  idempotencyKey: string;
  scheduledAt?: string;
  createdAt: string;
  metadata?: Record<string, unknown>;
}

export interface DispatchOptions {
  id?: string;
  delayMs?: number;
  scheduledAt?: Date | string;
  idempotencyKey?: string;
  traceId?: string;
  metadata?: Record<string, unknown>;
}

export interface DispatchResult {
  ids: string[];
  envelopes: WorkflowEventEnvelope[];
  instances?: WorkflowInstance[];
}

export interface WorkflowInstance {
  id: string;
  name: string;
  status: WorkflowStatus;
  traceId?: string;
  idempotencyKey?: string;
  scheduledAt?: string;
  createdAt?: string;
  updatedAt?: string;
  output?: unknown;
  error?: {
    name: string;
    message: string;
  };
}

export interface WorkflowAdapter {
  dispatch(envelope: WorkflowEventEnvelope): Promise<WorkflowInstance>;
  dispatchBatch?(envelopes: WorkflowEventEnvelope[]): Promise<WorkflowInstance[]>;
  dispatchCronRun?(runKey: string, envelope: WorkflowEventEnvelope): Promise<WorkflowInstance | null>;
  requeue?(
    envelope: WorkflowEventEnvelope,
    options: {
      scheduledAt?: Date | string;
      error?: { name: string; message: string };
    },
  ): Promise<void>;
  getInstance(id: string, options?: { name?: string }): Promise<WorkflowInstance | null>;
  cancel?(id: string): Promise<void>;
  claimCronRun?(runKey: string, envelope: WorkflowEventEnvelope): Promise<boolean>;
  releaseCronRun?(runKey: string, envelope: WorkflowEventEnvelope, error?: Error): Promise<void>;
  claimNext?(now?: Date): Promise<WorkflowEventEnvelope | null>;
  recoverStalled?(before: Date): Promise<number>;
  updateInstance?(
    id: string,
    status: WorkflowStatus,
    patch?: { output?: unknown; error?: { name: string; message: string } },
  ): Promise<void>;
  getStepResult?(instanceId: string, stepName: string): Promise<unknown | undefined>;
  hasStepResult?(instanceId: string, stepName: string): Promise<boolean>;
  saveStepResult?(instanceId: string, stepName: string, result: unknown): Promise<void>;
  recordDeadLetter?(envelope: WorkflowEventEnvelope, error: Error): Promise<void>;
}

export interface WorkflowClientHooks {
  onDispatch?(envelope: WorkflowEventEnvelope): void | Promise<void>;
  onDispatchFailed?(envelope: WorkflowEventEnvelope, error: Error): void | Promise<void>;
}

export interface WorkflowClientConfig {
  adapter: WorkflowAdapter;
  logger?: WorkflowLogger;
  hooks?: WorkflowClientHooks;
  idGenerator?: () => string;
  now?: () => Date;
}

/** Retry policy configuration for workflow steps. */
export interface RetryPolicy {
  /** Maximum number of retry attempts (default: 3). */
  maxAttempts: number;
  /** Initial backoff in ms (default: 1000). */
  initialIntervalMs: number;
  /** Backoff multiplier (default: 2). */
  multiplier: number;
  /** Maximum backoff in ms (default: 30_000). */
  maxIntervalMs: number;
}
