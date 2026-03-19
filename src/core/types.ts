// ─── Core Types ──────────────────────────────────────────────────────────────
// Runtime-agnostic types for the workflows SDK.

/** Result returned by send/dispatch operations. */
export interface SendResult {
  ids: string[];
}

/** Options for dispatching a workflow event. */
export interface SendOptions {
  /** Delay in milliseconds before the workflow should execute. */
  delay?: number;
  /** Explicit idempotency key. A unique key is generated per dispatch when omitted. */
  idempotencyKey?: string;
  /** Trace ID for distributed tracing across SDK → Worker → Backend. Auto-generated if omitted. */
  traceId?: string;
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

/** Minimal shape of every workflow event dispatched through the SDK. */
export interface WorkflowDispatchEvent<
  TName extends string = string,
  TData extends object = object,
> {
  name: TName;
  data: TData;
}

/**
 * Transport adapter — the pluggable piece that actually delivers events to
 * the workflow runtime (Cloudflare Worker, local mock, Inngest, etc.).
 */
export interface WorkflowTransport {
  /** Send a single event or an array of events. */
  send(
    events: WorkflowDispatchEvent | WorkflowDispatchEvent[],
    options?: SendOptions,
  ): Promise<SendResult>;
}

/**
 * Context passed to the `onSendExhausted` callback when all retries fail.
 * Contains everything needed to persist the failed event for later retry.
 */
export interface SendExhaustedContext {
  /** The events that failed to dispatch. */
  events: WorkflowDispatchEvent[];
  /** Original send options (delay, idempotency key). */
  options?: SendOptions;
  /** The final error after all retries were exhausted. */
  error: Error;
  /** Number of attempts made before giving up. */
  attempts: number;
}

/** Configuration for creating a workflows client. */
export interface WorkflowsClientConfig {
  /** Primary transport (e.g. Cloudflare Worker HTTP). */
  transport: WorkflowTransport;
  /**
   * Optional secondary transport for dual-run mode.
   * When set, events are dispatched to both transports.
   */
  shadowTransport?: WorkflowTransport;
  /** When true, enables dual-run fan-out. */
  dualRun?: boolean;
  /**
   * Called when all transport-level retries are exhausted.
   * Use this to persist the failed event to a database for later retry.
   * If this callback itself throws, the original error is re-thrown to the caller.
   */
  onSendExhausted?: (ctx: SendExhaustedContext) => Promise<void>;
}
