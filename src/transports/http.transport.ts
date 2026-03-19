// ─── HTTP Transport ──────────────────────────────────────────────────────────
// Sends workflow events to the Cloudflare Worker via HTTP POST.

import { WorkflowSendError } from "../core/errors";
import type {
  RetryPolicy,
  SendOptions,
  SendResult,
  WorkflowDispatchEvent,
  WorkflowTransport,
} from "../core/types";
import { generateEventId } from "../helpers/idempotency";
import { DEFAULT_RETRY_POLICY, withRetry } from "../helpers/retry";

export interface HttpTransportConfig {
  /** Base URL of the workflows worker (e.g. https://workflows.example.com). */
  baseUrl: string;
  /** Shared secret for authenticating requests to the worker. */
  authToken: string;
  /** Request timeout in ms (default: 10_000). */
  timeoutMs?: number;
  /**
   * Retry policy for transient failures (network errors, 5xx, timeouts).
   * Set to `false` to disable transport-level retries.
   * Default: 3 attempts with exponential backoff.
   */
  retry?: RetryPolicy | false;
}

export class HttpTransport implements WorkflowTransport {
  private baseUrl: string;
  private authToken: string;
  private timeoutMs: number;
  private retryPolicy: RetryPolicy | false;

  constructor(config: HttpTransportConfig) {
    this.baseUrl = config.baseUrl.replace(/\/+$/, "");
    this.authToken = config.authToken;
    this.timeoutMs = config.timeoutMs ?? 10_000;
    this.retryPolicy = config.retry ?? DEFAULT_RETRY_POLICY;
  }

  async send(
    events: WorkflowDispatchEvent | WorkflowDispatchEvent[],
    options?: SendOptions,
  ): Promise<SendResult> {
    const eventArray = Array.isArray(events) ? events : [events];

    const payload = eventArray.map((event) => ({
      id: generateEventId(),
      idempotencyKey: options?.idempotencyKey ?? generateEventId(),
      traceId: options?.traceId ?? generateEventId(),
      event,
      delayMs: options?.delay ?? 0,
    }));

    const doSend = () => this._doSend(payload);

    // If retry is disabled, send once
    if (this.retryPolicy === false) {
      return doSend();
    }

    // Retry with exponential backoff on transient failures
    return withRetry(doSend, this.retryPolicy);
  }

  /** Internal: single HTTP dispatch attempt. */
  private async _doSend(
    payload: Array<{
      id: string;
      idempotencyKey: string;
      event: WorkflowDispatchEvent;
      delayMs: number;
    }>,
  ): Promise<SendResult> {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.timeoutMs);

    try {
      const response = await fetch(`${this.baseUrl}/dispatch`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${this.authToken}`,
        },
        body: JSON.stringify({ events: payload }),
        signal: controller.signal,
      });

      // 4xx errors (except 429) are non-retryable — bad input, auth, etc.
      if (
        response.status >= 400 &&
        response.status < 500 &&
        response.status !== 429
      ) {
        const body = await response.text().catch(() => "");
        const err = new WorkflowSendError(
          `Worker responded with ${response.status}: ${body}`,
        );
        (err as any).nonRetryable = true;
        throw err;
      }

      // 5xx / 429 — transient, let retry logic handle it
      if (!response.ok) {
        const body = await response.text().catch(() => "");
        throw new WorkflowSendError(
          `Worker responded with ${response.status}: ${body}`,
        );
      }

      const result = (await response.json()) as {
        ids: string[];
        errors?: Array<{ id: string; error: string }>;
      };

      // Log partial failures but still return successfully dispatched IDs.
      // Throwing here would discard successful dispatches and could cause
      // the caller to retry the entire batch, creating duplicates.
      if (result.errors?.length) {
        const failed = result.errors
          .map((e) => `${e.id}: ${e.error}`)
          .join("; ");
        console.error(
          `[HttpTransport] Partial dispatch failure (${result.errors.length}/${result.ids.length + result.errors.length} failed): ${failed}`,
        );
      }

      return { ids: result.ids };
    } catch (error) {
      // Re-throw WorkflowSendError as-is (preserves nonRetryable flag)
      if (error instanceof WorkflowSendError) throw error;

      // Network errors / timeouts are transient — wrap and let retry handle
      throw new WorkflowSendError(
        `Failed to dispatch workflow events: ${error instanceof Error ? error.message : String(error)}`,
        error,
      );
    } finally {
      clearTimeout(timeout);
    }
  }
}
