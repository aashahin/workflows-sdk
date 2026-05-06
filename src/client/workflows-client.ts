// ─── Workflows Client ────────────────────────────────────────────────────────
// Central client that dispatches events through configured transports.
// Supports dual-run mode for phased migration.

import type {
  SendExhaustedContext,
  SendOptions,
  SendResult,
  WorkflowDispatchEvent,
  WorkflowsClientConfig,
  WorkflowTransport,
} from "../core/types";
import { DEFAULT_RETRY_POLICY } from "../helpers/retry";

function isNonRetryableTransportError(error: unknown): boolean {
  return (
    !!error &&
    typeof error === "object" &&
    "nonRetryable" in error &&
    (error as { nonRetryable?: unknown }).nonRetryable === true
  );
}

export class WorkflowsClient {
  private transport: WorkflowTransport;
  private shadowTransport?: WorkflowTransport;
  private dualRun: boolean;
  private onSendExhausted?: (ctx: SendExhaustedContext) => Promise<void>;

  constructor(config: WorkflowsClientConfig) {
    this.transport = config.transport;
    this.shadowTransport = config.shadowTransport;
    this.dualRun = config.dualRun ?? false;
    this.onSendExhausted = config.onSendExhausted;
  }

  /**
   * Dispatch one or more workflow events.
   *
   * In dual-run mode, events are dispatched to both transports.
   * The primary result is returned; shadow failures are logged but not thrown.
   *
   * If the primary transport exhausts all retries and `onSendExhausted` is
   * configured, the callback is invoked to persist the failed event.
   * The method then returns `{ ids: [] }` instead of throwing, so the
   * caller's business logic is not interrupted.
   */
  async send(
    events: WorkflowDispatchEvent | WorkflowDispatchEvent[],
    options?: SendOptions,
  ): Promise<SendResult> {
    const eventArray = Array.isArray(events) ? events : [events];

    try {
      // Primary dispatch — must succeed
      const result = await this.transport.send(events, options);

      // Shadow dispatch (dual-run) — best-effort
      if (this.dualRun && this.shadowTransport) {
        void this.shadowTransport.send(events, options).catch((error) => {
          console.warn(
            "[WorkflowsClient] Shadow transport dispatch failed:",
            error instanceof Error ? error.message : String(error),
          );
        });
      }

      return result;
    } catch (error) {
      // If no fallback is configured, let the error propagate
      if (!this.onSendExhausted) throw error;

      const err = error instanceof Error ? error : new Error(String(error));
      const nonRetryable = isNonRetryableTransportError(error);

      if (nonRetryable) {
        console.warn(
          `[WorkflowsClient] Non-retryable transport failure — recording ${eventArray.length} event(s) as dead letter:`,
          err.message,
        );
      } else {
        console.error(
          `[WorkflowsClient] Transport exhausted — persisting ${eventArray.length} event(s) for later retry:`,
          err.message,
        );
      }

      try {
        await this.onSendExhausted({
          events: eventArray,
          options,
          error: err,
          attempts: DEFAULT_RETRY_POLICY.maxAttempts + 1,
        });
      } catch (persistError) {
        // If even the DB persist fails, log it and re-throw the original error
        console.error(
          "[WorkflowsClient] Failed to persist exhausted events:",
          persistError instanceof Error
            ? persistError.message
            : String(persistError),
        );
        throw error;
      }

      // Event is safely persisted — return empty result instead of throwing
      return { ids: [] };
    }
  }

  /** Update dual-run mode at runtime (e.g. from feature flag). */
  setDualRun(enabled: boolean): void {
    this.dualRun = enabled;
  }
}

/**
 * Factory to create a WorkflowsClient.
 * Mirrors the pattern used in other SDK packages.
 */
export function createWorkflowsClient(
  config: WorkflowsClientConfig,
): WorkflowsClient {
  return new WorkflowsClient(config);
}
