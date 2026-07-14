// ─── Retry Helpers ───────────────────────────────────────────────────────────

import type { RetryPolicy } from "../core/types";

/** Default retry policy matching Inngest's retries: 3 with exponential backoff. */
export const DEFAULT_RETRY_POLICY: RetryPolicy = {
  maxAttempts: 3,
  initialIntervalMs: 1_000,
  multiplier: 2,
  maxIntervalMs: 30_000,
};

/** Calculate the backoff delay for a given attempt (0-indexed). */
export function getBackoffDelay(
  attempt: number,
  policy: RetryPolicy = DEFAULT_RETRY_POLICY,
): number {
  const delay = Math.min(
    policy.initialIntervalMs * Math.pow(policy.multiplier, attempt),
    policy.maxIntervalMs,
  );

  // Optional jitter spreads out otherwise-synchronized retries so a recovering
  // dependency isn't hit by identical backoff schedules (thundering herd).
  // Default (undefined) preserves exact exponential backoff.
  if (policy.jitter && policy.jitter > 0) {
    const fraction = Math.min(policy.jitter, 1);
    return delay * (1 - fraction * Math.random());
  }

  return delay;
}

/**
 * Execute a function with retry according to a policy.
 * Returns the result on success or throws after all retries are exhausted.
 *
 * If the thrown error has a `nonRetryable` property set to `true`,
 * retries are skipped and the error is re-thrown immediately.
 */
export async function withRetry<T>(
  fn: () => Promise<T>,
  policy: RetryPolicy = DEFAULT_RETRY_POLICY,
): Promise<T> {
  let lastError: unknown;

  for (let attempt = 0; attempt <= policy.maxAttempts; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;

      // Non-retryable errors (e.g. 4xx) should fail immediately
      if (isNonRetryable(error)) {
        throw error;
      }

      if (attempt < policy.maxAttempts) {
        const delay = getBackoffDelay(attempt, policy);
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
    }
  }

  throw lastError;
}

/** Check if an error is marked as non-retryable. */
function isNonRetryable(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    "nonRetryable" in error &&
    (error as any).nonRetryable === true
  );
}
