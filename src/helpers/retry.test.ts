import { describe, expect, test } from "bun:test";
import type { RetryPolicy } from "../core/types";
import { DEFAULT_RETRY_POLICY, getBackoffDelay, withRetry } from "./retry";

describe("getBackoffDelay", () => {
  test("computes exact exponential backoff without jitter", () => {
    expect(getBackoffDelay(0)).toBe(1_000);
    expect(getBackoffDelay(1)).toBe(2_000);
    expect(getBackoffDelay(2)).toBe(4_000);
  });

  test("caps the delay at maxIntervalMs", () => {
    expect(getBackoffDelay(10)).toBe(DEFAULT_RETRY_POLICY.maxIntervalMs);
  });

  test("applies jitter within [delay * (1 - jitter), delay] when configured", () => {
    const policy: RetryPolicy = {
      maxAttempts: 3,
      initialIntervalMs: 1_000,
      multiplier: 2,
      maxIntervalMs: 30_000,
      jitter: 0.5,
    };

    for (let i = 0; i < 100; i++) {
      const delay = getBackoffDelay(1, policy);
      expect(delay).toBeLessThanOrEqual(2_000);
      expect(delay).toBeGreaterThanOrEqual(1_000);
    }
  });

  test("jitter defaults to no jitter (exact delay preserved)", () => {
    const policy: RetryPolicy = { ...DEFAULT_RETRY_POLICY };
    expect(getBackoffDelay(0, policy)).toBe(1_000);
  });
});

describe("withRetry", () => {
  test("retries until success", async () => {
    let attempts = 0;
    const result = await withRetry(
      async () => {
        attempts += 1;
        if (attempts < 3) throw new Error("boom");
        return "ok";
      },
      { maxAttempts: 3, initialIntervalMs: 1, multiplier: 1, maxIntervalMs: 1 },
    );
    expect(result).toBe("ok");
    expect(attempts).toBe(3);
  });

  test("fails fast on non-retryable errors", async () => {
    let attempts = 0;
    const error = Object.assign(new Error("nope"), { nonRetryable: true });
    await expect(
      withRetry(
        async () => {
          attempts += 1;
          throw error;
        },
        { maxAttempts: 3, initialIntervalMs: 1, multiplier: 1, maxIntervalMs: 1 },
      ),
    ).rejects.toThrow("nope");
    expect(attempts).toBe(1);
  });
});
