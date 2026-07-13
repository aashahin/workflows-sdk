/**
 * Controls how missed cron occurrences are handled when a poll/tick runs later
 * than the moment an occurrence was due (cold start, deploy gap, downtime).
 *
 * - `"skip"` (default): only dispatch the most recent occurrence if it is still
 *   fresh — due within `CronDefinition.maxDelayMs` (default 1 hour). A run that
 *   was missed longer ago than that window is dropped.
 * - `"catch-up-latest"`: always dispatch the single most recent missed
 *   occurrence, regardless of how long ago it was due.
 * - `{ mode: "catch-up-all", maxRuns }`: dispatch up to `maxRuns` of the most
 *   recent missed occurrences, in chronological order.
 */
export type MissedRunPolicy =
  | "skip"
  | "catch-up-latest"
  | {
      mode: "catch-up-all";
      maxRuns: number;
    };

export interface CronDefinition {
  name?: string;
  schedule: string;
  timezone?: string;
  payload?: Record<string, unknown> | (() => Record<string, unknown>);
  missedRunPolicy?: MissedRunPolicy;
  /**
   * Freshness window in milliseconds for the `"skip"` missed-run policy. A
   * missed occurrence older than this is skipped instead of dispatched.
   * Defaults to 3_600_000 (1 hour). Ignored by the catch-up policies.
   */
  maxDelayMs?: number;
  metadata?: Record<string, unknown>;
}

export interface CronRun {
  workflowName: string;
  cronName: string;
  schedule: string;
  scheduledAt: Date;
  runKey: string;
  payload: Record<string, unknown>;
  metadata?: Record<string, unknown>;
}
