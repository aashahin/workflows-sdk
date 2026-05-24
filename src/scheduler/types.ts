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
