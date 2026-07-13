// ─── Workflow Errors ─────────────────────────────────────────────────────────

/** Base error for all workflow SDK errors. */
export class WorkflowError extends Error {
  public readonly code: string;

  constructor(message: string, code: string) {
    super(message);
    this.name = "WorkflowError";
    this.code = code;
  }
}

/** Transport-level send failure. */
export class WorkflowSendError extends WorkflowError {
  public override readonly cause?: unknown;

  constructor(message: string, cause?: unknown) {
    super(message, "WORKFLOW_SEND_ERROR");
    this.name = "WorkflowSendError";
    this.cause = cause;
  }
}

/** Invalid event payload. */
export class WorkflowValidationError extends WorkflowError {
  constructor(message: string) {
    super(message, "WORKFLOW_VALIDATION_ERROR");
    this.name = "WorkflowValidationError";
  }
}

/**
 * Max retries exhausted.
 *
 * Part of the SDK's public error taxonomy: runtime adapters that surface retry
 * exhaustion explicitly may throw this, and consumers can match on it via
 * `instanceof`. The built-in `withRetry` helper intentionally re-throws the
 * underlying error unchanged (so `nonRetryable`/cause information is preserved),
 * so the core runtime does not construct this itself.
 */
export class WorkflowRetryExhaustedError extends WorkflowError {
  public readonly attempts: number;

  constructor(message: string, attempts: number) {
    super(message, "WORKFLOW_RETRY_EXHAUSTED");
    this.name = "WorkflowRetryExhaustedError";
    this.attempts = attempts;
  }
}

/** Workflow definition is missing from the configured registry. */
export class WorkflowNotFoundError extends WorkflowError {
  constructor(name: string) {
    super(`Workflow "${name}" is not registered`, "WORKFLOW_NOT_FOUND");
    this.name = "WorkflowNotFoundError";
  }
}

/**
 * A workflow was claimed by another runtime instance.
 *
 * Part of the SDK's public error taxonomy for adapters that use a claim/lease
 * model (e.g. the Redis/SQLite Bun adapters); consumers can match on it via
 * `instanceof` to detect claim races. Not thrown by the core runtime itself.
 */
export class WorkflowAlreadyClaimedError extends WorkflowError {
  constructor(message: string) {
    super(message, "WORKFLOW_ALREADY_CLAIMED");
    this.name = "WorkflowAlreadyClaimedError";
  }
}
