import type { WorkflowRegistry } from "../core/registry";
import { WorkflowValidationError } from "../core/errors";
import type {
  RegisteredWorkflow,
  RetryPolicy,
  WorkflowPayload,
  WorkflowRunContext,
  WorkflowStepOptions,
} from "../core/types";

export interface BackendCallbackExecuteContext {
  workflowName: string;
  stepName: string;
  traceId: string;
  eventId: string;
  idempotencyKey: string;
}

export interface BackendCallbackService {
  execute(
    path: string,
    payload: WorkflowPayload,
    context: BackendCallbackExecuteContext,
  ): Promise<void>;
}

export interface BackendCallbackFailedEvent {
  eventId: string;
  workflowName: string;
  backendPath: string;
  backendEventId: string;
  backendSteps?: Array<{
    backendPath: string;
    backendEventId: string;
  }>;
  payload: WorkflowPayload;
  idempotencyKey: string;
  error: string;
}

export interface BackendCallbackFailedEventService {
  record(event: BackendCallbackFailedEvent): Promise<void>;
}

export interface BackendCallbackWorkflowServices {
  backend: BackendCallbackService;
  failedEvents?: BackendCallbackFailedEventService;
}

export interface BackendCallbackStep {
  stepName: string;
  backendPath: string;
  backendEventId?: string;
  backendEventIdSuffix?: string;
}

export interface BackendCallbackWorkflowRegistryOptions {
  defaultStepName?: (workflowName: string) => string;
  stepOptions?: WorkflowStepOptions;
  failedEventStepOptions?: WorkflowStepOptions;
  workflowRetry?: RetryPolicy | false;
  workflowTimeoutMs?: number;
  /** Return true to allow a workflow name, or a reason string to reject it. */
  workflowNamePolicy?: (workflowName: string) => true | string;
  /**
   * Validate explicit multi-step metadata for a specific workflow. An empty
   * list means the normal implicit single callback whose path is the workflow
   * name. Use this to keep a signed dispatch token from turning one Workflow
   * instance into unrelated backend side effects.
   */
  callbackStepsPolicy?: (
    workflowName: string,
    steps: readonly BackendCallbackStep[],
  ) => true | string;
}

type BackendCallbackMetadata = {
  callbackSteps?: unknown;
};

type BackendCallbackWorkflow = RegisteredWorkflow<
  WorkflowPayload,
  unknown,
  BackendCallbackWorkflowServices
>;

const FAILED_EVENT_RETRY: RetryPolicy = {
  maxAttempts: 1,
  initialIntervalMs: 1_000,
  multiplier: 1,
  maxIntervalMs: 1_000,
};

const DEFAULT_STEP_OPTIONS: WorkflowStepOptions = {
  // Queue recovery is the single retry owner for backend side effects.
  retry: false,
};

const DEFAULT_FAILED_EVENT_STEP_OPTIONS: WorkflowStepOptions = {
  retry: FAILED_EVENT_RETRY,
};

export function createBackendCallbackWorkflowRegistry(
  options: BackendCallbackWorkflowRegistryOptions = {},
): WorkflowRegistry<readonly BackendCallbackWorkflow[]> {
  const workflows = new Map<string, BackendCallbackWorkflow>();

  return {
    workflows: [],
    get(name: string) {
      const workflowName = normalizeWorkflowName(name);
      const rejection = workflowNameRejection(workflowName, options);
      if (rejection) throw new WorkflowValidationError(rejection);
      let workflow = workflows.get(workflowName);
      if (!workflow) {
        workflow = createBackendCallbackWorkflow(workflowName, options);
        workflows.set(workflowName, workflow);
      }
      return workflow;
    },
    has(name: string) {
      const workflowName = normalizeWorkflowName(name);
      return (
        workflowName.length > 0 &&
        workflowNameRejection(workflowName, options) === null
      );
    },
    names() {
      return [...workflows.keys()];
    },
    parsePayload<TPayload extends WorkflowPayload>(
      _workflow: RegisteredWorkflow<TPayload>,
      payload: unknown,
    ): TPayload {
      if (
        typeof payload !== "object" ||
        payload === null ||
        Array.isArray(payload)
      ) {
        throw new Error("Workflow payload validation failed: payload must be an object");
      }
      return payload as TPayload;
    },
    validateEvent(_workflow, envelope) {
      validateCallbackSteps(
        envelope.name,
        envelope.metadata as BackendCallbackMetadata | undefined,
        options,
      );
    },
  };
}

export function isNonRetryableCallbackWorkflowFailure(error: unknown): boolean {
  if (!(error instanceof Error)) return false;

  return (
    error.name === "NonRetryableError" ||
    error.name === "NonRetryableWorkflowError" ||
    error.message.startsWith("NonRetryableError:")
  );
}

/**
 * Callback paths are URL path fragments, never URLs. Keep them inside the
 * `/workflows/execute/` namespace even when metadata came from a signed caller.
 */
export function assertBackendCallbackPath(
  value: string,
  label = "Backend callback path",
): void {
  if (value.length === 0 || value.length > 512) {
    throw new Error(`${label} must contain 1-512 characters`);
  }
  const segments = value.split("/");
  if (
    segments.some(
      (segment) =>
        segment.length === 0 ||
        segment.length > 128 ||
        segment === "." ||
        segment === ".." ||
        !/^[a-zA-Z0-9_][a-zA-Z0-9._-]*$/.test(segment),
    )
  ) {
    throw new Error(
      `${label} must use slash-separated alphanumeric, dot, underscore, or hyphen segments`,
    );
  }
}

function createBackendCallbackWorkflow(
  name: string,
  options: BackendCallbackWorkflowRegistryOptions,
): BackendCallbackWorkflow {
  return {
    name,
    retry: options.workflowRetry,
    timeoutMs: options.workflowTimeoutMs,
    async run(ctx, payload) {
      const steps = getCallbackSteps(ctx, options);
      let failedStepIndex = 0;

      const runStep = async (stepIndex: number) => {
        const step = steps[stepIndex]!;
        failedStepIndex = stepIndex;
        await executeBackendStep(ctx, payload, step, options);
      };

      try {
        for (let index = 0; index < steps.length; index++) {
          await runStep(index);
        }
      } catch (error) {
        const failedStep = steps[failedStepIndex]!;
        const remainingSteps = steps.slice(failedStepIndex);
        const failedEvent = createFailedEvent(ctx, payload, {
          backendPath: failedStep.backendPath,
          backendEventId: failedStep.backendEventId,
          backendSteps:
            remainingSteps.length > 1
              ? remainingSteps.map((step) => ({
                  backendPath: step.backendPath,
                  backendEventId: step.backendEventId,
                }))
              : undefined,
          error,
        });
        const queued = await recordFailedEvent(ctx, failedEvent, error, options);
        if (queued) {
          return queuedForRetryResult(ctx, failedEvent);
        }
        throw error;
      }

      return { status: "completed", eventId: ctx.event.id, eventName: name };
    },
  };
}

async function executeBackendStep(
  ctx: WorkflowRunContext<BackendCallbackWorkflowServices>,
  payload: WorkflowPayload,
  step: Required<Pick<BackendCallbackStep, "stepName" | "backendPath">> & {
    backendEventId: string;
  },
  options: BackendCallbackWorkflowRegistryOptions,
): Promise<void> {
  await ctx.step(
    step.stepName,
    () =>
      ctx.services.backend.execute(step.backendPath, payload, {
        workflowName: ctx.event.name,
        stepName: step.stepName,
        traceId: ctx.traceId,
        eventId: step.backendEventId,
        idempotencyKey: ctx.idempotencyKey,
      }),
    options.stepOptions ?? DEFAULT_STEP_OPTIONS,
  );
}

async function recordFailedEvent(
  ctx: WorkflowRunContext<BackendCallbackWorkflowServices>,
  event: BackendCallbackFailedEvent,
  originalError: unknown,
  options: BackendCallbackWorkflowRegistryOptions,
): Promise<boolean> {
  if (
    !ctx.services.failedEvents ||
    isNonRetryableCallbackWorkflowFailure(originalError)
  ) {
    return false;
  }

  try {
    await ctx.step(
      "persist-failed-event",
      () => ctx.services.failedEvents?.record(event),
      options.failedEventStepOptions ?? DEFAULT_FAILED_EVENT_STEP_OPTIONS,
    );
    return true;
  } catch (recordError) {
    ctx.logger.error?.("workflow.failed_event_record_failed", {
      workflow: ctx.event.name,
      eventId: ctx.event.id,
      backendPath: event.backendPath,
      originalError: event.error,
      recordError:
        recordError instanceof Error
          ? recordError.message
          : String(recordError),
    });
    return false;
  }
}

function queuedForRetryResult(
  ctx: WorkflowRunContext<BackendCallbackWorkflowServices>,
  event: BackendCallbackFailedEvent,
) {
  return {
    status: "queued_for_retry",
    eventId: ctx.event.id,
    eventName: ctx.event.name,
    backendPath: event.backendPath,
    backendEventId: event.backendEventId,
    error: event.error,
  };
}

function createFailedEvent(
  ctx: WorkflowRunContext<BackendCallbackWorkflowServices>,
  payload: WorkflowPayload,
  params: {
    backendPath: string;
    backendEventId: string;
    backendSteps?: Array<{
      backendPath: string;
      backendEventId: string;
    }>;
    error: unknown;
  },
): BackendCallbackFailedEvent {
  return {
    eventId: ctx.event.id,
    workflowName: ctx.event.name,
    backendPath: params.backendPath,
    backendEventId: params.backendEventId,
    ...(params.backendSteps === undefined
      ? {}
      : { backendSteps: params.backendSteps }),
    payload,
    idempotencyKey: ctx.idempotencyKey,
    error: boundedError(params.error),
  };
}

function getCallbackSteps(
  ctx: WorkflowRunContext<BackendCallbackWorkflowServices>,
  options: BackendCallbackWorkflowRegistryOptions,
): Array<
  Required<Pick<BackendCallbackStep, "stepName" | "backendPath">> & {
    backendEventId: string;
  }
> {
  const metadata = ctx.event.metadata as BackendCallbackMetadata | undefined;
  const configuredSteps = validateCallbackSteps(
    ctx.event.name,
    metadata,
    options,
  );

  if (configuredSteps.length === 0) {
    const backendPath = ctx.event.name;
    return [
      {
        stepName: (options.defaultStepName ?? defaultStepName)(backendPath),
        backendPath,
        backendEventId: ctx.event.id,
      },
    ];
  }

  return configuredSteps.map((step) => ({
    stepName: step.stepName,
    backendPath: step.backendPath,
    backendEventId: resolveBackendEventId(ctx.event.id, step),
  }));
}

function validateCallbackSteps(
  workflowName: string,
  metadata: BackendCallbackMetadata | undefined,
  options: BackendCallbackWorkflowRegistryOptions,
): BackendCallbackStep[] {
  const configuredSteps = parseCallbackSteps(metadata?.callbackSteps);
  const stepRejection = options.callbackStepsPolicy?.(
    workflowName,
    configuredSteps,
  );
  if (typeof stepRejection === "string") {
    throw new WorkflowValidationError(stepRejection);
  }
  return configuredSteps;
}

function parseCallbackSteps(value: unknown): BackendCallbackStep[] {
  if (value === undefined) return [];
  if (!Array.isArray(value)) {
    throw new Error(
      "Workflow callbackSteps metadata must be an array when provided",
    );
  }
  if (value.length > 3) {
    throw new Error("Workflow callbackSteps metadata must contain at most 3 items");
  }

  const steps: BackendCallbackStep[] = [];

  for (const [index, item] of value.entries()) {
    if (typeof item !== "object" || item === null || Array.isArray(item)) {
      throw new Error(
        `Workflow callbackSteps metadata item ${index} must be an object`,
      );
    }

    const step = item as Record<string, unknown>;
    const stepName = parseRequiredMetadataString(
      step.stepName,
      `callbackSteps[${index}].stepName`,
      256,
    );
    const backendPath = parseRequiredMetadataString(
      step.backendPath,
      `callbackSteps[${index}].backendPath`,
      512,
    );
    if (
      stepName === null ||
      backendPath === null
    ) {
      throw new Error(
        `Workflow callbackSteps metadata item ${index} requires stepName and backendPath`,
      );
    }
    assertBackendCallbackPath(
      backendPath,
      `Workflow callbackSteps[${index}].backendPath`,
    );

    const parsed: BackendCallbackStep = {
      stepName,
      backendPath,
    };
    const backendEventId = parseOptionalMetadataString(
      step.backendEventId,
      `callbackSteps[${index}].backendEventId`,
      512,
    );
    const backendEventIdSuffix = parseOptionalMetadataString(
      step.backendEventIdSuffix,
      `callbackSteps[${index}].backendEventIdSuffix`,
      256,
    );
    if (backendEventId !== undefined) parsed.backendEventId = backendEventId;
    if (backendEventIdSuffix !== undefined) {
      parsed.backendEventIdSuffix = backendEventIdSuffix;
    }
    steps.push(parsed);
  }

  return steps;
}

function parseRequiredMetadataString(
  value: unknown,
  field: string,
  maxLength: number,
): string | null {
  if (typeof value !== "string") return null;
  const normalized = value.trim();
  if (
    normalized.length === 0 ||
    normalized.length > maxLength ||
    /[\u0000-\u001f\u007f]/.test(normalized)
  ) {
    throw new Error(
      `Workflow ${field} must contain 1-${maxLength} printable characters`,
    );
  }
  return normalized;
}

function parseOptionalMetadataString(
  value: unknown,
  field: string,
  maxLength: number,
): string | undefined {
  if (value === undefined) return undefined;
  const parsed = parseRequiredMetadataString(value, field, maxLength);
  if (parsed === null) {
    throw new Error(
      `Workflow ${field} must contain 1-${maxLength} printable characters`,
    );
  }
  return parsed;
}

function resolveBackendEventId(
  workflowEventId: string,
  step: BackendCallbackStep,
): string {
  if (step.backendEventId) return step.backendEventId;
  if (step.backendEventIdSuffix) {
    return `${workflowEventId}:${step.backendEventIdSuffix}`;
  }
  return workflowEventId;
}

function normalizeWorkflowName(name: string): string {
  return typeof name === "string" ? name.trim() : "";
}

function workflowNameRejection(
  workflowName: string,
  options: BackendCallbackWorkflowRegistryOptions,
): string | null {
  if (!workflowName) return "Workflow name must not be empty";
  try {
    assertBackendCallbackPath(workflowName, "Workflow name");
  } catch (error) {
    return error instanceof Error ? error.message : String(error);
  }
  const decision = options.workflowNamePolicy?.(workflowName) ?? true;
  return decision === true ? null : decision;
}

function boundedError(error: unknown): string {
  const raw = error instanceof Error ? error.message : String(error);
  const clean = raw
    .replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  return (clean || "Backend callback failed").slice(0, 2_048);
}

export function createBackendCallbackStepName(workflowName: string): string {
  const prefix = "callback-";
  const sanitized =
    workflowName.replace(/[^a-zA-Z0-9]+/g, "-").replace(/^-+|-+$/g, "") ||
    "workflow";
  const full = `${prefix}${sanitized}`;
  if (full.length <= 256) return full;

  const suffix = `-${stableStepNameHash(workflowName)}`;
  return `${prefix}${sanitized.slice(0, 256 - prefix.length - suffix.length)}${suffix}`;
}

function stableStepNameHash(value: string): string {
  let first = 0x811c9dc5;
  let second = 0x9e3779b9;
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    first = Math.imul(first ^ code, 0x01000193);
    second = Math.imul(second ^ (code + index), 0x85ebca6b);
  }
  return `${(first >>> 0).toString(16).padStart(8, "0")}${(second >>> 0)
    .toString(16)
    .padStart(8, "0")}`;
}

const defaultStepName = createBackendCallbackStepName;
