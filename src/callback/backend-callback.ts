import type { WorkflowRegistry } from "../core/registry";
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
}

type BackendCallbackMetadata = {
  callbackSteps?: unknown;
};

type BackendCallbackWorkflow = RegisteredWorkflow<
  WorkflowPayload,
  unknown,
  BackendCallbackWorkflowServices
>;

const DEFAULT_STEP_RETRY: RetryPolicy = {
  maxAttempts: 3,
  initialIntervalMs: 1_000,
  multiplier: 2,
  maxIntervalMs: 30_000,
};

const FAILED_EVENT_RETRY: RetryPolicy = {
  maxAttempts: 2,
  initialIntervalMs: 1_000,
  multiplier: 1,
  maxIntervalMs: 1_000,
};

const DEFAULT_STEP_OPTIONS: WorkflowStepOptions = {
  retry: DEFAULT_STEP_RETRY,
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
      let workflow = workflows.get(workflowName);
      if (!workflow) {
        workflow = createBackendCallbackWorkflow(workflowName, options);
        workflows.set(workflowName, workflow);
      }
      return workflow;
    },
    has(name: string) {
      return normalizeWorkflowName(name).length > 0;
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
  };
}

export function isNonRetryableCallbackWorkflowFailure(error: unknown): boolean {
  if (!(error instanceof Error)) return false;

  return (
    error.name === "NonRetryableError" ||
    error.name === "NonRetryableWorkflowError" ||
    error.message.includes("NonRetryableError")
  );
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
        const queued = await recordFailedEvent(ctx, payload, {
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
          options,
        });
        if (queued) {
          return queuedForRetryResult(ctx, {
            backendPath: failedStep.backendPath,
            backendEventId: failedStep.backendEventId,
            error,
          });
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
  payload: WorkflowPayload,
  params: {
    backendPath: string;
    backendEventId: string;
    backendSteps?: Array<{
      backendPath: string;
      backendEventId: string;
    }>;
    error: unknown;
    options: BackendCallbackWorkflowRegistryOptions;
  },
): Promise<boolean> {
  if (
    !ctx.services.failedEvents ||
    isNonRetryableCallbackWorkflowFailure(params.error)
  ) {
    return false;
  }

  const message =
    params.error instanceof Error ? params.error.message : String(params.error);

  try {
    await ctx.step(
      "persist-failed-event",
      () =>
        ctx.services.failedEvents?.record({
          eventId: ctx.event.id,
          workflowName: ctx.event.name,
          backendPath: params.backendPath,
          backendEventId: params.backendEventId,
          backendSteps: params.backendSteps,
          payload,
          idempotencyKey: ctx.idempotencyKey,
          error: message,
        }),
      params.options.failedEventStepOptions ?? DEFAULT_FAILED_EVENT_STEP_OPTIONS,
    );
    return true;
  } catch (recordError) {
    ctx.logger.error?.("workflow.failed_event_record_failed", {
      workflow: ctx.event.name,
      eventId: ctx.event.id,
      backendPath: params.backendPath,
      originalError: message,
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
  params: {
    backendPath: string;
    backendEventId: string;
    error: unknown;
  },
) {
  return {
    status: "queued_for_retry",
    eventId: ctx.event.id,
    eventName: ctx.event.name,
    backendPath: params.backendPath,
    backendEventId: params.backendEventId,
    error:
      params.error instanceof Error
        ? params.error.message
        : String(params.error),
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
  const configuredSteps = parseCallbackSteps(metadata?.callbackSteps);

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

function parseCallbackSteps(value: unknown): BackendCallbackStep[] {
  if (value === undefined) return [];
  if (!Array.isArray(value)) {
    throw new Error(
      "Workflow callbackSteps metadata must be an array when provided",
    );
  }

  const steps: BackendCallbackStep[] = [];

  for (const [index, item] of value.entries()) {
    if (typeof item !== "object" || item === null || Array.isArray(item)) {
      throw new Error(
        `Workflow callbackSteps metadata item ${index} must be an object`,
      );
    }

    const step = item as Record<string, unknown>;
    if (
      typeof step.stepName !== "string" ||
      step.stepName.trim().length === 0 ||
      typeof step.backendPath !== "string" ||
      step.backendPath.trim().length === 0
    ) {
      throw new Error(
        `Workflow callbackSteps metadata item ${index} requires stepName and backendPath`,
      );
    }

    const parsed: BackendCallbackStep = {
      stepName: step.stepName.trim(),
      backendPath: step.backendPath.trim(),
    };
    if (
      typeof step.backendEventId === "string" &&
      step.backendEventId.trim().length > 0
    ) {
      parsed.backendEventId = step.backendEventId.trim();
    }
    if (
      typeof step.backendEventIdSuffix === "string" &&
      step.backendEventIdSuffix.trim().length > 0
    ) {
      parsed.backendEventIdSuffix = step.backendEventIdSuffix.trim();
    }
    steps.push(parsed);
  }

  return steps;
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

function defaultStepName(workflowName: string): string {
  return `callback-${workflowName.replace(/[^a-zA-Z0-9]+/g, "-")}`;
}
