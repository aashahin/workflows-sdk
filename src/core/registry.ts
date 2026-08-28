import { WorkflowNotFoundError, WorkflowValidationError } from "./errors";
import type {
  RegisteredWorkflow,
  WorkflowEventEnvelope,
  WorkflowPayload,
  WorkflowSchema,
} from "./types";

export interface WorkflowRegistry<
  TWorkflows extends readonly RegisteredWorkflow[] = readonly RegisteredWorkflow[],
> {
  readonly workflows: TWorkflows;
  get(name: string): RegisteredWorkflow;
  has(name: string): boolean;
  names(): string[];
  parsePayload<TPayload extends WorkflowPayload>(
    workflow: RegisteredWorkflow<TPayload>,
    payload: unknown,
  ): TPayload;
  /**
   * Optional envelope-level admission hook. Cloudflare dispatchers call this
   * after payload parsing but before any Workflow binding create operation.
   */
  validateEvent?(
    workflow: RegisteredWorkflow,
    envelope: WorkflowEventEnvelope,
  ): void;
}

export function defineWorkflowRegistry<
  const TWorkflows extends readonly RegisteredWorkflow[],
>(workflows: TWorkflows): WorkflowRegistry<TWorkflows> {
  const map = new Map<string, RegisteredWorkflow>();

  for (const workflow of workflows) {
    if (map.has(workflow.name)) {
      throw new WorkflowValidationError(
        `Duplicate workflow name "${workflow.name}"`,
      );
    }
    map.set(workflow.name, workflow);
  }

  return {
    workflows,
    get(name: string) {
      const workflow = map.get(name);
      if (!workflow) throw new WorkflowNotFoundError(name);
      return workflow;
    },
    has(name: string) {
      return map.has(name);
    },
    names() {
      return [...map.keys()];
    },
    parsePayload<TPayload extends WorkflowPayload>(
      workflow: RegisteredWorkflow<TPayload>,
      payload: unknown,
    ): TPayload {
      return parseWorkflowPayload(workflow.schema, payload);
    },
  };
}

export function parseWorkflowPayload<TPayload extends WorkflowPayload>(
  schema: WorkflowSchema<TPayload> | undefined,
  payload: unknown,
): TPayload {
  if (!schema) return payload as TPayload;

  if (typeof schema === "function") {
    return schema(payload);
  }

  if ("parse" in schema) {
    return schema.parse(payload);
  }

  const result = schema.safeParse(payload);
  if (result.success) return result.data;

  throw new WorkflowValidationError(
    `Workflow payload validation failed: ${String(result.error)}`,
  );
}
