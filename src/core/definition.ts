import type {
  RegisteredWorkflow,
  WorkflowDefinition,
  WorkflowPayload,
} from "./types";

export function defineWorkflow<
  const TName extends string,
  TPayload extends WorkflowPayload = WorkflowPayload,
  TResult = unknown,
>(
  name: TName,
  definition: WorkflowDefinition<TPayload, TResult>,
): RegisteredWorkflow<TPayload, TResult> & { name: TName } {
  return {
    ...definition,
    name,
  };
}
