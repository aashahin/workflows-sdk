import type {
  RegisteredWorkflow,
  WorkflowDefinition,
  WorkflowPayload,
} from "./types";

export function defineWorkflow<
  const TName extends string,
  TPayload extends WorkflowPayload = WorkflowPayload,
  TResult = unknown,
  TServices = unknown,
>(
  name: TName,
  definition: WorkflowDefinition<TPayload, TResult, TServices>,
): RegisteredWorkflow<TPayload, TResult, TServices> & { name: TName } {
  return {
    ...definition,
    name,
  };
}
