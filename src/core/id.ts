const DEFAULT_ID_PREFIX = "wf";

export function createWorkflowId(prefix = DEFAULT_ID_PREFIX): string {
  if (typeof crypto !== "undefined" && "randomUUID" in crypto) {
    return `${prefix}_${crypto.randomUUID()}`;
  }

  return `${prefix}_${Date.now().toString(36)}_${Math.random().toString(36).slice(2)}`;
}

export function createTraceId(): string {
  return createWorkflowId("trace");
}

export function createIdempotencyKey(
  name: string,
  payload: unknown,
  scheduledAt?: string,
): string {
  return `${name}:${scheduledAt ?? "now"}:${stableStringify(payload)}`;
}

export function stableStringify(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map((item) => stableStringify(item)).join(",")}]`;
  }

  if (value && typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>).sort(
      ([a], [b]) => a.localeCompare(b),
    );
    return `{${entries
      .map(([key, item]) => `${JSON.stringify(key)}:${stableStringify(item)}`)
      .join(",")}}`;
  }

  return JSON.stringify(value);
}
