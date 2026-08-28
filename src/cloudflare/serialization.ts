import type { WorkflowEventEnvelope } from "../core/types";

/**
 * Keep one event comfortably below the 1 MiB Workflow RPC ceiling and the
 * 128 KB Queue ceiling used for failed-event recovery. The tighter product
 * limit leaves room for recovery metadata and Queue framing.
 */
export const MAX_CLOUDFLARE_WORKFLOW_EVENT_BYTES = 96_000;

const MAX_SERIALIZATION_DEPTH = 64;
const MAX_SERIALIZATION_NODES = 50_000;
export const MAX_CLOUDFLARE_SCHEDULE_AHEAD_MS = 365 * 24 * 60 * 60 * 1_000;
const CLOUDFLARE_INSTANCE_ID = /^[A-Za-z0-9_][A-Za-z0-9_-]*$/;
const CLOUDFLARE_RESERVED_INSTANCE_ID = /^cf_[0-9a-f]{64}$/;

export function assertCloudflareWorkflowEnvelope(
  value: unknown,
): asserts value is WorkflowEventEnvelope {
  if (!isPlainRecord(value)) {
    throw new Error("Workflow event must be a plain object");
  }

  assertCloudflareInstanceId(value.id);
  assertCanonicalString(value.name, "event.name", 256);
  assertCanonicalString(value.traceId, "event.traceId", 256);
  assertCanonicalString(value.idempotencyKey, "event.idempotencyKey", 512);
  assertIsoDate(value.createdAt, "event.createdAt");

  if (value.scheduledAt !== undefined) {
    const scheduledAt = assertIsoDate(value.scheduledAt, "event.scheduledAt");
    if (scheduledAt - Date.now() > MAX_CLOUDFLARE_SCHEDULE_AHEAD_MS) {
      throw new Error("event.scheduledAt exceeds Cloudflare's 365-day horizon");
    }
  }
  if (!isPlainRecord(value.payload)) {
    throw new Error("Workflow event payload must be a plain object");
  }
  if (value.metadata !== undefined && !isPlainRecord(value.metadata)) {
    throw new Error("Workflow event metadata must be a plain object");
  }

  const bytes = assertCloudflareJsonSerializable(value, "Workflow event");
  if (bytes > MAX_CLOUDFLARE_WORKFLOW_EVENT_BYTES) {
    throw new Error(
      `Workflow event exceeds ${MAX_CLOUDFLARE_WORKFLOW_EVENT_BYTES} bytes`,
    );
  }
}

/**
 * Workflow parameters are documented as JSON-serializable. Validate before a
 * binding call so functions, bigint, cycles, class instances, accessors, and
 * non-finite numbers fail at the caller with a deterministic error.
 */
export function assertCloudflareJsonSerializable(
  value: unknown,
  label: string,
): number {
  const seen = new Set<object>();
  let nodes = 0;

  const visit = (candidate: unknown, path: string, depth: number): void => {
    nodes += 1;
    if (nodes > MAX_SERIALIZATION_NODES) {
      throw new Error(`${label} contains too many values`);
    }
    if (depth > MAX_SERIALIZATION_DEPTH) {
      throw new Error(`${label} exceeds the maximum nesting depth at ${path}`);
    }

    if (
      candidate === null ||
      typeof candidate === "string" ||
      typeof candidate === "boolean"
    ) {
      return;
    }
    if (typeof candidate === "number") {
      if (!Number.isFinite(candidate)) {
        throw new Error(`${label} contains a non-finite number at ${path}`);
      }
      return;
    }
    if (typeof candidate !== "object") {
      throw new Error(
        `${label} contains non-JSON value ${typeof candidate} at ${path}`,
      );
    }

    if (seen.has(candidate)) {
      throw new Error(`${label} contains a cycle at ${path}`);
    }
    seen.add(candidate);
    try {
      if (Array.isArray(candidate)) {
        for (let index = 0; index < candidate.length; index += 1) {
          visit(candidate[index], `${path}[${index}]`, depth + 1);
        }
        return;
      }

      if (!isPlainRecord(candidate)) {
        throw new Error(`${label} contains a non-plain object at ${path}`);
      }
      if (Object.getOwnPropertySymbols(candidate).length > 0) {
        throw new Error(`${label} contains a symbol key at ${path}`);
      }
      for (const key of Object.keys(candidate)) {
        const descriptor = Object.getOwnPropertyDescriptor(candidate, key);
        if (!descriptor || !("value" in descriptor)) {
          throw new Error(`${label} contains an accessor at ${path}.${key}`);
        }
        visit(descriptor.value, `${path}.${key}`, depth + 1);
      }
    } finally {
      seen.delete(candidate);
    }
  };

  visit(value, "$", 0);
  const encoded = JSON.stringify(value);
  if (encoded === undefined) {
    throw new Error(`${label} cannot be encoded as JSON`);
  }
  return new TextEncoder().encode(encoded).byteLength;
}

function assertCloudflareInstanceId(value: unknown): asserts value is string {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    value.length > 100 ||
    !CLOUDFLARE_INSTANCE_ID.test(value) ||
    CLOUDFLARE_RESERVED_INSTANCE_ID.test(value)
  ) {
    throw new Error(
      "event.id must match ^[A-Za-z0-9_][A-Za-z0-9_-]*$, contain at most 100 characters, and not use Cloudflare's reserved cf_<64 lowercase hex> form",
    );
  }
}

function assertCanonicalString(
  value: unknown,
  label: string,
  maxLength: number,
): asserts value is string {
  if (typeof value !== "string" || value.length === 0 || value.length > maxLength) {
    throw new Error(`${label} must contain 1-${maxLength} characters`);
  }
  if (value.trim() !== value || /[\u0000-\u001f\u007f]/.test(value)) {
    throw new Error(
      `${label} must not contain surrounding whitespace or control characters`,
    );
  }
}

function assertIsoDate(value: unknown, label: string): number {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`${label} must be a canonical ISO date string`);
  }
  const timestamp = Date.parse(value);
  if (Number.isNaN(timestamp) || new Date(timestamp).toISOString() !== value) {
    throw new Error(`${label} must be a canonical ISO date string`);
  }
  return timestamp;
}

function isPlainRecord(value: unknown): value is Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return false;
  }
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}
