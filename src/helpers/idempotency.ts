// ─── Idempotency Helpers ─────────────────────────────────────────────────────

import type { WorkflowDispatchEvent } from "../core/types";

/**
 * Derive a deterministic idempotency key from a workflow event.
 *
 * Strategy: sort-stable JSON of (name + data), then 64-bit hash.
 * NOTE: This is NOT used by default — the HttpTransport generates a unique key
 * per dispatch. Use this explicitly via `SendOptions.idempotencyKey` when you
 * need content-based deduplication (e.g., preventing double-sends of the same event).
 *
 * ⚠ Uses FNV-1a 64-bit hash — collision probability is negligible for normal
 * volumes (<1M events) but not cryptographically strong.
 */
export function deriveIdempotencyKey(event: WorkflowDispatchEvent): string {
  const payload = JSON.stringify({ n: event.name, d: sortObject(event.data) });
  return hash64(payload);
}

/**
 * Generate a unique event ID (nano-id-style using crypto).
 * Used when the transport needs a per-dispatch identifier.
 */
export function generateEventId(): string {
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  return Array.from(bytes)
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

// ─── Internal ────────────────────────────────────────────────────────────────

/** Recursively sort object keys for deterministic serialization. */
function sortObject(obj: unknown): unknown {
  if (obj === null || obj === undefined) return obj;
  if (Array.isArray(obj)) return obj.map(sortObject);
  if (typeof obj === "object") {
    const sorted: Record<string, unknown> = {};
    for (const key of Object.keys(obj as Record<string, unknown>).sort()) {
      sorted[key] = sortObject((obj as Record<string, unknown>)[key]);
    }
    return sorted;
  }
  return obj;
}

/**
 * Dual-pass FNV-1a producing a 64-bit (16-char hex) hash.
 * Significantly lower collision probability than 32-bit (~50% at 2^32 vs 2^16 events).
 */
function hash64(input: string): string {
  let h1 = 0x811c9dc5;
  let h2 = 0x050c5d1f; // offset basis for second pass
  for (let i = 0; i < input.length; i++) {
    const c = input.charCodeAt(i);
    h1 ^= c;
    h1 = (h1 * 0x01000193) >>> 0;
    h2 ^= c;
    h2 = (h2 * 0x01000193) >>> 0;
  }
  return h1.toString(16).padStart(8, "0") + h2.toString(16).padStart(8, "0");
}
