import { describe, expect, test } from "bun:test";

import type { WorkflowEventEnvelope } from "../core/types";
import { assertCloudflareWorkflowEnvelope } from "./serialization";

function envelope(
  overrides: Partial<WorkflowEventEnvelope> = {},
): WorkflowEventEnvelope {
  return {
    id: "wf_1",
    name: "email/send",
    payload: {},
    traceId: "trace_1",
    idempotencyKey: "idem_1",
    createdAt: "2026-08-27T00:00:00.000Z",
    ...overrides,
  };
}

describe("Cloudflare Workflow envelope validation", () => {
  test("rejects instance IDs outside Cloudflare's grammar", () => {
    expect(() =>
      assertCloudflareWorkflowEnvelope(envelope({ id: "bad/id" })),
    ).toThrow("must match");
    expect(() =>
      assertCloudflareWorkflowEnvelope(envelope({ id: "-bad" })),
    ).toThrow("must match");
    expect(() =>
      assertCloudflareWorkflowEnvelope(
        envelope({ id: `cf_${"a".repeat(64)}` }),
      ),
    ).toThrow("reserved");
  });

  test("rejects non-canonical workflow names before dispatch", () => {
    expect(() =>
      assertCloudflareWorkflowEnvelope(envelope({ name: " email/send " })),
    ).toThrow("surrounding whitespace");
  });

  test("requires canonical ISO timestamps", () => {
    expect(() =>
      assertCloudflareWorkflowEnvelope(
        envelope({ createdAt: "2026-02-30T00:00:00.000Z" }),
      ),
    ).toThrow("canonical ISO");
  });

  test("rejects schedules beyond Cloudflare's 365-day horizon", () => {
    expect(() =>
      assertCloudflareWorkflowEnvelope(
        envelope({
          scheduledAt: new Date(
            Date.now() + 366 * 24 * 60 * 60 * 1_000,
          ).toISOString(),
        }),
      ),
    ).toThrow("365-day horizon");
  });
});
