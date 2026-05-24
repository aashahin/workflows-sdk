import { describe, expect, test } from "bun:test";
import type { WorkflowEventEnvelope } from "../core/types";
import { SignedHttpAdapter } from "./signed-http-adapter";

function envelope(name = "email/send"): WorkflowEventEnvelope {
  return {
    id: "wf_1",
    name,
    payload: { tenantId: "tenant_1" },
    traceId: "trace_1",
    idempotencyKey: "idem_1",
    createdAt: "2026-05-24T09:00:00.000Z",
  };
}

describe("SignedHttpAdapter", () => {
  test("marks permanent dispatcher validation errors as non-retryable", async () => {
    const adapter = new SignedHttpAdapter({
      baseUrl: "https://workflows.example.test",
      authToken: "token",
      fetch: (async () =>
        Response.json({
          ids: [],
          errors: [{ id: "wf_1", error: "Unknown event: email/nope" }],
        })) as unknown as typeof fetch,
    });

    try {
      await adapter.dispatch(envelope("email/nope"));
      throw new Error("dispatch should have failed");
    } catch (error) {
      expect(error).toBeInstanceOf(Error);
      expect((error as { nonRetryable?: boolean }).nonRetryable).toBe(true);
    }
  });

  test("keeps ambiguous dispatcher item errors retryable", async () => {
    const adapter = new SignedHttpAdapter({
      baseUrl: "https://workflows.example.test",
      authToken: "token",
      fetch: (async () =>
        Response.json({
          ids: [],
          errors: [{ id: "wf_1", error: "Cloudflare API rate limited" }],
        })) as unknown as typeof fetch,
    });

    try {
      await adapter.dispatch(envelope());
      throw new Error("dispatch should have failed");
    } catch (error) {
      expect(error).toBeInstanceOf(Error);
      expect((error as { nonRetryable?: boolean }).nonRetryable).toBeUndefined();
    }
  });

  test("exposes successful and failed ids for partial batch failures", async () => {
    const adapter = new SignedHttpAdapter({
      baseUrl: "https://workflows.example.test",
      authToken: "token",
      fetch: (async () =>
        Response.json({
          ids: ["wf_ok"],
          errors: [{ id: "wf_bad", error: "Unknown event: email/nope" }],
        })) as unknown as typeof fetch,
    });

    try {
      await adapter.dispatchBatch([
        { ...envelope("email/send"), id: "wf_ok" },
        { ...envelope("email/nope"), id: "wf_bad" },
      ]);
      throw new Error("dispatchBatch should have failed");
    } catch (error) {
      expect(error).toBeInstanceOf(Error);
      expect((error as { dispatchedIds?: string[] }).dispatchedIds).toEqual([
        "wf_ok",
      ]);
      expect((error as { failedIds?: string[] }).failedIds).toEqual(["wf_bad"]);
      expect((error as { nonRetryable?: boolean }).nonRetryable).toBe(true);
    }
  });
});
