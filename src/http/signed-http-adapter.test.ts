import { describe, expect, test } from "bun:test";
import type { WorkflowEventEnvelope } from "../core/types";
import { SignedHttpAdapter } from "./signed-http-adapter";

type FetchInput = Parameters<typeof fetch>[0];
type FetchInit = Parameters<typeof fetch>[1];

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

  test("marks durable receipt hash conflicts as non-retryable", async () => {
    const adapter = new SignedHttpAdapter({
      baseUrl: "https://workflows.example.test",
      authToken: "token",
      fetch: (async () =>
        Response.json({
          ids: [],
          errors: [
            {
              id: "wf_1",
              error:
                "Cloudflare Workflow receipt conflict for wf_1: the instance id is reserved for a different canonical envelope",
            },
          ],
        })) as unknown as typeof fetch,
    });

    try {
      await adapter.dispatch(envelope());
      throw new Error("dispatch should have failed");
    } catch (error) {
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

  test("marks auth and config HTTP failures as non-retryable", async () => {
    for (const status of [401, 403]) {
      const adapter = new SignedHttpAdapter({
        baseUrl: "https://workflows.example.test",
        authToken: "token",
        fetch: (async () =>
          new Response("Authorization rejected", {
            status,
          })) as unknown as typeof fetch,
      });

      try {
        await adapter.dispatch(envelope());
        throw new Error("dispatch should have failed");
      } catch (error) {
        expect(error).toBeInstanceOf(Error);
        expect((error as { nonRetryable?: boolean }).nonRetryable).toBe(true);
      }
    }
  });

  test("marks permanent request-shape HTTP failures as non-retryable", async () => {
    const adapter = new SignedHttpAdapter({
      baseUrl: "https://workflows.example.test",
      authToken: "token",
      fetch: (async () =>
        Response.json({ error: "Missing events array" }, { status: 400 })) as unknown as typeof fetch,
    });

    try {
      await adapter.dispatch(envelope());
      throw new Error("dispatch should have failed");
    } catch (error) {
      expect(error).toBeInstanceOf(Error);
      expect((error as { nonRetryable?: boolean }).nonRetryable).toBe(true);
    }
  });

  test("marks deterministic request-size failures as non-retryable", async () => {
    const adapter = new SignedHttpAdapter({
      baseUrl: "https://workflows.example.test",
      authToken: "token",
      fetch: (async () =>
        Response.json(
          { error: "Too many events; maximum is 100" },
          { status: 413 },
        )) as unknown as typeof fetch,
    });

    try {
      await adapter.dispatch(envelope());
      throw new Error("dispatch should have failed");
    } catch (error) {
      expect((error as { nonRetryable?: boolean }).nonRetryable).toBe(true);
    }
  });

  test("chunks dispatches to at most 100 events per request", async () => {
    const batchSizes: number[] = [];
    const adapter = new SignedHttpAdapter({
      baseUrl: "https://workflows.example.test",
      authToken: "token",
      fetch: (async (_input: FetchInput, init?: FetchInit) => {
        const body = JSON.parse(String(init?.body)) as {
          events: WorkflowEventEnvelope[];
        };
        batchSizes.push(body.events.length);
        return Response.json({
          instances: body.events.map((event) => ({
            id: event.id,
            name: event.name,
            status: "queued",
          })),
        });
      }) as unknown as typeof fetch,
    });
    const events = Array.from({ length: 205 }, (_, index) => ({
      ...envelope(),
      id: `wf_${index}`,
      traceId: `trace_${index}`,
      idempotencyKey: `idem_${index}`,
    }));

    await expect(adapter.dispatchBatch(events)).resolves.toHaveLength(205);
    expect(batchSizes).toEqual([100, 100, 5]);
  });

  test("evicts old status-name hints at the configured LRU bound", async () => {
    const statusUrls: string[] = [];
    const adapter = new SignedHttpAdapter({
      baseUrl: "https://workflows.example.test",
      authToken: "token",
      instanceNameCacheSize: 2,
      fetch: (async (input: FetchInput, init?: FetchInit) => {
        if (init?.method === "POST") {
          const body = JSON.parse(String(init.body)) as {
            events: WorkflowEventEnvelope[];
          };
          return Response.json({
            instances: body.events.map((event) => ({
              id: event.id,
              name: event.name,
              status: "queued",
            })),
          });
        }
        statusUrls.push(String(input));
        return new Response(null, { status: 404 });
      }) as unknown as typeof fetch,
    });

    await adapter.dispatchBatch([
      { ...envelope("email/first"), id: "wf_1" },
      { ...envelope("email/second"), id: "wf_2" },
      { ...envelope("email/third"), id: "wf_3" },
    ]);
    await expect(adapter.getInstance("wf_1")).rejects.toThrow(
      "requires options.name or a prior dispatch",
    );
    await expect(adapter.getInstance("wf_2")).resolves.toBeNull();

    expect(statusUrls).toEqual([
      "https://workflows.example.test/status/wf_2?name=email%2Fsecond",
    ]);
  });

  test("fails before a cold-start status request when workflow name is unknown", async () => {
    let requests = 0;
    const adapter = new SignedHttpAdapter({
      baseUrl: "https://workflows.example.test",
      authToken: "token",
      fetch: (async () => {
        requests += 1;
        return new Response(null, { status: 404 });
      }) as unknown as typeof fetch,
    });

    try {
      await adapter.getInstance("wf_1");
      throw new Error("getInstance should fail");
    } catch (error) {
      expect((error as Error).message).toContain("requires options.name");
      expect((error as { nonRetryable?: boolean }).nonRetryable).toBe(true);
    }
    expect(requests).toBe(0);
  });

  test("rejects invalid instance-name cache bounds", () => {
    expect(
      () =>
        new SignedHttpAdapter({
          baseUrl: "https://workflows.example.test",
          authToken: "token",
          instanceNameCacheSize: -1,
        }),
    ).toThrow("non-negative integer");
  });

  test("cancels a not-found status body before returning", async () => {
    let cancelled = false;
    const adapter = new SignedHttpAdapter({
      baseUrl: "https://workflows.example.test",
      authToken: "token",
      fetch: (async () =>
        new Response(
          new ReadableStream({
            cancel() {
              cancelled = true;
            },
          }),
          { status: 404 },
        )) as unknown as typeof fetch,
    });

    await expect(
      adapter.getInstance("wf_404", { name: "email/send" }),
    ).resolves.toBeNull();
    expect(cancelled).toBe(true);
  });

  test("bounds dispatcher error bodies", async () => {
    const adapter = new SignedHttpAdapter({
      baseUrl: "https://workflows.example.test",
      authToken: "token",
      fetch: (async () =>
        new Response("x".repeat(20_000), { status: 500 })) as unknown as typeof fetch,
    });

    try {
      await adapter.dispatch(envelope());
      throw new Error("dispatch should have failed");
    } catch (error) {
      expect((error as Error).message.length).toBeLessThan(4_300);
      expect((error as Error).message).toEndWith("…");
    }
  });

  test("bounds successful dispatcher response bodies", async () => {
    const adapter = new SignedHttpAdapter({
      baseUrl: "https://workflows.example.test",
      authToken: "token",
      fetch: (async () =>
        Response.json({
          ids: ["wf_1"],
          padding: "x".repeat(1_100_000),
        })) as unknown as typeof fetch,
    });

    await expect(adapter.dispatch(envelope())).rejects.toThrow(
      "Workflow dispatcher response exceeds 1000000 bytes",
    );
  });

  test("URL-encodes status ids and bounds successful status responses", async () => {
    const urls: string[] = [];
    const adapter = new SignedHttpAdapter({
      baseUrl: "https://workflows.example.test",
      authToken: "token",
      fetch: (async (input: FetchInput) => {
        urls.push(String(input));
        return Response.json({
          id: "wf/with?segments",
          name: "email/send",
          status: "queued",
        });
      }) as unknown as typeof fetch,
    });

    await expect(
      adapter.getInstance("wf/with?segments", { name: "email/send" }),
    ).resolves.toMatchObject({ id: "wf/with?segments", status: "queued" });
    expect(urls).toEqual([
      "https://workflows.example.test/status/wf%2Fwith%3Fsegments?name=email%2Fsend",
    ]);

    const oversizedAdapter = new SignedHttpAdapter({
      baseUrl: "https://workflows.example.test",
      authToken: "token",
      fetch: (async () =>
        Response.json({
          id: "wf_1",
          name: "email/send",
          status: "queued",
          padding: "x".repeat(1_100_000),
        })) as unknown as typeof fetch,
    });
    await expect(
      oversizedAdapter.getInstance("wf_1", { name: "email/send" }),
    ).rejects.toThrow("Workflow status response exceeds 1000000 bytes");
  });

  test("marks status auth failures as non-retryable", async () => {
    for (const status of [401, 403]) {
      const adapter = new SignedHttpAdapter({
        baseUrl: "https://workflows.example.test",
        authToken: "token",
        fetch: (async () =>
          new Response("Authorization rejected", {
            status,
          })) as unknown as typeof fetch,
      });

      try {
        await adapter.getInstance("wf_1", { name: "email/send" });
        throw new Error("getInstance should have failed");
      } catch (error) {
        expect((error as { nonRetryable?: boolean }).nonRetryable).toBe(true);
      }
    }
  });

  test("applies the configured timeout to status lookups", async () => {
    const observedSignals: AbortSignal[] = [];
    const adapter = new SignedHttpAdapter({
      baseUrl: "https://workflows.example.test",
      authToken: "token",
      timeoutMs: 1,
      fetch: ((
        _input: FetchInput,
        init?: FetchInit,
      ) => {
        const signal = init?.signal;
        if (signal) observedSignals.push(signal);
        return new Promise<Response>((_resolve, reject) => {
          signal?.addEventListener(
            "abort",
            () => reject(new DOMException("aborted", "AbortError")),
            { once: true },
          );
        });
      }) as unknown as typeof fetch,
    });

    await expect(
      adapter.getInstance("wf_1", { name: "email/send" }),
    ).rejects.toThrow("Failed to get workflow status");
    expect(observedSignals[0]?.aborted).toBe(true);
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
