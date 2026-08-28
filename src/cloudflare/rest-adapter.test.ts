import { describe, expect, test } from "bun:test";
import type { WorkflowEventEnvelope } from "../core/types";
import { CloudflareRestWorkflowAdapter } from "./rest-adapter";

function envelope(): WorkflowEventEnvelope {
  return {
    id: "wf_1",
    name: "email/send",
    payload: { tenantId: "tenant_1" },
    traceId: "trace_1",
    idempotencyKey: "idem_1",
    createdAt: "2026-05-24T09:00:00.000Z",
  };
}

describe("CloudflareRestWorkflowAdapter", () => {
  test("creates Workflow instances using the Cloudflare REST API shape", async () => {
    const requests: Array<{ url: string; body: unknown }> = [];
    const adapter = new CloudflareRestWorkflowAdapter({
      accountId: "acct_1",
      apiToken: "token",
      workflowName: (name) => name.replace("/", "-"),
      baseUrl: "https://api.example.test/client/v4",
      fetch: async (input, init) => {
        requests.push({
          url: String(input),
          body: JSON.parse(String(init?.body)),
        });
        return Response.json({
          success: true,
          result: { id: "wf_1", status: "queued" },
        });
      },
    });

    await expect(adapter.dispatch(envelope())).resolves.toMatchObject({
      id: "wf_1",
      name: "email/send",
      status: "queued",
    });
    expect(requests[0]).toEqual({
      url: "https://api.example.test/client/v4/accounts/acct_1/workflows/email-send/instances",
      body: {
        instance_id: "wf_1",
        params: JSON.stringify(envelope()),
        instance_retention: {
          success_retention: "1 day",
          error_retention: "3 days",
        },
      },
    });
  });

  test("uses the bulk API in bounded chunks with retention on every item", async () => {
    const batchSizes: number[] = [];
    const adapter = new CloudflareRestWorkflowAdapter({
      accountId: "acct_1",
      apiToken: "token",
      workflowName: "email-workflow",
      baseUrl: "https://api.example.test/client/v4",
      fetch: async (input, init) => {
        expect(String(input)).toEndWith("/instances/batch");
        const body = JSON.parse(String(init?.body)) as Array<{
          instance_id: string;
          params: string;
          instance_retention: unknown;
        }>;
        batchSizes.push(body.length);
        expect(body.every((item) => typeof item.params === "string")).toBe(true);
        expect(
          body.every((item) => item.instance_retention !== undefined),
        ).toBe(true);
        return Response.json({
          success: true,
          result: body.map((item) => ({
            id: item.instance_id,
            status: "queued",
          })),
        });
      },
    });
    const envelopes = Array.from({ length: 205 }, (_, index) => ({
      ...envelope(),
      id: `wf_${index}`,
      traceId: `trace_${index}`,
      idempotencyKey: `idem_${index}`,
    }));

    const instances = await adapter.dispatchBatch(envelopes);

    expect(batchSizes).toEqual([100, 100, 5]);
    expect(instances).toHaveLength(205);
    expect(instances[204]?.id).toBe("wf_204");
  });

  test("evicts old REST status-name hints at the configured LRU bound", async () => {
    const requests: string[] = [];
    const adapter = new CloudflareRestWorkflowAdapter({
      accountId: "acct_1",
      apiToken: "token",
      workflowName: (name) => name.replace("/", "-"),
      instanceNameCacheSize: 2,
      baseUrl: "https://api.example.test/client/v4",
      fetch: async (input, init) => {
        requests.push(`${init?.method ?? "GET"} ${String(input)}`);
        if (init?.method === "POST") {
          const body = JSON.parse(String(init.body)) as Array<{
            instance_id: string;
          }>;
          return Response.json({
            success: true,
            result: body.map((item) => ({
              id: item.instance_id,
              status: "queued",
            })),
          });
        }
        return Response.json({
          success: true,
          result: { id: "wf_2", status: "running" },
        });
      },
    });

    await adapter.dispatchBatch([
      { ...envelope(), id: "wf_1", name: "email/first" },
      { ...envelope(), id: "wf_2", name: "email/second" },
      { ...envelope(), id: "wf_3", name: "email/third" },
    ]);

    await expect(adapter.getInstance("wf_1")).rejects.toThrow(
      "requires options.name",
    );
    await expect(adapter.getInstance("wf_2")).resolves.toMatchObject({
      id: "wf_2",
      status: "running",
    });
    expect(requests.at(-1)).toEndWith(
      "/workflows/email-second/instances/wf_2",
    );
  });

  test("fails closed when a batch result omits an unverified existing id", async () => {
    let requests = 0;
    const adapter = new CloudflareRestWorkflowAdapter({
      accountId: "acct_1",
      apiToken: "token",
      workflowName: "email-workflow",
      baseUrl: "https://api.example.test/client/v4",
      fetch: async () => {
        requests += 1;
        return Response.json({
          success: true,
          result: [{ id: "wf_1", status: "queued" }],
        });
      },
    });

    try {
      await adapter.dispatchBatch([
        envelope(),
        {
          ...envelope(),
          id: "wf_2",
          traceId: "trace_2",
          idempotencyKey: "idem_2",
        },
      ]);
      throw new Error("dispatchBatch should have failed");
    } catch (error) {
      expect((error as Error).message).toContain("omitted 1 instance");
      expect((error as Error).message).toContain("refusing unverified existing ids");
      expect((error as { dispatchedIds?: string[] }).dispatchedIds).toEqual([
        "wf_1",
      ]);
      expect((error as { failedIds?: string[] }).failedIds).toEqual(["wf_2"]);
    }
    expect(requests).toBe(1);
  });

  test("does not add status reads when every batch id is omitted", async () => {
    let requests = 0;
    const adapter = new CloudflareRestWorkflowAdapter({
      accountId: "acct_1",
      apiToken: "token",
      workflowName: "email-workflow",
      baseUrl: "https://api.example.test/client/v4",
      fetch: async () => {
        requests += 1;
        return Response.json({ success: true, result: [] });
      },
    });
    const envelopes = Array.from({ length: 20 }, (_, index) => ({
      ...envelope(),
      id: `wf_${index}`,
      traceId: `trace_${index}`,
      idempotencyKey: `idem_${index}`,
    }));

    try {
      await adapter.dispatchBatch(envelopes);
      throw new Error("dispatchBatch should have failed");
    } catch (error) {
      expect((error as Error).message).toContain("omitted 20 instance");
      expect((error as { dispatchedIds?: string[] }).dispatchedIds).toEqual([]);
      expect((error as { failedIds?: string[] }).failedIds).toEqual(
        envelopes.map((item) => item.id),
      );
    }
    expect(requests).toBe(1);
  });

  test("rejects invalid envelopes before calling the REST API", async () => {
    let requests = 0;
    const adapter = new CloudflareRestWorkflowAdapter({
      accountId: "acct_1",
      apiToken: "token",
      workflowName: "email-workflow",
      fetch: async () => {
        requests += 1;
        return Response.json({ success: true, result: {} });
      },
    });

    await expect(
      adapter.dispatch({ ...envelope(), id: "bad/id" }),
    ).rejects.toThrow("must match");
    expect(requests).toBe(0);
  });

  test("bounds Cloudflare API error bodies", async () => {
    const adapter = new CloudflareRestWorkflowAdapter({
      accountId: "acct_1",
      apiToken: "token",
      workflowName: "email-workflow",
      fetch: async () => new Response("x".repeat(20_000), { status: 500 }),
    });

    try {
      await adapter.dispatch(envelope());
      throw new Error("dispatch should fail");
    } catch (error) {
      expect(error).toBeInstanceOf(Error);
      expect((error as Error).message.length).toBeLessThan(4_300);
      expect((error as Error).message).toEndWith("…");
    }
  });

  for (const status of [400, 401, 403, 404, 422]) {
    test(`marks permanent Cloudflare API HTTP ${status} failures as non-retryable`, async () => {
      const adapter = new CloudflareRestWorkflowAdapter({
        accountId: "acct_1",
        apiToken: "token",
        workflowName: "email-workflow",
        fetch: async () =>
          Response.json(
            { success: false, errors: [{ code: 1000, message: "Invalid request" }] },
            { status },
          ),
      });

      try {
        await adapter.dispatch(envelope());
        throw new Error("dispatch should fail");
      } catch (error) {
        expect((error as { nonRetryable?: boolean }).nonRetryable).toBe(true);
      }
    });
  }

  for (const status of [429, 503]) {
    test(`keeps Cloudflare API HTTP ${status} failures retryable`, async () => {
      const adapter = new CloudflareRestWorkflowAdapter({
        accountId: "acct_1",
        apiToken: "token",
        workflowName: "email-workflow",
        fetch: async () =>
          Response.json(
            {
              success: false,
              errors: [{ code: 10000, message: "Authentication error" }],
            },
            { status },
          ),
      });

      try {
        await adapter.dispatch(envelope());
        throw new Error("dispatch should fail");
      } catch (error) {
        expect((error as { nonRetryable?: boolean }).nonRetryable).toBeUndefined();
      }
    });
  }

  test("keeps network failures retryable", async () => {
    const adapter = new CloudflareRestWorkflowAdapter({
      accountId: "acct_1",
      apiToken: "token",
      workflowName: "email-workflow",
      fetch: async () => {
        throw new TypeError("network unavailable");
      },
    });

    try {
      await adapter.dispatch(envelope());
      throw new Error("dispatch should fail");
    } catch (error) {
      expect((error as { nonRetryable?: boolean }).nonRetryable).toBeUndefined();
    }
  });

  test("marks Cloudflare validation failures in successful HTTP envelopes as non-retryable", async () => {
    const adapter = new CloudflareRestWorkflowAdapter({
      accountId: "acct_1",
      apiToken: "token",
      workflowName: "email-workflow",
      fetch: async () =>
        Response.json({
          success: false,
          errors: [{ code: 1001, message: "Workflow configuration is invalid" }],
        }),
    });

    try {
      await adapter.dispatch(envelope());
      throw new Error("dispatch should fail");
    } catch (error) {
      expect((error as { nonRetryable?: boolean }).nonRetryable).toBe(true);
    }
  });

  test("fails closed on an existing id without a durable envelope receipt", async () => {
    const requests: Array<{ url: string; method: string }> = [];
    const adapter = new CloudflareRestWorkflowAdapter({
      accountId: "acct_1",
      apiToken: "token",
      workflowName: (name) => name.replace("/", "-"),
      baseUrl: "https://api.example.test/client/v4",
      fetch: async (input, init) => {
        requests.push({
          url: String(input),
          method: init?.method ?? "GET",
        });
        return Response.json(
          {
            success: false,
            errors: [{ code: 1002, message: "Workflow instance already exists" }],
          },
          { status: 409 },
        );
      },
    });

    try {
      await adapter.dispatch(envelope());
      throw new Error("dispatch should fail");
    } catch (error) {
      expect((error as Error).message).toContain("responded with 409");
      expect((error as { nonRetryable?: boolean }).nonRetryable).toBe(true);
    }
    expect(requests).toEqual([
      {
        url: "https://api.example.test/client/v4/accounts/acct_1/workflows/email-send/instances",
        method: "POST",
      },
    ]);
  });

  test("does not add a status lookup after a generic create conflict", async () => {
    let requests = 0;
    const adapter = new CloudflareRestWorkflowAdapter({
      accountId: "acct_1",
      apiToken: "token",
      workflowName: "email-workflow",
      fetch: async () => {
        requests += 1;
        return Response.json(
          { success: false, errors: [{ code: 1002, message: "Conflict" }] },
          { status: 409 },
        );
      },
    });

    try {
      await adapter.dispatch(envelope());
      throw new Error("dispatch should fail");
    } catch (error) {
      expect((error as Error).message).toContain("responded with 409");
      expect((error as { nonRetryable?: boolean }).nonRetryable).toBe(true);
    }
    expect(requests).toBe(1);
  });

  test("gets Workflow status through the Cloudflare REST API", async () => {
    const adapter = new CloudflareRestWorkflowAdapter({
      accountId: "acct_1",
      apiToken: "token",
      workflowName: "email-workflow",
      baseUrl: "https://api.example.test/client/v4",
      fetch: async () =>
        Response.json({
          success: true,
          result: {
            id: "wf_1",
            status: "complete",
            output: { ok: true },
          },
        }),
    });

    await expect(adapter.getInstance("wf_1", { name: "email/send" })).resolves.toMatchObject({
      id: "wf_1",
      status: "complete",
      output: { ok: true },
    });
  });

  test("normalizes Cloudflare terminal status names", async () => {
    const adapter = new CloudflareRestWorkflowAdapter({
      accountId: "acct_1",
      apiToken: "token",
      workflowName: "email-workflow",
      baseUrl: "https://api.example.test/client/v4",
      fetch: async () =>
        Response.json({
          success: true,
          result: {
            id: "wf_1",
            status: "completed",
          },
        }),
    });

    await expect(adapter.getInstance("wf_1", { name: "email/send" })).resolves.toMatchObject({
      status: "complete",
    });
  });
});
