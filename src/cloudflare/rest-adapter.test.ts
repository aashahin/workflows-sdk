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
        params: envelope(),
      },
    });
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
