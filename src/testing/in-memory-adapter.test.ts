import { describe, expect, test } from "bun:test";
import { createWorkflowClient } from "../core/client";
import { InMemoryWorkflowAdapter } from "./in-memory-adapter";

describe("in-memory adapter", () => {
  test("deduplicates claimed cron runs", async () => {
    const adapter = new InMemoryWorkflowAdapter();
    const client = createWorkflowClient({
      adapter,
      idGenerator: () => "wf_cron",
      now: () => new Date("2026-05-24T09:00:00.000Z"),
    });
    const result = await client.dispatch("billing/daily", {});
    const envelope = result.envelopes[0]!;

    await expect(adapter.claimCronRun("run-key", envelope)).resolves.toBe(true);
    await expect(adapter.claimCronRun("run-key", envelope)).resolves.toBe(false);
  });
});
