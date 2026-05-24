import { describe, expect, test } from "bun:test";
import { defineWorkflow } from "../core/definition";
import {
  collectDueCronRuns,
  createCronRunKey,
  getNextCronDate,
  normalizeCronDefinitions,
} from "./cron";

describe("cron scheduler", () => {
  test("creates deterministic cron run keys", () => {
    expect(
      createCronRunKey(
        "billing/trial-reminder",
        "daily",
        new Date("2026-05-24T09:00:00.000Z"),
      ),
    ).toBe("billing/trial-reminder:daily:2026-05-24T09:00:00.000Z");
  });

  test("normalizes cron definitions", () => {
    const workflow = defineWorkflow("billing/trial-reminder", {
      cron: "0 9 * * *",
      run: () => undefined,
    });

    expect(normalizeCronDefinitions(workflow)).toEqual([
      {
        name: "billing/trial-reminder#1",
        schedule: "0 9 * * *",
        missedRunPolicy: "skip",
      },
    ]);
  });

  test("collects due runs with deterministic payload metadata", () => {
    const workflow = defineWorkflow("billing/trial-reminder", {
      cron: [{ name: "daily", schedule: "0 9 * * *", payload: { plan: "pro" } }],
      run: () => undefined,
    });

    const runs = collectDueCronRuns(
      workflow,
      new Date("2026-05-24T09:05:00.000Z"),
    );

    expect(runs[0]).toMatchObject({
      workflowName: "billing/trial-reminder",
      cronName: "daily",
      payload: { plan: "pro" },
      runKey: "billing/trial-reminder:daily:2026-05-24T09:00:00.000Z",
    });
  });

  test("uses Bun.cron.parse return value correctly", () => {
    expect(
      getNextCronDate(
        "0 9 * * *",
        new Date("2026-05-24T08:59:59.000Z"),
      ).toISOString(),
    ).toBe("2026-05-24T09:00:00.000Z");
  });

  test("collects catch-up-all missed cron runs", () => {
    const workflow = defineWorkflow("billing/hourly", {
      cron: [
        {
          name: "hourly",
          schedule: "0 * * * *",
          missedRunPolicy: { mode: "catch-up-all", maxRuns: 3 },
        },
      ],
      run: () => undefined,
    });

    expect(
      collectDueCronRuns(
        workflow,
        new Date("2026-05-24T03:05:00.000Z"),
      ).map((run) => run.scheduledAt.toISOString()),
    ).toEqual([
      "2026-05-24T01:00:00.000Z",
      "2026-05-24T02:00:00.000Z",
      "2026-05-24T03:00:00.000Z",
    ]);
  });

  test("collects OS cron runs in the provided local timezone", () => {
    const workflow = defineWorkflow("billing/local-daily", {
      cron: [{ name: "daily", schedule: "0 9 * * *" }],
      run: () => undefined,
    });

    const runs = collectDueCronRuns(
      workflow,
      new Date("2026-05-24T07:05:00.000Z"),
      "Africa/Cairo",
    );

    expect(runs[0]?.scheduledAt.toISOString()).toBe("2026-05-24T06:00:00.000Z");
  });
});
