import { describe, expect, test } from "bun:test";
import { defineWorkflow } from "../core/definition";
import {
  collectDueCronRuns,
  createCronRunKey,
  getNextCronDate,
  getNextCronDateInTimezone,
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

  test("computes the next daily occurrence", () => {
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

    // 06:30Z is 09:30 in Cairo (UTC+3 in May), so the most recent 09:00 Cairo
    // occurrence is 06:00Z and is still fresh for the default "skip" policy.
    const runs = collectDueCronRuns(
      workflow,
      new Date("2026-05-24T06:30:00.000Z"),
      "Africa/Cairo",
    );

    expect(runs[0]?.scheduledAt.toISOString()).toBe("2026-05-24T06:00:00.000Z");
  });
});

describe("cron field parsing", () => {
  const next = (schedule: string, from: string) =>
    getNextCronDate(schedule, new Date(from)).toISOString();

  test("weekly schedule fires only on the matching day of week", () => {
    // 2026-05-24 is a Sunday; the next Monday 09:00 is the 25th.
    expect(next("0 9 * * 1", "2026-05-24T10:00:00.000Z")).toBe(
      "2026-05-25T09:00:00.000Z",
    );
    // From Monday 08:00 the same day's 09:00 is next.
    expect(next("0 9 * * 1", "2026-05-25T08:00:00.000Z")).toBe(
      "2026-05-25T09:00:00.000Z",
    );
    // From Monday 10:00 the following Monday is next.
    expect(next("0 9 * * 1", "2026-05-25T10:00:00.000Z")).toBe(
      "2026-06-01T09:00:00.000Z",
    );
  });

  test("monthly schedule fires on the first of the month", () => {
    expect(next("0 0 1 * *", "2026-05-24T00:00:00.000Z")).toBe(
      "2026-06-01T00:00:00.000Z",
    );
  });

  test("DOM and DOW are OR-ed when both are restricted", () => {
    // 13th of the month OR any Friday.
    const schedule = "0 0 13 * 5";
    // 2026-05-13 is a Wednesday -> matches via day-of-month.
    expect(next(schedule, "2026-05-12T00:00:00.000Z")).toBe(
      "2026-05-13T00:00:00.000Z",
    );
    // From the 14th the next Friday (15th) matches via day-of-week.
    expect(next(schedule, "2026-05-14T00:00:00.000Z")).toBe(
      "2026-05-15T00:00:00.000Z",
    );
  });

  test("supports lists", () => {
    expect(next("0,30 * * * *", "2026-05-24T09:10:00.000Z")).toBe(
      "2026-05-24T09:30:00.000Z",
    );
    expect(next("0,30 * * * *", "2026-05-24T09:45:00.000Z")).toBe(
      "2026-05-24T10:00:00.000Z",
    );
  });

  test("supports ranges", () => {
    expect(next("0 9-17 * * *", "2026-05-24T17:30:00.000Z")).toBe(
      "2026-05-25T09:00:00.000Z",
    );
    expect(next("0 9-17 * * *", "2026-05-24T12:30:00.000Z")).toBe(
      "2026-05-24T13:00:00.000Z",
    );
  });

  test("supports step values", () => {
    expect(next("*/15 * * * *", "2026-05-24T09:07:00.000Z")).toBe(
      "2026-05-24T09:15:00.000Z",
    );
    // 10-50/20 -> minutes 10, 30, 50.
    expect(next("10-50/20 * * * *", "2026-05-24T09:35:00.000Z")).toBe(
      "2026-05-24T09:50:00.000Z",
    );
    expect(next("10-50/20 * * * *", "2026-05-24T09:55:00.000Z")).toBe(
      "2026-05-24T10:10:00.000Z",
    );
  });

  test("supports month and day names", () => {
    expect(next("0 0 1 JAN *", "2026-05-24T00:00:00.000Z")).toBe(
      "2027-01-01T00:00:00.000Z",
    );
    // SUN == 0. 2026-05-24 is a Sunday.
    expect(next("0 12 * * SUN", "2026-05-24T11:00:00.000Z")).toBe(
      "2026-05-24T12:00:00.000Z",
    );
  });

  test("treats 7 as Sunday", () => {
    expect(next("0 12 * * 7", "2026-05-24T11:00:00.000Z")).toBe(
      "2026-05-24T12:00:00.000Z",
    );
  });

  test("supports an optional seconds field", () => {
    expect(next("30 0 9 * * *", "2026-05-24T09:00:00.000Z")).toBe(
      "2026-05-24T09:00:30.000Z",
    );
  });

  test("handles leap-year February 29th", () => {
    // 2026 and 2027 are not leap years; the next Feb 29 is in 2028.
    expect(next("0 0 29 2 *", "2026-05-24T00:00:00.000Z")).toBe(
      "2028-02-29T00:00:00.000Z",
    );
  });

  test("respects month boundaries", () => {
    // Next 31st after May 31 is July 31 (June has 30 days).
    expect(next("0 0 31 * *", "2026-05-31T12:00:00.000Z")).toBe(
      "2026-07-31T00:00:00.000Z",
    );
  });

  test("next occurrence is strictly monotonic", () => {
    const schedule = "*/15 * * * *";
    let cursor = new Date("2026-05-24T00:00:00.000Z");
    for (let i = 0; i < 200; i++) {
      const upcoming = getNextCronDate(schedule, cursor);
      expect(upcoming.getTime()).toBeGreaterThan(cursor.getTime());
      cursor = upcoming;
    }
  });

  test("throws for malformed expressions", () => {
    expect(() => getNextCronDate("* * *", new Date())).toThrow();
    expect(() => getNextCronDate("99 * * * *", new Date())).toThrow();
    expect(() => getNextCronDate("0 0 * * 8", new Date())).toThrow();
    expect(() => getNextCronDate("*/0 * * * *", new Date())).toThrow();
    expect(() => getNextCronDate("abc * * * *", new Date())).toThrow();
  });
});

describe("timezone projection", () => {
  test("projects daily schedule through America/New_York", () => {
    // 09:00 New York in May (EDT, UTC-4) == 13:00 UTC.
    expect(
      getNextCronDateInTimezone(
        "0 9 * * *",
        new Date("2026-05-24T12:00:00.000Z"),
        "America/New_York",
      ).toISOString(),
    ).toBe("2026-05-24T13:00:00.000Z");
  });

  test("smoke tests DST spring-forward in America/New_York", () => {
    // 2026-03-08: clocks jump from 02:00 to 03:00 EST->EDT.
    // A 02:30 local schedule does not exist that day; the engine still returns
    // a valid future instant rather than throwing.
    const result = getNextCronDateInTimezone(
      "30 2 * * *",
      new Date("2026-03-08T06:00:00.000Z"),
      "America/New_York",
    );
    expect(Number.isNaN(result.getTime())).toBe(false);
    expect(result.getTime()).toBeGreaterThan(
      new Date("2026-03-08T06:00:00.000Z").getTime(),
    );
  });

  test("smoke tests DST fall-back in America/New_York", () => {
    const result = getNextCronDateInTimezone(
      "30 1 * * *",
      new Date("2026-11-01T04:00:00.000Z"),
      "America/New_York",
    );
    expect(Number.isNaN(result.getTime())).toBe(false);
    expect(result.getTime()).toBeGreaterThan(
      new Date("2026-11-01T04:00:00.000Z").getTime(),
    );
  });
});

describe("missedRunPolicy semantics", () => {
  test("skip drops occurrences older than the freshness window", () => {
    const workflow = defineWorkflow("billing/skip", {
      cron: [{ name: "daily", schedule: "0 9 * * *", missedRunPolicy: "skip" }],
      run: () => undefined,
    });

    // Two hours after the 09:00 run, past the default 1h window.
    expect(
      collectDueCronRuns(workflow, new Date("2026-05-24T11:00:00.000Z")),
    ).toHaveLength(0);
  });

  test("skip fires occurrences within the freshness window", () => {
    const workflow = defineWorkflow("billing/skip", {
      cron: [{ name: "daily", schedule: "0 9 * * *", missedRunPolicy: "skip" }],
      run: () => undefined,
    });

    const runs = collectDueCronRuns(
      workflow,
      new Date("2026-05-24T09:30:00.000Z"),
    );
    expect(runs.map((run) => run.scheduledAt.toISOString())).toEqual([
      "2026-05-24T09:00:00.000Z",
    ]);
  });

  test("skip honours a custom maxDelayMs", () => {
    const workflow = defineWorkflow("billing/skip", {
      cron: [
        {
          name: "daily",
          schedule: "0 9 * * *",
          missedRunPolicy: "skip",
          maxDelayMs: 3 * 3_600_000,
        },
      ],
      run: () => undefined,
    });

    const runs = collectDueCronRuns(
      workflow,
      new Date("2026-05-24T11:00:00.000Z"),
    );
    expect(runs.map((run) => run.scheduledAt.toISOString())).toEqual([
      "2026-05-24T09:00:00.000Z",
    ]);
  });

  test("catch-up-latest always fires the most recent missed occurrence", () => {
    const workflow = defineWorkflow("billing/catchup-latest", {
      cron: [
        {
          name: "daily",
          schedule: "0 9 * * *",
          missedRunPolicy: "catch-up-latest",
        },
      ],
      run: () => undefined,
    });

    // A full day late; skip would drop this, catch-up-latest still fires it.
    const runs = collectDueCronRuns(
      workflow,
      new Date("2026-05-25T08:00:00.000Z"),
    );
    expect(runs.map((run) => run.scheduledAt.toISOString())).toEqual([
      "2026-05-24T09:00:00.000Z",
    ]);
  });

  test("catch-up-all fires up to maxRuns missed occurrences", () => {
    const workflow = defineWorkflow("billing/catchup-all", {
      cron: [
        {
          name: "hourly",
          schedule: "0 * * * *",
          missedRunPolicy: { mode: "catch-up-all", maxRuns: 2 },
        },
      ],
      run: () => undefined,
    });

    const runs = collectDueCronRuns(
      workflow,
      new Date("2026-05-24T05:10:00.000Z"),
    );
    expect(runs.map((run) => run.scheduledAt.toISOString())).toEqual([
      "2026-05-24T04:00:00.000Z",
      "2026-05-24T05:00:00.000Z",
    ]);
  });

  test("skip and catch-up-latest differ across a gap", () => {
    const base = {
      name: "daily",
      schedule: "0 9 * * *",
    };
    const skipWorkflow = defineWorkflow("billing/skip-vs", {
      cron: [{ ...base, missedRunPolicy: "skip" }],
      run: () => undefined,
    });
    const latestWorkflow = defineWorkflow("billing/latest-vs", {
      cron: [{ ...base, missedRunPolicy: "catch-up-latest" }],
      run: () => undefined,
    });

    const now = new Date("2026-05-24T14:00:00.000Z");
    expect(collectDueCronRuns(skipWorkflow, now)).toHaveLength(0);
    expect(collectDueCronRuns(latestWorkflow, now)).toHaveLength(1);
  });
});
