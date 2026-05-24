import type { RegisteredWorkflow, WorkflowPayload } from "../core/types";
import type { CronDefinition, CronRun } from "./types";

type BunCronParser = {
  next?(from?: Date): Date | null;
};

export function normalizeCronDefinitions(
  workflow: RegisteredWorkflow,
): CronDefinition[] {
  if (!workflow.cron) return [];

  const crons = Array.isArray(workflow.cron)
    ? workflow.cron
    : [{ schedule: workflow.cron }];

  return crons.map((cron, index) => ({
    missedRunPolicy: "skip",
    ...cron,
    name: cron.name ?? `${workflow.name}#${index + 1}`,
  }));
}

export function createCronRunKey(
  workflowName: string,
  cronName: string,
  scheduledAt: Date | string,
): string {
  const iso =
    scheduledAt instanceof Date
      ? scheduledAt.toISOString()
      : new Date(scheduledAt).toISOString();
  return `${workflowName}:${cronName}:${iso}`;
}

export function createCronRun(
  workflow: RegisteredWorkflow,
  cron: CronDefinition,
  scheduledAt: Date,
): CronRun {
  const cronName = cron.name ?? workflow.name;
  const payload =
    typeof cron.payload === "function"
      ? cron.payload()
      : ((cron.payload ?? {}) as WorkflowPayload);

  return {
    workflowName: workflow.name,
    cronName,
    schedule: cron.schedule,
    scheduledAt,
    runKey: createCronRunKey(workflow.name, cronName, scheduledAt),
    payload,
    metadata: {
      ...cron.metadata,
      cronName,
      cronSchedule: cron.schedule,
      cronRunKey: createCronRunKey(workflow.name, cronName, scheduledAt),
    },
  };
}

export function getNextCronDate(schedule: string, from = new Date()): Date {
  return getNextCronDateInTimezone(schedule, from);
}

export function getNextCronDateInTimezone(
  schedule: string,
  from = new Date(),
  timezone?: string,
): Date {
  const cronFrom = toCronParserDate(from, timezone);
  const next = parseNextCronDate(schedule, cronFrom);

  return fromCronParserDate(next, timezone);
}

function parseNextCronDate(schedule: string, from: Date): Date {
  const bun = (globalThis as { Bun?: unknown }).Bun as
    | {
        cron?: {
          parse?: (
            schedule: string,
            relativeDate?: Date | number,
          ) => Date | null | BunCronParser;
        };
      }
    | undefined;

  const parsed = bun?.cron?.parse?.(schedule, from);
  const next =
    parsed instanceof Date
      ? parsed
      : parsed && "next" in parsed
        ? parsed.next?.(from)
        : null;
  if (next instanceof Date && !Number.isNaN(next.getTime())) {
    return next;
  }

  return fallbackNextCronDate(schedule, from);
}

export function collectDueCronRuns(
  workflow: RegisteredWorkflow,
  now = new Date(),
  defaultTimezone?: string,
): CronRun[] {
  const runs: CronRun[] = [];

  for (const cron of normalizeCronDefinitions(workflow)) {
    const maxRuns =
      typeof cron.missedRunPolicy === "object" &&
      cron.missedRunPolicy.mode === "catch-up-all"
        ? Math.max(1, cron.missedRunPolicy.maxRuns)
        : 1;
    for (const previous of getPreviousCronDates(
      cron.schedule,
        now,
        maxRuns,
        cron.timezone ?? defaultTimezone,
      )) {
      if (previous.getTime() <= now.getTime()) {
        runs.push(createCronRun(workflow, cron, previous));
      }
    }
  }

  return runs;
}

function getPreviousCronDates(
  schedule: string,
  now: Date,
  maxRuns: number,
  timezone?: string,
): Date[] {
  const parsedPrevious = getPreviousCronDatesFromBun(
    schedule,
    now,
    maxRuns,
    timezone,
  );
  if (parsedPrevious.length > 0) return parsedPrevious;

  // Fallback for non-Bun runtimes. This intentionally handles only the common
  // simple cron subset used in tests and docs; Bun owns production parsing.
  return [fallbackPreviousCronDate(schedule, toCronParserDate(now, timezone))].map(
    (date) => fromCronParserDate(date, timezone),
  );
}

function fallbackPreviousCronDate(schedule: string, now: Date): Date {
  const parts = schedule.trim().split(/\s+/);
  if (parts.length !== 5 && parts.length !== 6) {
    throw new Error(`Unsupported cron expression "${schedule}"`);
  }

  const hasSeconds = parts.length === 6;
  const second = hasSeconds ? parseField(parts[0]!, 0, now.getSeconds()) : 0;
  const minute = parseField(parts[hasSeconds ? 1 : 0]!, 0, now.getMinutes());
  const hour = parseField(parts[hasSeconds ? 2 : 1]!, 0, now.getHours());

  const previous = new Date(now);
  previous.setMilliseconds(0);
  previous.setSeconds(second);
  previous.setMinutes(minute);
  previous.setHours(hour);

  if (previous.getTime() > now.getTime()) {
    previous.setDate(previous.getDate() - 1);
  }

  return previous;
}

function getPreviousCronDatesFromBun(
  schedule: string,
  now: Date,
  maxRuns: number,
  timezone?: string,
): Date[] {
  const cronNow = toCronParserDate(now, timezone);

  const lookbackWindows = [
    60_000,
    3_600_000,
    86_400_000,
    7 * 86_400_000,
    31 * 86_400_000,
    366 * 86_400_000,
  ];
  let best: Date[] = [];

  for (const lookbackMs of lookbackWindows) {
    let cursor = new Date(cronNow.getTime() - lookbackMs);
    const previous: Date[] = [];

    for (let index = 0; index < 100_000; index++) {
      const next = parseNextCronDate(schedule, cursor);
      if (next.getTime() > cronNow.getTime()) {
        if (previous.length > best.length) best = previous;
        if (previous.length >= maxRuns) {
          return previous
            .slice(-maxRuns)
            .map((date) => fromCronParserDate(date, timezone));
        }
        break;
      }
      previous.push(next);
      cursor = new Date(next.getTime() + 1);
    }
  }

  return best.slice(-maxRuns).map((date) => fromCronParserDate(date, timezone));
}

function fallbackNextCronDate(schedule: string, from: Date): Date {
  const parts = schedule.trim().split(/\s+/);
  if (parts.length !== 5 && parts.length !== 6) {
    throw new Error(`Unsupported cron expression "${schedule}"`);
  }

  const hasSeconds = parts.length === 6;
  const second = hasSeconds ? parseField(parts[0]!, 0, 0) : 0;
  const minute = parseField(parts[hasSeconds ? 1 : 0]!, 0, 0);
  const hour = parseField(parts[hasSeconds ? 2 : 1]!, 0, 0);

  const next = new Date(from);
  next.setMilliseconds(0);
  next.setSeconds(second);
  next.setMinutes(minute);
  next.setHours(hour);

  if (next.getTime() <= from.getTime()) {
    next.setDate(next.getDate() + 1);
  }

  return next;
}

function parseField(field: string, fallback: number, current: number): number {
  if (field === "*") return current;
  if (field.startsWith("*/")) return current - (current % Number(field.slice(2)));
  const parsed = Number(field);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function toCronParserDate(date: Date, timezone?: string): Date {
  if (!timezone || timezone.toUpperCase() === "UTC") return date;
  return getZonedWallClockDate(date, timezone);
}

function fromCronParserDate(date: Date, timezone?: string): Date {
  if (!timezone || timezone.toUpperCase() === "UTC") return date;
  return zonedWallClockDateToUtc(date, timezone);
}

function getZonedWallClockDate(date: Date, timezone: string): Date {
  const parts = getTimeZoneParts(date, timezone);
  return new Date(
    Date.UTC(
      parts.year,
      parts.month - 1,
      parts.day,
      parts.hour,
      parts.minute,
      parts.second,
      date.getUTCMilliseconds(),
    ),
  );
}

function zonedWallClockDateToUtc(date: Date, timezone: string): Date {
  const target = Date.UTC(
    date.getUTCFullYear(),
    date.getUTCMonth(),
    date.getUTCDate(),
    date.getUTCHours(),
    date.getUTCMinutes(),
    date.getUTCSeconds(),
    date.getUTCMilliseconds(),
  );
  let utc = new Date(target);

  for (let attempt = 0; attempt < 3; attempt++) {
    const zoned = getZonedWallClockDate(utc, timezone).getTime();
    const offset = zoned - utc.getTime();
    utc = new Date(target - offset);
  }

  return utc;
}

function getTimeZoneParts(date: Date, timezone: string): {
  year: number;
  month: number;
  day: number;
  hour: number;
  minute: number;
  second: number;
} {
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone: timezone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hourCycle: "h23",
  });
  const values = Object.fromEntries(
    formatter.formatToParts(date).map((part) => [part.type, part.value]),
  );

  return {
    year: Number(values.year),
    month: Number(values.month),
    day: Number(values.day),
    hour: Number(values.hour),
    minute: Number(values.minute),
    second: Number(values.second),
  };
}
