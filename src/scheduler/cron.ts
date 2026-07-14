import type { RegisteredWorkflow, WorkflowPayload } from "../core/types";
import type { CronDefinition, CronRun, MissedRunPolicy } from "./types";

const DEFAULT_MAX_DELAY_MS = 3_600_000;

const MONTH_NAMES: Record<string, number> = {
  JAN: 1,
  FEB: 2,
  MAR: 3,
  APR: 4,
  MAY: 5,
  JUN: 6,
  JUL: 7,
  AUG: 8,
  SEP: 9,
  OCT: 10,
  NOV: 11,
  DEC: 12,
};

const DOW_NAMES: Record<string, number> = {
  SUN: 0,
  MON: 1,
  TUE: 2,
  WED: 3,
  THU: 4,
  FRI: 5,
  SAT: 6,
};

interface CronField {
  values: Set<number>;
  isStar: boolean;
}

interface ParsedCron {
  hasSeconds: boolean;
  second: Set<number>;
  minute: Set<number>;
  hour: Set<number>;
  dom: Set<number>;
  month: Set<number>;
  dow: Set<number>;
  domRestricted: boolean;
  dowRestricted: boolean;
}

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
  const parsed = parseCron(schedule);
  const cronFrom = toCronParserDate(from, timezone);
  const next = computeNextCronDate(parsed, cronFrom);

  return fromCronParserDate(next, timezone);
}

export function collectDueCronRuns(
  workflow: RegisteredWorkflow,
  now = new Date(),
  defaultTimezone?: string,
): CronRun[] {
  const runs: CronRun[] = [];

  for (const cron of normalizeCronDefinitions(workflow)) {
    const policy: MissedRunPolicy = cron.missedRunPolicy ?? "skip";
    const timezone = cron.timezone ?? defaultTimezone;
    const maxRuns =
      typeof policy === "object" && policy.mode === "catch-up-all"
        ? Math.max(1, policy.maxRuns)
        : 1;

    const occurrences = getPreviousCronDates(
      cron.schedule,
      now,
      maxRuns,
      timezone,
    );

    for (const occurrence of selectDueOccurrences(
      policy,
      occurrences,
      now,
      cron.maxDelayMs,
    )) {
      runs.push(createCronRun(workflow, cron, occurrence));
    }
  }

  return runs;
}

// Applies the missed-run policy to the ascending list of past occurrences
// (all <= now, oldest first, newest last) collected for a single cron.
function selectDueOccurrences(
  policy: MissedRunPolicy,
  occurrences: Date[],
  now: Date,
  maxDelayMs?: number,
): Date[] {
  if (occurrences.length === 0) return [];

  // catch-up-all: dispatch every collected occurrence in chronological order.
  if (typeof policy === "object") return occurrences;

  const latest = occurrences[occurrences.length - 1]!;

  // catch-up-latest: always dispatch the most recent missed occurrence.
  if (policy === "catch-up-latest") return [latest];

  // skip (default): only dispatch when the most recent occurrence is still
  // fresh; a run missed longer than maxDelayMs ago is dropped.
  const maxDelay = maxDelayMs ?? DEFAULT_MAX_DELAY_MS;
  return now.getTime() - latest.getTime() <= maxDelay ? [latest] : [];
}

// Returns up to `maxRuns` occurrences at or before `now`, oldest first.
function getPreviousCronDates(
  schedule: string,
  now: Date,
  maxRuns: number,
  timezone?: string,
): Date[] {
  const parsed = parseCron(schedule);
  const results: Date[] = [];

  let cursor = toCronParserDate(now, timezone);
  for (let index = 0; index < maxRuns; index++) {
    const previous = computePreviousCronDate(parsed, cursor);
    results.unshift(fromCronParserDate(previous, timezone));
    cursor = new Date(previous.getTime() - 1);
  }

  return results;
}

// ---------------------------------------------------------------------------
// Cron parser + next/previous occurrence engine.
//
// All arithmetic runs on the "cron-parser frame" date produced by
// toCronParserDate(), whose UTC getter/setter fields represent the wall-clock
// time in the target timezone. Every field access below therefore uses the
// getUTC*/setUTC* family so the computation stays internally consistent no
// matter what the host timezone is.
// ---------------------------------------------------------------------------

function parseCron(schedule: string): ParsedCron {
  const parts = schedule.trim().split(/\s+/);
  if (parts.length !== 5 && parts.length !== 6) {
    throw new Error(`Unsupported cron expression "${schedule}"`);
  }

  const hasSeconds = parts.length === 6;
  const offset = hasSeconds ? 1 : 0;

  const second = hasSeconds
    ? parseCronField(parts[0]!, 0, 59)
    : { values: new Set<number>([0]), isStar: false };
  const minute = parseCronField(parts[offset]!, 0, 59);
  const hour = parseCronField(parts[offset + 1]!, 0, 23);
  const dom = parseCronField(parts[offset + 2]!, 1, 31);
  const month = parseCronField(parts[offset + 3]!, 1, 12, MONTH_NAMES);
  const dow = parseCronField(
    parts[offset + 4]!,
    0,
    7,
    DOW_NAMES,
    (value) => value % 7,
  );

  return {
    hasSeconds,
    second: second.values,
    minute: minute.values,
    hour: hour.values,
    dom: dom.values,
    month: month.values,
    dow: dow.values,
    domRestricted: !dom.isStar,
    dowRestricted: !dow.isStar,
  };
}

function parseCronField(
  raw: string,
  min: number,
  max: number,
  names?: Record<string, number>,
  wrap?: (value: number) => number,
): CronField {
  const trimmed = raw.trim();
  const isStar = trimmed === "*";
  const values = new Set<number>();

  for (const token of trimmed.split(",")) {
    if (token === "") {
      throw new Error(`Invalid cron field "${raw}"`);
    }

    let range = token;
    let step = 1;
    const slashIndex = token.indexOf("/");
    if (slashIndex !== -1) {
      range = token.slice(0, slashIndex);
      step = Number(token.slice(slashIndex + 1));
      if (!Number.isInteger(step) || step <= 0) {
        throw new Error(`Invalid cron step "${token}" in "${raw}"`);
      }
    }

    let lo: number;
    let hi: number;
    if (range === "*") {
      lo = min;
      hi = max;
    } else {
      const dashIndex = range.indexOf("-");
      if (dashIndex > 0) {
        lo = parseCronValue(range.slice(0, dashIndex), min, max, names, raw);
        hi = parseCronValue(range.slice(dashIndex + 1), min, max, names, raw);
      } else {
        lo = parseCronValue(range, min, max, names, raw);
        // A bare value with a step (e.g. "5/10") runs from the value to max.
        hi = slashIndex !== -1 ? max : lo;
      }
    }

    if (lo > hi) {
      throw new Error(`Invalid cron range "${token}" in "${raw}"`);
    }

    for (let value = lo; value <= hi; value += step) {
      values.add(wrap ? wrap(value) : value);
    }
  }

  return { values, isStar };
}

function parseCronValue(
  raw: string,
  min: number,
  max: number,
  names: Record<string, number> | undefined,
  field: string,
): number {
  const token = raw.trim();
  let value: number;

  const named = names?.[token.toUpperCase()];
  if (named !== undefined) {
    value = named;
  } else {
    if (!/^\d+$/.test(token)) {
      throw new Error(`Invalid cron value "${raw}" in "${field}"`);
    }
    value = Number(token);
  }

  if (value < min || value > max) {
    throw new Error(
      `Cron value "${raw}" out of range ${min}-${max} in "${field}"`,
    );
  }

  return value;
}

// Vixie-cron day matching: when both DOM and DOW are restricted a date matches
// if EITHER matches; otherwise only the restricted field(s) must match.
function matchesDayRule(parsed: ParsedCron, date: Date): boolean {
  const domMatch = parsed.dom.has(date.getUTCDate());
  const dowMatch = parsed.dow.has(date.getUTCDay());

  if (parsed.domRestricted && parsed.dowRestricted) return domMatch || dowMatch;
  if (parsed.dowRestricted) return dowMatch;
  return domMatch;
}

const MAX_CRON_ITERATIONS = 100_000;

function computeNextCronDate(parsed: ParsedCron, from: Date): Date {
  const date = new Date(from.getTime());
  date.setUTCMilliseconds(0);
  if (parsed.hasSeconds) {
    date.setUTCSeconds(date.getUTCSeconds() + 1);
  } else {
    date.setUTCSeconds(0);
    date.setUTCMinutes(date.getUTCMinutes() + 1);
  }

  for (let iteration = 0; iteration < MAX_CRON_ITERATIONS; iteration++) {
    if (!parsed.month.has(date.getUTCMonth() + 1)) {
      date.setUTCMonth(date.getUTCMonth() + 1, 1);
      date.setUTCHours(0, 0, 0, 0);
      continue;
    }
    if (!matchesDayRule(parsed, date)) {
      date.setUTCDate(date.getUTCDate() + 1);
      date.setUTCHours(0, 0, 0, 0);
      continue;
    }
    if (!parsed.hour.has(date.getUTCHours())) {
      date.setUTCHours(date.getUTCHours() + 1, 0, 0, 0);
      continue;
    }
    if (!parsed.minute.has(date.getUTCMinutes())) {
      date.setUTCMinutes(date.getUTCMinutes() + 1, 0, 0);
      continue;
    }
    if (parsed.hasSeconds && !parsed.second.has(date.getUTCSeconds())) {
      date.setUTCSeconds(date.getUTCSeconds() + 1, 0);
      continue;
    }
    return date;
  }

  throw new Error("Unable to compute next cron occurrence");
}

function computePreviousCronDate(parsed: ParsedCron, from: Date): Date {
  const maxSecond = parsed.hasSeconds ? 59 : 0;
  const date = new Date(from.getTime());
  date.setUTCMilliseconds(0);
  if (!parsed.hasSeconds) date.setUTCSeconds(0);

  for (let iteration = 0; iteration < MAX_CRON_ITERATIONS; iteration++) {
    if (!parsed.month.has(date.getUTCMonth() + 1)) {
      // Jump to the last day of the previous month.
      date.setUTCDate(0);
      date.setUTCHours(23, 59, maxSecond, 0);
      continue;
    }
    if (!matchesDayRule(parsed, date)) {
      date.setUTCDate(date.getUTCDate() - 1);
      date.setUTCHours(23, 59, maxSecond, 0);
      continue;
    }
    if (!parsed.hour.has(date.getUTCHours())) {
      date.setUTCHours(date.getUTCHours() - 1, 59, maxSecond, 0);
      continue;
    }
    if (!parsed.minute.has(date.getUTCMinutes())) {
      date.setUTCMinutes(date.getUTCMinutes() - 1, maxSecond, 0);
      continue;
    }
    if (parsed.hasSeconds && !parsed.second.has(date.getUTCSeconds())) {
      date.setUTCSeconds(date.getUTCSeconds() - 1, 0);
      continue;
    }
    return date;
  }

  throw new Error("Unable to compute previous cron occurrence");
}

// ---------------------------------------------------------------------------
// Timezone projection helpers.
//
// toCronParserDate() projects a real UTC instant into a "fake UTC" Date whose
// UTC fields spell out the wall-clock time in the target timezone;
// fromCronParserDate() reverses the projection.
// ---------------------------------------------------------------------------

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
