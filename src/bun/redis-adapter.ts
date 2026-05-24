import type {
  WorkflowAdapter,
  WorkflowEventEnvelope,
  WorkflowInstance,
  WorkflowStatus,
} from "../core/types";

export interface BunRedisAdapterConfig {
  url?: string;
  client?: unknown;
  namespace?: string;
  leaseTtlMs?: number;
}

type RedisLike = {
  get?(key: string): Promise<string | null> | string | null;
  set?(key: string, value: string, ...args: unknown[]): Promise<unknown> | unknown;
  del?(key: string): Promise<unknown> | unknown;
  hset?(key: string, values: Record<string, string>): Promise<unknown> | unknown;
  hgetall?(key: string): Promise<Record<string, string> | null> | Record<string, string> | null;
  zadd?(key: string, score: number, member: string): Promise<unknown> | unknown;
  zrem?(key: string, member: string): Promise<unknown> | unknown;
  send?(command: string, args: unknown[]): Promise<unknown>;
};

export class BunRedisWorkflowAdapter implements WorkflowAdapter {
  private readonly redis: RedisLike;
  private readonly namespace: string;
  private readonly leaseTtlMs: number;

  constructor(config: BunRedisAdapterConfig = {}) {
    const RedisClient = (globalThis as {
      Bun?: { RedisClient?: new (url?: string) => RedisLike };
    }).Bun?.RedisClient;
    this.redis =
      (config.client as RedisLike | undefined) ??
      (RedisClient ? new RedisClient(config.url) : missingRedisClient());
    this.namespace = config.namespace ?? "workflows";
    this.leaseTtlMs = config.leaseTtlMs ?? 30_000;
  }

  async dispatch(envelope: WorkflowEventEnvelope): Promise<WorkflowInstance> {
    const status: WorkflowStatus = envelope.scheduledAt ? "scheduled" : "queued";
    const instance: WorkflowInstance = {
      id: envelope.id,
      name: envelope.name,
      status,
      traceId: envelope.traceId,
      idempotencyKey: envelope.idempotencyKey,
      scheduledAt: envelope.scheduledAt,
      createdAt: envelope.createdAt,
      updatedAt: new Date().toISOString(),
    };
    const queueKey = envelope.scheduledAt ? this.key("scheduled") : this.key("ready");
    const score = envelope.scheduledAt
      ? new Date(envelope.scheduledAt).getTime()
      : safeTimestamp(envelope.createdAt, Date.now());
    const result = await this.send("EVAL", [
      DISPATCH_ENVELOPE_SCRIPT,
      3,
      this.instanceKey(envelope.id),
      queueKey,
      this.key("idempotency", envelope.idempotencyKey),
      envelope.id,
      JSON.stringify(instance),
      JSON.stringify(envelope),
      score,
      86_400,
    ]);
    const [state, existingId] = parseRedisArray(result);

    if (state === "IDEMPOTENT" && existingId) {
      const existing = await this.getInstance(existingId);
      if (existing) return existing;
    }
    return instance;
  }

  async dispatchBatch(
    envelopes: WorkflowEventEnvelope[],
  ): Promise<WorkflowInstance[]> {
    const instances: WorkflowInstance[] = [];
    for (const envelope of envelopes) {
      instances.push(await this.dispatch(envelope));
    }
    return instances;
  }

  async dispatchCronRun(
    runKey: string,
    envelope: WorkflowEventEnvelope,
  ): Promise<WorkflowInstance | null> {
    const status: WorkflowStatus = envelope.scheduledAt ? "scheduled" : "queued";
    const instance: WorkflowInstance = {
      id: envelope.id,
      name: envelope.name,
      status,
      traceId: envelope.traceId,
      idempotencyKey: envelope.idempotencyKey,
      scheduledAt: envelope.scheduledAt,
      createdAt: envelope.createdAt,
      updatedAt: new Date().toISOString(),
    };
    const queueKey = envelope.scheduledAt ? this.key("scheduled") : this.key("ready");
    const score = envelope.scheduledAt
      ? new Date(envelope.scheduledAt).getTime()
      : safeTimestamp(envelope.createdAt, Date.now());

    const result = await this.send("EVAL", [
      DISPATCH_CRON_RUN_SCRIPT,
      4,
      this.key("cron", runKey),
      this.instanceKey(envelope.id),
      queueKey,
      this.key("idempotency", envelope.idempotencyKey),
      JSON.stringify({
        id: envelope.id,
        name: envelope.name,
        scheduledAt: envelope.scheduledAt,
        claimedAt: new Date().toISOString(),
      }),
      envelope.id,
      JSON.stringify(instance),
      JSON.stringify(envelope),
      score,
      86_400,
    ]);
    const [state, existingId] = parseRedisArray(result);

    if (state === "CLAIMED") return instance;
    if (state === "IDEMPOTENT" && existingId) {
      return await this.getInstance(existingId);
    }
    return null;
  }

  async requeue(
    envelope: WorkflowEventEnvelope,
    options: {
      scheduledAt?: Date | string;
      error?: { name: string; message: string };
    } = {},
  ): Promise<void> {
    const scheduledAt = normalizeDateString(options.scheduledAt);
    const status: WorkflowStatus = scheduledAt ? "scheduled" : "queued";
    const row = await this.hgetall(this.instanceKey(envelope.id));
    const current = row?.instance
      ? (JSON.parse(row.instance) as WorkflowInstance)
      : ({ id: envelope.id, name: envelope.name, status } satisfies WorkflowInstance);
    const updatedEnvelope: WorkflowEventEnvelope = {
      ...envelope,
      scheduledAt,
    };
    const instance: WorkflowInstance = {
      ...current,
      status,
      scheduledAt,
      error: options.error ?? current.error,
      updatedAt: new Date().toISOString(),
    };
    const queueKey = scheduledAt ? this.key("scheduled") : this.key("ready");
    const score = scheduledAt ? new Date(scheduledAt).getTime() : Date.now();

    await this.hset(this.instanceKey(envelope.id), {
      instance: JSON.stringify(instance),
      envelope: JSON.stringify(updatedEnvelope),
    });
    await this.releaseLease(envelope.id);
    await this.zadd(queueKey, score, envelope.id);
  }

  async getInstance(id: string): Promise<WorkflowInstance | null> {
    const row = await this.hgetall(this.instanceKey(id));
    if (!row?.instance) return null;
    return JSON.parse(row.instance) as WorkflowInstance;
  }

  async claimCronRun(
    runKey: string,
    _envelope: WorkflowEventEnvelope,
  ): Promise<boolean> {
    const result = await this.set(
      this.key("cron", runKey),
      JSON.stringify({
        id: _envelope.id,
        name: _envelope.name,
        scheduledAt: _envelope.scheduledAt,
        claimedAt: new Date().toISOString(),
      }),
      "NX",
    );
    return result === "OK" || result === true;
  }

  async releaseCronRun(runKey: string): Promise<void> {
    await this.del(this.key("cron", runKey));
  }

  async claimNext(now = new Date()): Promise<WorkflowEventEnvelope | null> {
    const scheduledId = await this.claimFromSortedSet(
      this.key("scheduled"),
      now.getTime(),
    );
    const ready = scheduledId
      ? null
      : await this.claimFromSortedSet(this.key("ready"), Infinity);
    const id = scheduledId ?? ready;
    if (!id) return null;

    const row = await this.hgetall(this.instanceKey(id));
    if (!row?.envelope) {
      await this.releaseLease(id);
      return null;
    }

    const instance = row.instance
      ? (JSON.parse(row.instance) as WorkflowInstance)
      : null;
    await this.hset(this.instanceKey(id), {
      instance: JSON.stringify({
        ...instance,
        id,
        name: instance?.name ?? "unknown",
        status: "running",
        updatedAt: new Date().toISOString(),
      } satisfies WorkflowInstance),
      envelope: row.envelope,
    });

    return JSON.parse(row.envelope) as WorkflowEventEnvelope;
  }

  async recoverStalled(before: Date): Promise<number> {
    const expired = (await this.zrangeByScore(
      this.key("processing"),
      "-inf",
      before.getTime(),
      100,
    )) as string[] | null;
    if (!Array.isArray(expired) || expired.length === 0) return 0;

    let recovered = 0;
    for (const id of expired) {
      await this.zrem(this.key("processing"), id);
      await this.del(this.key("lease", id));

      const row = await this.hgetall(this.instanceKey(id));
      if (!row?.envelope || !row.instance) continue;

      const envelope = JSON.parse(row.envelope) as WorkflowEventEnvelope;
      const current = JSON.parse(row.instance) as WorkflowInstance;
      if (current.status !== "running") continue;

      const status: WorkflowStatus =
        envelope.scheduledAt && new Date(envelope.scheduledAt).getTime() > Date.now()
          ? "scheduled"
          : "queued";

      await this.hset(this.instanceKey(id), {
        instance: JSON.stringify({
          ...current,
          status,
          updatedAt: new Date().toISOString(),
        } satisfies WorkflowInstance),
        envelope: row.envelope,
      });

      if (status === "scheduled" && envelope.scheduledAt) {
        await this.zadd(
          this.key("scheduled"),
          new Date(envelope.scheduledAt).getTime(),
          id,
        );
      } else {
        await this.zadd(this.key("ready"), Date.now(), id);
      }
      recovered++;
    }

    return recovered;
  }

  async updateInstance(
    id: string,
    status: WorkflowStatus,
    patch: { output?: unknown; error?: { name: string; message: string } } = {},
  ): Promise<void> {
    const row = await this.hgetall(this.instanceKey(id));
    const current = row?.instance
      ? (JSON.parse(row.instance) as WorkflowInstance)
      : ({ id, name: "unknown", status } satisfies WorkflowInstance);

    await this.hset(this.instanceKey(id), {
      ...(row?.envelope ? { envelope: row.envelope } : {}),
      instance: JSON.stringify({
        ...current,
        status,
        output: patch.output ?? current.output,
        error: patch.error ?? current.error,
        updatedAt: new Date().toISOString(),
      } satisfies WorkflowInstance),
    });
    await this.releaseLease(id);
  }

  async recordDeadLetter(
    envelope: WorkflowEventEnvelope,
    error: Error,
  ): Promise<void> {
    await this.hset(this.key("dead", envelope.id), {
      envelope: JSON.stringify(envelope),
      error: JSON.stringify({ name: error.name, message: error.message }),
      createdAt: new Date().toISOString(),
    });
  }

  async getStepResult(
    instanceId: string,
    stepName: string,
  ): Promise<unknown | undefined> {
    const row = await this.hgetall(this.stepKey(instanceId, stepName));
    if (!row?.result) return undefined;
    const parsed = JSON.parse(row.result) as { hasValue?: boolean; value?: unknown };
    return parsed.hasValue === true ? parsed.value : parsed;
  }

  async hasStepResult(instanceId: string, stepName: string): Promise<boolean> {
    const row = await this.hgetall(this.stepKey(instanceId, stepName));
    return Boolean(row?.result);
  }

  async saveStepResult(
    instanceId: string,
    stepName: string,
    result: unknown,
  ): Promise<void> {
    await this.hset(this.stepKey(instanceId, stepName), {
      result: JSON.stringify({ hasValue: true, value: result }),
      createdAt: new Date().toISOString(),
    });
  }

  private key(...parts: string[]): string {
    return [this.namespace, ...parts].join(":");
  }

  private instanceKey(id: string): string {
    return this.key("instance", id);
  }

  private stepKey(instanceId: string, stepName: string): string {
    return this.key("step", instanceId, stepName);
  }

  private async get(key: string): Promise<string | null> {
    if (this.redis.get) return await this.redis.get(key);
    return (await this.send("GET", [key])) as string | null;
  }

  private async set(key: string, value: string, ...args: unknown[]): Promise<unknown> {
    return this.send("SET", [key, value, ...args]);
  }

  private async hset(
    key: string,
    values: Record<string, string>,
  ): Promise<unknown> {
    if (this.redis.hset) return await this.redis.hset(key, values);
    return this.send("HSET", [
      key,
      ...Object.entries(values).flatMap(([field, value]) => [field, value]),
    ]);
  }

  private async hgetall(key: string): Promise<Record<string, string> | null> {
    if (this.redis.hgetall) return await this.redis.hgetall(key);
    const value = await this.send("HGETALL", [key]);
    if (typeof value === "object" && value !== null && !Array.isArray(value)) {
      return Object.fromEntries(
        Object.entries(value as Record<string, unknown>).map(([field, fieldValue]) => [
          field,
          String(fieldValue),
        ]),
      );
    }
    if (!Array.isArray(value)) return null;
    const record: Record<string, string> = {};
    for (let index = 0; index < value.length; index += 2) {
      record[String(value[index])] = String(value[index + 1]);
    }
    return record;
  }

  private async zadd(
    key: string,
    score: number,
    member: string,
  ): Promise<unknown> {
    if (this.redis.zadd) return await this.redis.zadd(key, score, member);
    return this.send("ZADD", [key, score, member]);
  }

  private async claimFromSortedSet(
    key: string,
    maxScore: number,
  ): Promise<string | null> {
    if (this.redis.send) {
      const result = await this.send("EVAL", [
        CLAIM_FROM_SORTED_SET_SCRIPT,
        3,
        key,
        this.key("lease"),
        this.key("processing"),
        serializeRedisScore(maxScore),
        this.leaseTtlMs,
        Date.now(),
        `${Date.now()}:${Math.random().toString(36).slice(2)}`,
      ]);
      return typeof result === "string" && result.length > 0 ? result : null;
    }

    const due = await this.zrangeByScore(key, "-inf", maxScore, 1);
    const id = Array.isArray(due) ? due[0] : undefined;
    if (!id) return null;

    const lease = await this.set(
      this.key("lease", id),
      `${Date.now()}:${Math.random().toString(36).slice(2)}`,
      "PX",
      this.leaseTtlMs,
      "NX",
    );
    if (lease !== "OK" && lease !== true) return null;

    await this.zrem(key, id);
    await this.zadd(this.key("processing"), Date.now() + this.leaseTtlMs, id);
    return id;
  }

  private async zrangeByScore(
    key: string,
    min: string | number,
    max: string | number,
    count: number,
  ): Promise<unknown> {
    return this.send("ZRANGEBYSCORE", [
      key,
      min,
      max,
      "LIMIT",
      0,
      count,
    ]);
  }

  private async zrem(key: string, member: string): Promise<unknown> {
    if (this.redis.zrem) return await this.redis.zrem(key, member);
    return this.send("ZREM", [key, member]);
  }

  private async releaseLease(id: string): Promise<void> {
    await this.del(this.key("lease", id));
    await this.zrem(this.key("processing"), id);
  }

  private async del(key: string): Promise<unknown> {
    if (this.redis.del) return await this.redis.del(key);
    return this.send("DEL", [key]);
  }

  private async send(command: string, args: unknown[]): Promise<unknown> {
    if (!this.redis.send) {
      throw new Error(`Redis client does not support ${command}`);
    }
    return this.redis.send(command, args.map((arg) => String(arg)));
  }
}

const CLAIM_FROM_SORTED_SET_SCRIPT = `
local id = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "LIMIT", 0, 1)[1]
if not id then
  return nil
end

local leaseKey = KEYS[2] .. ":" .. id
if redis.call("SET", leaseKey, ARGV[4], "PX", ARGV[2], "NX") == false then
  return nil
end

redis.call("ZREM", KEYS[1], id)
redis.call("ZADD", KEYS[3], tonumber(ARGV[3]) + tonumber(ARGV[2]), id)
return id
`;

const DISPATCH_CRON_RUN_SCRIPT = `
if redis.call("SET", KEYS[1], ARGV[1], "NX") == false then
  return {"DUPLICATE", ARGV[2]}
end

if redis.call("SET", KEYS[4], ARGV[2], "NX", "EX", ARGV[6]) == false then
  local existingId = redis.call("GET", KEYS[4])
  redis.call("DEL", KEYS[1])
  return {"IDEMPOTENT", existingId or ""}
end

redis.call("HSET", KEYS[2], "instance", ARGV[3], "envelope", ARGV[4])
redis.call("ZADD", KEYS[3], ARGV[5], ARGV[2])
return {"CLAIMED", ARGV[2]}
`;

const DISPATCH_ENVELOPE_SCRIPT = `
if redis.call("SET", KEYS[3], ARGV[1], "NX", "EX", ARGV[5]) == false then
  local existingId = redis.call("GET", KEYS[3])
  return {"IDEMPOTENT", existingId or ""}
end

redis.call("HSET", KEYS[1], "instance", ARGV[2], "envelope", ARGV[3])
redis.call("ZADD", KEYS[2], ARGV[4], ARGV[1])
return {"CLAIMED", ARGV[1]}
`;

function parseRedisArray(value: unknown): string[] {
  return Array.isArray(value) ? value.map((item) => String(item)) : [];
}

function serializeRedisScore(score: number): string | number {
  if (score === Infinity) return "+inf";
  if (score === -Infinity) return "-inf";
  return score;
}

function missingRedisClient(): never {
  throw new Error(
    "Bun.RedisClient is unavailable. Pass an existing Redis client to BunRedisWorkflowAdapter.",
  );
}

function safeTimestamp(value: string | undefined, fallback: number): number {
  const timestamp = value ? Date.parse(value) : Number.NaN;
  return Number.isFinite(timestamp) ? timestamp : fallback;
}

function normalizeDateString(value: Date | string | undefined): string | undefined {
  if (value instanceof Date) return value.toISOString();
  if (typeof value === "string") return new Date(value).toISOString();
  return undefined;
}
