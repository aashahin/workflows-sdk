import { describe, expect, test } from "bun:test";
import type { WorkflowEventEnvelope } from "../core/types";
import { BunRedisWorkflowAdapter } from "./redis-adapter";

class FakeRedis {
  readonly strings = new Map<string, string>();
  readonly hashes = new Map<string, Record<string, string>>();
  readonly sortedSets = new Map<string, Map<string, number>>();
  readonly expirations = new Map<string, number>();

  async send(command: string, args: unknown[]): Promise<unknown> {
    switch (command.toUpperCase()) {
      case "SET": {
        const key = String(args[0]);
        const value = String(args[1]);
        const options = args.slice(2).map(String);
        if (options.includes("NX") && this.strings.has(key)) return null;
        this.strings.set(key, value);
        return "OK";
      }
      case "GET":
        return this.strings.get(String(args[0])) ?? null;
      case "DEL": {
        const key = String(args[0]);
        const removed = this.strings.delete(key);
        this.hashes.delete(key);
        return removed ? 1 : 0;
      }
      case "EXPIRE": {
        const key = String(args[0]);
        this.expirations.set(key, Number(args[1]));
        return this.strings.has(key) || this.hashes.has(key) || this.sortedSets.has(key)
          ? 1
          : 0;
      }
      case "HSET": {
        const key = String(args[0]);
        const fields = args.slice(1).map(String);
        const hash = this.hashes.get(key) ?? {};
        for (let index = 0; index < fields.length; index += 2) {
          hash[fields[index]!] = fields[index + 1]!;
        }
        this.hashes.set(key, hash);
        return fields.length / 2;
      }
      case "HGETALL": {
        const hash = this.hashes.get(String(args[0]));
        return hash ? Object.entries(hash).flat() : [];
      }
      case "ZADD": {
        const [key, score, member] = args as [string, number, string];
        const set = this.sortedSets.get(key) ?? new Map<string, number>();
        set.set(member, Number(score));
        this.sortedSets.set(key, set);
        return 1;
      }
      case "ZRANGEBYSCORE": {
        const [key, min, max] = args as [string, string | number, string | number];
        const limitIndex = args.findIndex((arg) => String(arg).toUpperCase() === "LIMIT");
        const offset = limitIndex >= 0 ? Number(args[limitIndex + 1]) : 0;
        const count = limitIndex >= 0 ? Number(args[limitIndex + 2]) : Infinity;
        const minScore = min === "-inf" ? -Infinity : Number(min);
        const maxScore = max === "+inf" ? Infinity : Number(max);
        return [...(this.sortedSets.get(key)?.entries() ?? [])]
          .filter(([, score]) => score >= minScore && score <= maxScore)
          .sort((a, b) => a[1] - b[1])
          .slice(offset, offset + count)
          .map(([member]) => member);
      }
      case "ZREM": {
        const [key, member] = args as [string, string];
        return this.sortedSets.get(key)?.delete(member) ? 1 : 0;
      }
      case "EVAL": {
        const script = String(args[0]);
        if (script.includes("ZRANGEBYSCORE")) {
          const [
            ,
            ,
            queueKey,
            leasePrefix,
            processingKey,
            instancePrefix,
            maxScore,
            ,
            now,
            token,
            updatedAt,
          ] = args as [
            string,
            number,
            string,
            string,
            string,
            string,
            string | number,
            number,
            number,
            string,
            string,
          ];
          const max = maxScore === "+inf" ? Infinity : Number(maxScore);
          const due = [...(this.sortedSets.get(queueKey)?.entries() ?? [])]
            .filter(([, score]) => score <= max)
            .sort((a, b) => a[1] - b[1])
            .map(([member]) => member)[0];
          if (!due) return null;

          const leaseKey = `${leasePrefix}:${due}`;
          if (this.strings.has(leaseKey)) return null;
          this.strings.set(leaseKey, String(token));
          this.sortedSets.get(queueKey)?.delete(due);
          const processing = this.sortedSets.get(processingKey) ?? new Map<string, number>();
          // Score is the claim time (mirrors the real Lua script).
          processing.set(due, Number(now));
          this.sortedSets.set(processingKey, processing);
          const instanceHash = this.hashes.get(`${instancePrefix}:${due}`);
          if (instanceHash) {
            instanceHash.status = "running";
            instanceHash.updatedAt = String(updatedAt);
          }
          return due;
        }

        if (Number(args[1]) === 3) {
          const [
            ,
            ,
            instanceKey,
            queueKey,
            idempotencyKey,
            id,
            instanceJson,
            envelopeJson,
            score,
          ] = args as [
            string,
            number,
            string,
            string,
            string,
            string,
            string,
            string,
            number,
            number,
          ];
          if (this.strings.has(idempotencyKey)) {
            return ["IDEMPOTENT", this.strings.get(idempotencyKey) ?? ""];
          }
          this.strings.set(idempotencyKey, id);
          this.hashes.set(instanceKey, { instance: instanceJson, envelope: envelopeJson });
          const queue = this.sortedSets.get(queueKey) ?? new Map<string, number>();
          queue.set(id, Number(score));
          this.sortedSets.set(queueKey, queue);
          return ["CLAIMED", id];
        }

        const [
          ,
          ,
          cronKey,
          instanceKey,
          queueKey,
          idempotencyKey,
          cronJson,
          id,
          instanceJson,
          envelopeJson,
          score,
        ] = args as [
          string,
          number,
          string,
          string,
          string,
          string,
          string,
          string,
          string,
          string,
          number,
          number,
        ];
        if (this.strings.has(cronKey)) return ["DUPLICATE", id];
        this.strings.set(cronKey, cronJson);
        if (this.strings.has(idempotencyKey)) {
          const existingId = this.strings.get(idempotencyKey) ?? "";
          this.strings.delete(cronKey);
          return ["IDEMPOTENT", existingId];
        }
        this.strings.set(idempotencyKey, id);
        this.hashes.set(instanceKey, { instance: instanceJson, envelope: envelopeJson });
        const queue = this.sortedSets.get(queueKey) ?? new Map<string, number>();
        queue.set(id, Number(score));
        this.sortedSets.set(queueKey, queue);
        return ["CLAIMED", id];
      }
      default:
        throw new Error(`Unsupported fake Redis command ${command}`);
    }
  }
}

function envelope(id: string, scheduledAt?: string): WorkflowEventEnvelope {
  return {
    id,
    name: "test/workflow",
    payload: {},
    traceId: "trace",
    idempotencyKey: `idem:${id}`,
    scheduledAt,
    createdAt: "2026-05-24T09:00:00.000Z",
  };
}

describe("BunRedisWorkflowAdapter", () => {
  test("deduplicates cron claims with SET key value NX", async () => {
    const redis = new FakeRedis();
    const adapter = new BunRedisWorkflowAdapter({ client: redis });

    await expect(adapter.claimCronRun("run-key", envelope("one"))).resolves.toBe(true);
    await expect(adapter.claimCronRun("run-key", envelope("two"))).resolves.toBe(false);
  });

  test("recovers expired running leases back to the ready set", async () => {
    const redis = new FakeRedis();
    const adapter = new BunRedisWorkflowAdapter({
      client: redis,
      leaseTtlMs: 1,
    });

    await adapter.dispatch(envelope("job-1"));
    const claimed = await adapter.claimNext(new Date("2026-05-24T09:00:00.000Z"));
    expect(claimed?.id).toBe("job-1");
    expect((await adapter.getInstance("job-1"))?.status).toBe("running");

    const recovered = await adapter.recoverStalled(new Date(Date.now() + 2));
    expect(recovered).toBe(1);
    expect((await adapter.getInstance("job-1"))?.status).toBe("queued");
    expect(await adapter.claimNext(new Date("2026-05-24T09:00:01.000Z"))).toMatchObject({
      id: "job-1",
    });
  });

  test("deduplicates normal dispatch through the atomic enqueue script", async () => {
    const redis = new FakeRedis();
    const adapter = new BunRedisWorkflowAdapter({ client: redis });

    const first = await adapter.dispatch(envelope("job-1"));
    const second = await adapter.dispatch({
      ...envelope("job-2"),
      idempotencyKey: "idem:job-1",
    });

    expect(second.id).toBe(first.id);
    expect(await adapter.claimNext(new Date("2026-05-24T09:00:00.000Z"))).toMatchObject({
      id: "job-1",
    });
    expect(await adapter.claimNext(new Date("2026-05-24T09:00:00.000Z"))).toBeNull();
  });

  test("atomically dispatches cron runs once", async () => {
    const redis = new FakeRedis();
    const adapter = new BunRedisWorkflowAdapter({ client: redis });

    const first = await adapter.dispatchCronRun("run-key", envelope("cron-1"));
    const second = await adapter.dispatchCronRun("run-key", envelope("cron-2"));

    expect(first?.id).toBe("cron-1");
    expect(second).toBeNull();
    expect(await adapter.claimNext(new Date("2026-05-24T09:00:00.000Z"))).toMatchObject({
      id: "cron-1",
    });
  });

  test("persists step results for recovered Redis workflows", async () => {
    const redis = new FakeRedis();
    const adapter = new BunRedisWorkflowAdapter({ client: redis });

    await adapter.saveStepResult("job-1", "side-effect", undefined);

    await expect(adapter.hasStepResult("job-1", "side-effect")).resolves.toBe(true);
    await expect(adapter.getStepResult("job-1", "side-effect")).resolves.toBeUndefined();
  });

  test("recovers a stalled job whose status never advanced to running", async () => {
    const redis = new FakeRedis();
    const adapter = new BunRedisWorkflowAdapter({ client: redis });

    await adapter.dispatch(envelope("job-x"));

    // Simulate a crash after the atomic claim moved the id into processing but
    // before the running-status write: the id sits in processing, the instance
    // JSON still says "queued", and there is no dedicated status field.
    redis.sortedSets.get("workflows:ready")?.delete("job-x");
    const processing =
      redis.sortedSets.get("workflows:processing") ?? new Map<string, number>();
    processing.set("job-x", Date.now() - 10_000);
    redis.sortedSets.set("workflows:processing", processing);
    redis.strings.set("workflows:lease:job-x", "token");

    const recovered = await adapter.recoverStalled(new Date(Date.now() - 5_000));
    expect(recovered).toBe(1);
    expect((await adapter.getInstance("job-x"))?.status).toBe("queued");
    expect(
      await adapter.claimNext(new Date("2026-05-24T09:00:00.000Z")),
    ).toMatchObject({ id: "job-x" });
  });

  test("does not re-enqueue a terminal job found in processing", async () => {
    const redis = new FakeRedis();
    const adapter = new BunRedisWorkflowAdapter({ client: redis });

    await adapter.dispatch(envelope("job-done"));
    await adapter.updateInstance("job-done", "complete", { output: 1 });

    const processing =
      redis.sortedSets.get("workflows:processing") ?? new Map<string, number>();
    processing.set("job-done", Date.now() - 10_000);
    redis.sortedSets.set("workflows:processing", processing);

    const recovered = await adapter.recoverStalled(new Date(Date.now() - 5_000));
    expect(recovered).toBe(0);
    expect((await adapter.getInstance("job-done"))?.status).toBe("complete");
    expect(redis.sortedSets.get("workflows:processing")?.has("job-done")).toBe(false);
  });

  test("expires terminal instance, step, and dead-letter hashes", async () => {
    const redis = new FakeRedis();
    const adapter = new BunRedisWorkflowAdapter({
      client: redis,
      retention: { completedTtlSeconds: 100, deadLetterTtlSeconds: 200 },
    });

    await adapter.dispatch(envelope("job-r"));
    await adapter.saveStepResult("job-r", "step", 1);
    await adapter.updateInstance("job-r", "complete", { output: 1 });
    await adapter.recordDeadLetter(envelope("job-d"), new Error("boom"));

    expect(redis.expirations.get("workflows:instance:job-r")).toBe(100);
    expect(redis.expirations.get("workflows:step:job-r:step")).toBe(200);
    expect(redis.expirations.get("workflows:dead:job-d")).toBe(200);
  });

  test("throws at construction for an unusable Redis client", () => {
    expect(() => new BunRedisWorkflowAdapter({ client: {} })).toThrow(
      /Unusable Redis client/,
    );
  });
});
