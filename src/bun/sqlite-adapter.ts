import { Database } from "bun:sqlite";
import type {
  WorkflowAdapter,
  WorkflowEventEnvelope,
  WorkflowInstance,
  WorkflowStatus,
} from "../core/types";

export interface BunSqliteAdapterConfig {
  path: string;
  namespace?: string;
}

export class BunSqliteWorkflowAdapter implements WorkflowAdapter {
  readonly db: Database;
  private readonly namespace: string;

  constructor(config: BunSqliteAdapterConfig) {
    this.db = new Database(config.path);
    this.namespace = config.namespace ?? "default";
    this.migrate();
  }

  async dispatch(envelope: WorkflowEventEnvelope): Promise<WorkflowInstance> {
    const status: WorkflowStatus = envelope.scheduledAt ? "scheduled" : "queued";
    const now = new Date().toISOString();
    const existingByIdempotency = this.getByIdempotencyKey(envelope.idempotencyKey);
    if (existingByIdempotency) return existingByIdempotency;

    this.db
      .query(
        `insert into workflow_instances
          (id, namespace, name, status, envelope_json, trace_id, idempotency_key, scheduled_at, created_at, updated_at)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        on conflict(namespace, id) do update set
          envelope_json = excluded.envelope_json,
          updated_at = excluded.updated_at`,
      )
      .run(
        envelope.id,
        this.namespace,
        envelope.name,
        status,
        JSON.stringify(envelope),
        envelope.traceId,
        envelope.idempotencyKey,
        envelope.scheduledAt ?? null,
        envelope.createdAt,
        now,
      );

    return this.getInstance(envelope.id).then((instance) => instance!);
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
    const now = new Date().toISOString();

    const claim = this.db.transaction(() => {
      const cron = this.db
        .query(
          `insert into workflow_cron_runs
            (namespace, run_key, workflow_name, cron_name, scheduled_at, envelope_id, created_at)
          values (?, ?, ?, ?, ?, ?, ?)
          on conflict(namespace, run_key) do nothing`,
        )
        .run(
          this.namespace,
          runKey,
          envelope.name,
          String(envelope.metadata?.cronName ?? envelope.name),
          envelope.scheduledAt ?? envelope.createdAt,
          envelope.id,
          now,
        );
      if (cron.changes === 0) return false;

      this.db
        .query(
          `insert into workflow_instances
            (id, namespace, name, status, envelope_json, trace_id, idempotency_key, scheduled_at, created_at, updated_at)
          values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          on conflict(namespace, id) do update set
            envelope_json = excluded.envelope_json,
            updated_at = excluded.updated_at`,
        )
        .run(
          envelope.id,
          this.namespace,
          envelope.name,
          status,
          JSON.stringify(envelope),
          envelope.traceId,
          envelope.idempotencyKey,
          envelope.scheduledAt ?? null,
          envelope.createdAt,
          now,
        );

      return true;
    });

    return claim() ? await this.getInstance(envelope.id) : null;
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
    const updatedEnvelope: WorkflowEventEnvelope = {
      ...envelope,
      scheduledAt,
    };

    this.db
      .query(
        `update workflow_instances
         set status = ?,
             envelope_json = ?,
             scheduled_at = ?,
             error_json = coalesce(?, error_json),
             updated_at = ?
         where namespace = ? and id = ?`,
      )
      .run(
        status,
        JSON.stringify(updatedEnvelope),
        scheduledAt ?? null,
        options.error === undefined ? null : JSON.stringify(options.error),
        new Date().toISOString(),
        this.namespace,
        envelope.id,
      );
  }

  async getInstance(id: string): Promise<WorkflowInstance | null> {
    const row = this.db
      .query(
        `select id, name, status, trace_id, idempotency_key, scheduled_at, created_at, updated_at, output_json, error_json
         from workflow_instances where namespace = ? and id = ?`,
      )
      .get(this.namespace, id) as Record<string, unknown> | null;

    if (!row) return null;

    return {
      id: String(row.id),
      name: String(row.name),
      status: row.status as WorkflowStatus,
      traceId: row.trace_id ? String(row.trace_id) : undefined,
      idempotencyKey: row.idempotency_key
        ? String(row.idempotency_key)
        : undefined,
      scheduledAt: row.scheduled_at ? String(row.scheduled_at) : undefined,
      createdAt: row.created_at ? String(row.created_at) : undefined,
      updatedAt: row.updated_at ? String(row.updated_at) : undefined,
      output: row.output_json ? JSON.parse(String(row.output_json)) : undefined,
      error: row.error_json ? JSON.parse(String(row.error_json)) : undefined,
    };
  }

  async claimCronRun(
    runKey: string,
    envelope: WorkflowEventEnvelope,
  ): Promise<boolean> {
    const result = this.db
      .query(
        `insert into workflow_cron_runs
          (namespace, run_key, workflow_name, cron_name, scheduled_at, envelope_id, created_at)
        values (?, ?, ?, ?, ?, ?, ?)
        on conflict(namespace, run_key) do nothing`,
      )
      .run(
        this.namespace,
        runKey,
        envelope.name,
        String(envelope.metadata?.cronName ?? envelope.name),
        envelope.scheduledAt ?? envelope.createdAt,
        envelope.id,
        new Date().toISOString(),
      );

    return result.changes > 0;
  }

  async releaseCronRun(runKey: string): Promise<void> {
    this.db
      .query(
        `delete from workflow_cron_runs
         where namespace = ? and run_key = ?`,
      )
      .run(this.namespace, runKey);
  }

  async claimNext(now = new Date()): Promise<WorkflowEventEnvelope | null> {
    const claim = this.db.transaction(() => {
      const row = this.db
        .query(
          `select id, envelope_json
           from workflow_instances
           where namespace = ?
             and (
               status = 'queued'
               or (status = 'scheduled' and scheduled_at <= ?)
             )
           order by coalesce(scheduled_at, created_at) asc
           limit 1`,
        )
        .get(this.namespace, now.toISOString()) as
        | { id: string; envelope_json: string }
        | null;

      if (!row) return null;

      this.db
        .query(
          `update workflow_instances
           set status = 'running', updated_at = ?
           where namespace = ? and id = ? and status in ('queued', 'scheduled')`,
        )
        .run(new Date().toISOString(), this.namespace, row.id);

      return row.envelope_json;
    });
    const claimed = claim();

    return claimed ? (JSON.parse(claimed) as WorkflowEventEnvelope) : null;
  }

  async recoverStalled(before: Date): Promise<number> {
    const result = this.db
      .query(
        `update workflow_instances
         set status = case
             when scheduled_at is not null and scheduled_at > ? then 'scheduled'
             else 'queued'
           end,
           updated_at = ?
         where namespace = ?
           and status = 'running'
           and updated_at < ?`,
      )
      .run(
        new Date().toISOString(),
        new Date().toISOString(),
        this.namespace,
        before.toISOString(),
      );

    return result.changes;
  }

  async updateInstance(
    id: string,
    status: WorkflowStatus,
    patch: { output?: unknown; error?: { name: string; message: string } } = {},
  ): Promise<void> {
    this.db
      .query(
        `update workflow_instances
         set status = ?, output_json = coalesce(?, output_json), error_json = coalesce(?, error_json), updated_at = ?
         where namespace = ? and id = ?`,
      )
      .run(
        status,
        patch.output === undefined ? null : JSON.stringify(patch.output),
        patch.error === undefined ? null : JSON.stringify(patch.error),
        new Date().toISOString(),
        this.namespace,
        id,
      );
  }

  async getStepResult(
    instanceId: string,
    stepName: string,
  ): Promise<unknown | undefined> {
    const row = this.db
      .query(
        `select result_json from workflow_step_results
         where namespace = ? and instance_id = ? and step_name = ? and error_json is null`,
      )
      .get(this.namespace, instanceId, stepName) as
      | { result_json: string | null }
      | null;

    if (!row) return undefined;
    if (row.result_json === null) return undefined;

    const parsed = JSON.parse(row.result_json) as {
      hasValue?: boolean;
      value?: unknown;
    };
    return parsed.hasValue === true ? parsed.value : parsed;
  }

  async hasStepResult(instanceId: string, stepName: string): Promise<boolean> {
    const row = this.db
      .query(
        `select 1 from workflow_step_results
         where namespace = ? and instance_id = ? and step_name = ? and error_json is null`,
      )
      .get(this.namespace, instanceId, stepName);

    return row !== null;
  }

  async saveStepResult(
    instanceId: string,
    stepName: string,
    result: unknown,
  ): Promise<void> {
    this.db
      .query(
        `insert into workflow_step_results(namespace, instance_id, step_name, result_json, created_at)
         values (?, ?, ?, ?, ?)
         on conflict(namespace, instance_id, step_name) do nothing`,
      )
      .run(
        this.namespace,
        instanceId,
        stepName,
        JSON.stringify({ hasValue: true, value: result }),
        new Date().toISOString(),
      );
  }

  async recordDeadLetter(
    envelope: WorkflowEventEnvelope,
    error: Error,
  ): Promise<void> {
    this.db
      .query(
        `insert into workflow_dead_letters(namespace, id, instance_id, name, envelope_json, error_json, created_at)
         values (?, ?, ?, ?, ?, ?, ?)
         on conflict(namespace, id) do nothing`,
      )
      .run(
        this.namespace,
        `${envelope.id}:dead`,
        envelope.id,
        envelope.name,
        JSON.stringify(envelope),
        JSON.stringify({ name: error.name, message: error.message }),
        new Date().toISOString(),
      );
  }

  private getByIdempotencyKey(idempotencyKey: string): WorkflowInstance | null {
    const row = this.db
      .query(
        `select id from workflow_instances
         where namespace = ? and idempotency_key = ?
         limit 1`,
      )
      .get(this.namespace, idempotencyKey) as { id: string } | null;

    return row ? (this.getInstanceSync(row.id) ?? null) : null;
  }

  private getInstanceSync(id: string): WorkflowInstance | null {
    const row = this.db
      .query(
        `select id, name, status, trace_id, idempotency_key, scheduled_at, created_at, updated_at, output_json, error_json
         from workflow_instances where namespace = ? and id = ?`,
      )
      .get(this.namespace, id) as Record<string, unknown> | null;

    if (!row) return null;

    return {
      id: String(row.id),
      name: String(row.name),
      status: row.status as WorkflowStatus,
      traceId: row.trace_id ? String(row.trace_id) : undefined,
      idempotencyKey: row.idempotency_key
        ? String(row.idempotency_key)
        : undefined,
      scheduledAt: row.scheduled_at ? String(row.scheduled_at) : undefined,
      createdAt: row.created_at ? String(row.created_at) : undefined,
      updatedAt: row.updated_at ? String(row.updated_at) : undefined,
      output: row.output_json ? JSON.parse(String(row.output_json)) : undefined,
      error: row.error_json ? JSON.parse(String(row.error_json)) : undefined,
    };
  }

  private migrate(): void {
    this.db.run("pragma journal_mode = WAL");
    this.db.run("pragma foreign_keys = on");
    this.db.run(
      `create table if not exists workflow_schema_versions (
        namespace text not null,
        version integer not null,
        applied_at text not null,
        primary key(namespace, version)
      )`,
    );
    this.db.run(
      `create table if not exists workflow_instances (
        id text not null,
        namespace text not null,
        name text not null,
        status text not null,
        envelope_json text not null,
        trace_id text,
        idempotency_key text,
        scheduled_at text,
        created_at text not null,
        updated_at text not null,
        output_json text,
        error_json text,
        primary key(namespace, id)
      )`,
    );
    this.db.run(
      `create unique index if not exists workflow_instances_idempotency_idx
       on workflow_instances(namespace, idempotency_key)
       where idempotency_key is not null`,
    );
    this.db.run(
      `create index if not exists workflow_instances_status_idx
       on workflow_instances(namespace, status, scheduled_at)`,
    );
    this.db.run(
      `create table if not exists workflow_step_results (
        namespace text not null,
        instance_id text not null,
        step_name text not null,
        result_json text,
        error_json text,
        created_at text not null,
        primary key(namespace, instance_id, step_name)
      )`,
    );
    this.db.run(
      `create table if not exists workflow_cron_runs (
        namespace text not null,
        run_key text not null,
        workflow_name text not null,
        cron_name text not null,
        scheduled_at text not null,
        envelope_id text not null,
        created_at text not null,
        primary key(namespace, run_key)
      )`,
    );
    this.db.run(
      `create table if not exists workflow_dead_letters (
        namespace text not null,
        id text not null,
        instance_id text,
        name text not null,
        envelope_json text not null,
        error_json text not null,
        created_at text not null,
        primary key(namespace, id)
      )`,
    );
    this.db
      .query(
        `insert into workflow_schema_versions(namespace, version, applied_at)
         values (?, 1, ?)
         on conflict(namespace, version) do nothing`,
      )
      .run(this.namespace, new Date().toISOString());
  }
}

function normalizeDateString(value: Date | string | undefined): string | undefined {
  if (value instanceof Date) return value.toISOString();
  if (typeof value === "string") return new Date(value).toISOString();
  return undefined;
}
