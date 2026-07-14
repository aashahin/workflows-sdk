# @abshahin/workflows-sdk

Runtime-neutral TypeScript workflow definitions with adapters for Bun and Cloudflare Workflows.

The SDK gives you one typed workflow contract:

- Define workflows once with `defineWorkflow()`.
- Dispatch standard envelopes with `createWorkflowClient()`.
- Run the same workflow definitions on Bun, a custom Cloudflare Worker endpoint, or Cloudflare's public Workflows REST API.
- Persist idempotency, cron claims, workflow state, step results, retries, and dead letters through runtime adapters.

## Installation

Install the package with your preferred JavaScript package manager:

```json
"@abshahin/workflows-sdk": "^0.1.0"
```

The package exports TypeScript source files directly. Use it from runtimes and bundlers that can load TypeScript subpath exports, or compile it as part of your application build.

### Runtime support

This package publishes its TypeScript sources as its entry points; it does not ship a compiled `dist/`. It is therefore consumed directly by:

- **Bun**, which runs TypeScript (and the `bun:sqlite`/`bun:test` imports used by the Bun adapter) natively, and
- **bundler-based TypeScript toolchains** (Vite, esbuild, Wrangler, Webpack, etc.) that resolve and compile TypeScript subpath exports as part of your application build.

Plain Node.js consuming the package without a TypeScript-aware loader/bundler, or a standalone `tsc` emit that expects prebuilt `.d.ts`/`.js` artifacts, is not supported. Compile the SDK as part of your own build if your toolchain requires that.

## Exports

| Import path | Purpose |
| --- | --- |
| `@abshahin/workflows-sdk` | Core workflow definition, registry, client, envelope, scheduler, and error types |
| `@abshahin/workflows-sdk/http` | `SignedHttpAdapter` for a custom worker endpoint with `/dispatch` and `/status/:id` |
| `@abshahin/workflows-sdk/cloudflare` | Cloudflare Worker dispatch handler, Workflow entrypoint helper, and REST API adapter |
| `@abshahin/workflows-sdk/bun` | Bun runtime plus SQLite and Redis adapters |
| `@abshahin/workflows-sdk/scheduler` | Cron helpers and cron definition types |
| `@abshahin/workflows-sdk/testing` | In-memory adapter for tests |

## Core API

Define workflow logic with a name, optional schema, optional cron definitions, optional retry/timeout defaults, and a `run()` function.

```ts
import {
  createWorkflowClient,
  defineWorkflow,
  defineWorkflowRegistry,
} from "@abshahin/workflows-sdk";
import { SignedHttpAdapter } from "@abshahin/workflows-sdk/http";

const sendEmail = defineWorkflow("email/send", {
  cron: [
    {
      name: "daily-digest",
      schedule: "0 9 * * *",
      payload: { kind: "daily-digest" },
    },
  ],
  retry: {
    maxAttempts: 3,
    initialIntervalMs: 1_000,
    multiplier: 2,
    maxIntervalMs: 30_000,
  },
  async run(ctx, payload) {
    await ctx.step("send", async () => {
      console.log("send email", payload);
    });
  },
});

export const registry = defineWorkflowRegistry([sendEmail]);

const client = createWorkflowClient({
  adapter: new SignedHttpAdapter({
    baseUrl: "https://workflows.example.com",
    authToken: process.env.WORKFLOWS_AUTH_TOKEN!,
  }),
});

await client.dispatch("email/send", { tenantId: "tenant_123" });
```

### Dispatch Options

`client.dispatch(name, payload, options)` accepts:

| Option | Meaning |
| --- | --- |
| `id` | Explicit workflow instance ID |
| `idempotencyKey` | Deduplication key used by adapters that support idempotency |
| `delayMs` | Relative delay before the workflow should run |
| `scheduledAt` | Absolute ISO string or `Date` for delayed execution |
| `traceId` | Trace/correlation ID |
| `metadata` | Extra envelope metadata |

Delayed envelopes are stored as `scheduled` by Bun adapters. Cloudflare runner helpers sleep inside the Workflow before running user code.

### Run Context

Every workflow receives a `ctx` object:

| Method/property | Purpose |
| --- | --- |
| `ctx.step(name, fn, options?)` | Runs a durable/idempotent step when the adapter/runtime supports step storage |
| `ctx.sleep(name, durationOrDate)` | Sleeps by duration string, milliseconds, or until a `Date` |
| `ctx.dispatch(name, payload, options?)` | Dispatches another workflow through the configured client |
| `ctx.event` | Original workflow envelope |
| `ctx.traceId` | Trace ID from the envelope |
| `ctx.idempotencyKey` | Idempotency key from the envelope |
| `ctx.logger` | Runtime logger |

Step results are cached by step name and workflow instance ID. Reusing the same step name for different side effects inside one workflow instance will reuse the first stored result.

## Bun Runtime

Use the Bun runtime when you want to execute workflows in a Bun process.

```ts
import {
  BunSqliteWorkflowAdapter,
  createBunWorkflowRuntime,
} from "@abshahin/workflows-sdk/bun";
import { registry } from "./workflows";

const runtime = createBunWorkflowRuntime({
  registry,
  adapter: new BunSqliteWorkflowAdapter({ path: "workflows.sqlite" }),
  concurrency: 4,
  scheduler: {
    mode: "external",
  },
});

await runtime.client.dispatch("email/send", { tenantId: "tenant_123" });
await runtime.tick();
await runtime.processReady();
```

### SQLite Adapter

`BunSqliteWorkflowAdapter` is the recommended single-server adapter. It stores:

- workflow instances
- scheduled and queued state
- idempotency keys
- cron run claims
- step results, including `undefined` results
- dead letters

```ts
import { BunSqliteWorkflowAdapter } from "@abshahin/workflows-sdk/bun";

const adapter = new BunSqliteWorkflowAdapter({
  path: "workflows.sqlite",
  namespace: "production",
});
```

### Redis Adapter

`BunRedisWorkflowAdapter` is intended for multi-instance Bun deployments. It uses Redis sorted sets, leases, idempotency keys, and step-result hashes.

```ts
import { BunRedisWorkflowAdapter } from "@abshahin/workflows-sdk/bun";

const adapter = new BunRedisWorkflowAdapter({
  url: Bun.env.REDIS_URL,
  namespace: "workflows",
  leaseTtlMs: 30_000,
});
```

You can also pass an existing Redis-like client:

```ts
const adapter = new BunRedisWorkflowAdapter({
  client: myRedisClient,
});
```

The adapter uses Bun's `RedisClient` when available. Raw Redis commands are sent through `redis.send(command, stringArgs)`.

### Scheduler Modes

| Mode | Use case | Behavior |
| --- | --- | --- |
| `external` | Kubernetes CronJob, systemd timer, queue worker, tests | You call `runtime.tick()` and/or `runtime.processReady()` yourself |
| `in-process` | Long-running Bun process | Schedules per-cron timers from the SDK's cron parser and processes due work in the same process (plus a periodic interval tick as a safety net) |
| `redis` | Multi-instance Bun deployment | Periodic interval tick as a wake-up mechanism and Redis for claims/leases |
| `os` | — | Not supported. `start()` throws: Bun has no OS cron registration API. Use `external` with your platform scheduler instead |

For an external scheduler (OS cron, Kubernetes CronJob, etc.), point it at a module that exports the scheduled handler:

```ts
import {
  BunSqliteWorkflowAdapter,
  createBunWorkflowScheduledHandler,
} from "@abshahin/workflows-sdk/bun";
import { registry } from "./workflows";

export default createBunWorkflowScheduledHandler({
  registry,
  adapter: new BunSqliteWorkflowAdapter({ path: "workflows.sqlite" }),
  scheduler: {
    mode: "external",
  },
});
```

Cron expressions use the standard 5-field form (an optional leading seconds field makes it 6). The SDK's own parser supports ranges, lists, steps, month/day names, and standard DOM-vs-DOW OR semantics. Schedules are evaluated in UTC unless a cron definition sets `timezone`; `scheduled()` invocations triggered by an OS-level scheduler (`controller.cron` set) are evaluated in the host's local timezone unless a cron definition overrides it.

### Cron Idempotency

Cron runs use deterministic keys:

```txt
${workflowName}:${cronName}:${scheduledAt.toISOString()}
```

SQLite or Redis stores the run key before dispatch, so duplicate wake-ups do not create duplicate workflow instances.

`missedRunPolicy` defaults to `skip`. Use catch-up mode when you explicitly want multiple missed runs:

```ts
const workflow = defineWorkflow("billing/hourly", {
  cron: [
    {
      name: "hourly",
      schedule: "0 * * * *",
      missedRunPolicy: { mode: "catch-up-all", maxRuns: 3 },
    },
  ],
  run: async () => {},
});
```

### Bun Retries and Recovery

The Bun runtime retries failed workflow runs when the workflow has a retry policy and the adapter implements `requeue()`.

- Step-level retry happens inside `ctx.step()`.
- Workflow-level retry requeues the same instance with attempt metadata.
- If retries are exhausted, the instance is marked `dead` and recorded as a dead letter when the adapter supports it.
- `recoverStalled()` returns stuck `running` instances to `queued` or `scheduled`.

## Cloudflare Workflows

There are two Cloudflare integration paths.

### Custom Worker Endpoint

Use `SignedHttpAdapter` from any producer service that dispatches to your own Worker endpoint:

```ts
import { createWorkflowClient } from "@abshahin/workflows-sdk";
import { SignedHttpAdapter } from "@abshahin/workflows-sdk/http";

const client = createWorkflowClient({
  adapter: new SignedHttpAdapter({
    baseUrl: "https://workflows.worker.example.com",
    authToken: Bun.env.WORKFLOWS_AUTH_TOKEN!,
  }),
});

await client.dispatch("email/send", { tenantId: "tenant_123" });
await client.getInstance("wf_123", { name: "email/send" });
```

The custom Worker endpoint must expose:

| Endpoint | Purpose |
| --- | --- |
| `POST /dispatch` | Accepts `{ events: WorkflowEventEnvelope[] }` and creates Workflow instances |
| `GET /status/:id?name=<eventName>` | Returns the instance status |
| `GET /health` | Optional health check |

You can build that Worker with `createCloudflareDispatchHandler()`:

```ts
import { createCloudflareDispatchHandler } from "@abshahin/workflows-sdk/cloudflare";
import { registry } from "./workflows";

export default createCloudflareDispatchHandler({
  registry,
  auth: {
    bearerToken: (env: { AUTH_TOKEN: string }) => env.AUTH_TOKEN,
  },
  resolveWorkflow(eventName, env: { EMAIL_WORKFLOW: Workflow }) {
    if (eventName.startsWith("email/")) return env.EMAIL_WORKFLOW;
    return null;
  },
});
```

The handler calls the Cloudflare binding with `Workflow.create({ id, params })`. Scheduled envelopes are passed as params and delayed by the Workflow entrypoint helper.

> **Rate limiting is best-effort.** The optional `rateLimit` option uses an in-memory fixed-window counter scoped to a single isolate. Cloudflare Workers run many isolates across PoPs, each with its own window, so the effective accepted rate is `max × <isolate count>` and a client spreading requests can bypass it. For true global enforcement, back it with a [Cloudflare Rate Limiting binding](https://developers.cloudflare.com/workers/runtime-apis/bindings/rate-limit/) or a Durable Object counter.

### Workflow Entrypoint Helper

Use `createCloudflareWorkflowEntrypoint()` when you want Cloudflare Workflows to execute SDK workflow definitions directly.

```ts
import { WorkflowEntrypoint } from "cloudflare:workers";
import { createCloudflareWorkflowEntrypoint } from "@abshahin/workflows-sdk/cloudflare";
import { registry } from "./workflows";

interface Env {
  AUTH_TOKEN: string;
}

const EmailWorkflowBase = createCloudflareWorkflowEntrypoint(
  WorkflowEntrypoint<Env>,
  { registry },
);

export class EmailWorkflow extends EmailWorkflowBase {}
```

The helper maps:

- `ctx.step()` to `step.do()`
- `ctx.sleep()` to `step.sleep()` or `step.sleepUntil()`
- workflow retry/timeout options to Cloudflare step config
- future `scheduledAt` envelopes to a durable Cloudflare sleep before user code runs

### Direct Cloudflare REST API

Use `CloudflareRestWorkflowAdapter` when you want to dispatch directly to Cloudflare's public Workflows REST API instead of your own Worker endpoint.

```ts
import { createWorkflowClient } from "@abshahin/workflows-sdk";
import { CloudflareRestWorkflowAdapter } from "@abshahin/workflows-sdk/cloudflare";

const client = createWorkflowClient({
  adapter: new CloudflareRestWorkflowAdapter({
    accountId: Bun.env.CLOUDFLARE_ACCOUNT_ID!,
    apiToken: Bun.env.CLOUDFLARE_API_TOKEN!,
    workflowName(eventName) {
      if (eventName.startsWith("email/")) return "email-workflow";
      throw new Error(`No Cloudflare Workflow for ${eventName}`);
    },
  }),
});

await client.dispatch("email/send", { tenantId: "tenant_123" });
await client.getInstance("wf_123", { name: "email/send" });
```

The adapter posts:

```json
{
  "instance_id": "wf_123",
  "params": {
    "id": "wf_123",
    "name": "email/send",
    "payload": {}
  }
}
```

to:

```txt
/accounts/{account_id}/workflows/{workflow_name}/instances
```

Status lookup requires either a prior dispatch through the same adapter instance or `getInstance(id, { name })`, because Cloudflare status endpoints are scoped to a workflow name.

## Testing

Use `InMemoryWorkflowAdapter` for unit tests:

```ts
import { createWorkflowClient } from "@abshahin/workflows-sdk";
import { InMemoryWorkflowAdapter } from "@abshahin/workflows-sdk/testing";

const adapter = new InMemoryWorkflowAdapter();
const client = createWorkflowClient({ adapter });

await client.dispatch("email/send", { tenantId: "tenant_123" });
```

For runtime-level tests, use `BunSqliteWorkflowAdapter({ path: ":memory:" })`.

## Error Classes

The root export includes:

- `WorkflowError`
- `WorkflowSendError`
- `WorkflowValidationError`
- `WorkflowRetryExhaustedError`
- `WorkflowNotFoundError`
- `WorkflowAlreadyClaimedError`

`SignedHttpAdapter` marks most non-429 `4xx` responses as non-retryable by setting `error.nonRetryable = true`.

## Production Notes

- Use Cloudflare Workflows for Worker deployments that need managed durable execution.
- Use SQLite for one Bun process/server.
- Use Redis for multiple Bun workers or multiple scheduler instances.
- Keep workflow instance IDs under Cloudflare's current instance ID limit when using the REST adapter.
- Keep workflow names under Cloudflare's current workflow name limit when using the REST adapter.
- Use unique, stable step names. Step results are keyed by instance ID and step name.
- Cron schedules are parsed by the SDK's own parser on every runtime (full 5/6-field support: ranges, lists, steps, names, DOM/DOW OR semantics).
- In-process cron timers evaluate in UTC unless a cron definition sets `timezone`; OS-triggered `scheduled()` ticks use the host timezone by default.
- This package currently ships TypeScript source via exports; compile before publishing to runtimes that cannot load TypeScript directly.

## Verification

Useful package-level checks:

```bash
bun test
bunx tsc --noEmit
```
