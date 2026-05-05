# @abshahin/workflows-sdk

TypeScript SDK for dispatching typed background workflow events to a Cloudflare Worker runtime.

The package is transport-oriented and framework-agnostic. It provides a client, an HTTP transport, strongly typed event contracts, retry utilities, and job facades for email, notification, and payment workflows.

It also provides a dedicated WhatsApp workflow facade for asynchronous template-message dispatch.

Related worker runtime: `https://github.com/aashahin/cloudflare-workflows-worker`

## Why this package exists

`@abshahin/workflows-sdk` separates event production from workflow execution.

- Producers only need to know how to send typed events
- The transport layer decides how those events reach the workflow runtime
- Event names and payloads stay typed across package boundaries
- Existing application code can migrate away from Inngest-style producers with minimal surface change
- The default companion runtime for this SDK is the Workflows Worker: `https://github.com/aashahin/cloudflare-workflows-worker`

## Features

- Typed contracts for email, notification, and payment events
- Typed contracts for WhatsApp template-send events
- HTTP transport for dispatching event batches to a worker endpoint
- Producer-friendly job facades: `EmailJobs`, `NotificationJobs`, `PaymentJobs`, `WhatsappJobs`
- Transport retry support with exponential backoff
- Optional dual-run mode for phased migrations
- Optional `onSendExhausted` hook for persisting failed transport attempts
- Sub-path exports for contracts and helpers

## Installation

Inside this monorepo:

```json
"@abshahin/workflows-sdk": "workspace:*"
```

Once published publicly, install it with your package manager of choice:

```bash
bun add @abshahin/workflows-sdk
```

```bash
npm install @abshahin/workflows-sdk
```

## Quick start

```typescript
import {
  EmailJobs,
  HttpTransport,
  NotificationJobs,
  PaymentJobs,
  WhatsappJobs,
  createWorkflowsClient,
} from "@abshahin/workflows-sdk";

const client = createWorkflowsClient({
  transport: new HttpTransport({
    baseUrl: "https://workflows.example.com",
    authToken: process.env.WORKFLOWS_AUTH_TOKEN!,
  }),
});

const emailJobs = new EmailJobs(client, () => true);
const notificationJobs = new NotificationJobs(client);
const paymentJobs = new PaymentJobs(client);
const whatsappJobs = new WhatsappJobs(client);

await emailJobs.sendResetPasswordEmail({
  email: "user@example.com",
  userName: "John",
  otpCode: "123456",
  tenantId: "tenant_123",
});

await notificationJobs.addNotification(
  "tenant_123",
  "user_456",
  {
    title: "New course available",
    message: "Check the dashboard for details.",
    type: "info",
  },
  { delay: 5_000 },
);

await paymentJobs.processPayout({
  tenantId: "tenant_123",
  transactionId: "txn_001",
  walletId: "wallet_001",
  amount: 250,
  currency: "USD",
});

await whatsappJobs.sendTemplateMessage({
  tenantId: "tenant_123",
  recipientPhone: "+15551234567",
  templateKey: "order_confirmed",
  triggerEvent: "order.completed",
  variables: {
    customer_name: "John",
    course_name: "Arabic 101",
    access_link: "https://tenant.example.com/dashboard",
  },
  customerId: "customer_789",
});
```

## How it fits together

```mermaid
flowchart LR
  A[Application code] --> B[Job facade]
  B --> C[WorkflowsClient]
  C --> D[Transport]
  D --> E[Worker /dispatch endpoint]
  E --> F[Workflow runtime]
```

## Core API

### `createWorkflowsClient`

Creates a `WorkflowsClient` from a transport configuration.

Use it when you want a minimal factory instead of constructing the client class directly.

### `HttpTransport`

Sends one or more workflow events to the worker's `/dispatch` endpoint.

Config:

- `baseUrl`: worker base URL
- `authToken`: shared bearer token
- `timeoutMs`: request timeout, default `10000`
- `retry`: retry policy, or `false` to disable transport retries

Transport behavior:

- Trims trailing slashes from `baseUrl`
- Wraps network/timeouts as `WorkflowSendError`
- Treats `429` and all `5xx` responses as retryable
- Treats other `4xx` responses as non-retryable transport failures
- Logs partial batch failures without throwing away successful IDs

### `EmailJobs`

Facade for email-related workflow events.

Notable methods:

- `sendResetPasswordEmail`
- `sendVerificationEmail`
- `sendChangeEmailVerification`
- `sendNewAccountCredentials`
- `sendInvitationEmail`
- `sendEnrollmentConfirmationEmail`
- `sendCartRecoveryEmail`
- `sendTrialEndingReminder`
- `sendPaymentReceiptEmail`
- `sendWithdrawalStatusEmail`
- `sendFailedPaymentAlertEmail`
- `sendRefundConfirmationEmail`

The constructor accepts an optional `isEnabled` callback to short-circuit email dispatch when email delivery is disabled.

### `NotificationJobs`

Facade for notification-related workflow events.

Notable methods:

- `addNotification`
- `addNotificationForCustomer`
- `addBulkNotification`
- `addMultipleNotifications`

`addMultipleNotifications` preserves per-item delays by sending individual events only when needed; otherwise it batches them.

### `PaymentJobs`

Facade for payment-related workflow events.

Current method:

- `processPayout`

This dispatches the payout orchestration workflow handled by the worker runtime.

### `WhatsappJobs`

Facade for WhatsApp-related workflow events.

Current method:

- `sendTemplateMessage`

This dispatches an asynchronous template-message send that the backend executes through its WhatsApp delivery service.

## Usage examples

### Email workflow dispatch

```typescript
import {
  EmailJobs,
  HttpTransport,
  createWorkflowsClient,
} from "@abshahin/workflows-sdk";

const client = createWorkflowsClient({
  transport: new HttpTransport({
    baseUrl: "https://workflows.example.com",
    authToken: "secret",
  }),
});

const emailJobs = new EmailJobs(client, () => true);

await emailJobs.sendVerificationEmail(
  {
    email: "user@example.com",
    otpCode: "123456",
    tenantId: "tenant_123",
  },
  { delay: 5_000 },
);
```

### Notification workflow dispatch

```typescript
import {
  HttpTransport,
  NotificationJobs,
  createWorkflowsClient,
} from "@abshahin/workflows-sdk";

const client = createWorkflowsClient({
  transport: new HttpTransport({
    baseUrl: "https://workflows.example.com",
    authToken: "secret",
  }),
});

const notificationJobs = new NotificationJobs(client);

await notificationJobs.addBulkNotification(
  "tenant_123",
  {
    title: "Billing update",
    message: "Your subscription has been renewed.",
    type: "billing",
  },
  ["user_1", "user_2"],
);
```

### Payment workflow dispatch

```typescript
import {
  HttpTransport,
  PaymentJobs,
  createWorkflowsClient,
} from "@abshahin/workflows-sdk";

const client = createWorkflowsClient({
  transport: new HttpTransport({
    baseUrl: "https://workflows.example.com",
    authToken: "secret",
  }),
});

const paymentJobs = new PaymentJobs(client);

await paymentJobs.processPayout({
  tenantId: "tenant_123",
  transactionId: "txn_001",
  walletId: "wallet_001",
  amount: 250,
  currency: "USD",
});
```

### WhatsApp workflow dispatch

```typescript
import {
  HttpTransport,
  WhatsappJobs,
  createWorkflowsClient,
} from "@abshahin/workflows-sdk";

const client = createWorkflowsClient({
  transport: new HttpTransport({
    baseUrl: "https://workflows.example.com",
    authToken: "secret",
  }),
});

const whatsappJobs = new WhatsappJobs(client);

await whatsappJobs.sendTemplateMessage({
  tenantId: "tenant_123",
  recipientPhone: "+15551234567",
  templateKey: "welcome_student",
  triggerEvent: "enrollment.created",
  variables: {
    student_name: "John",
    course_name: "Arabic 101",
    start_link: "https://tenant.example.com/course/intro/learn",
  },
});
```

## Advanced patterns

### Dual-run mode

Use dual-run mode when migrating between workflow backends or validating a new transport.

```typescript
import { HttpTransport, createWorkflowsClient } from "@abshahin/workflows-sdk";

const primaryTransport = new HttpTransport({
  baseUrl: "https://primary.example.com",
  authToken: "primary-secret",
});

const shadowTransport = new HttpTransport({
  baseUrl: "https://shadow.example.com",
  authToken: "shadow-secret",
});

const client = createWorkflowsClient({
  transport: primaryTransport,
  shadowTransport,
  dualRun: true,
});
```

The primary transport result is returned. Shadow transport failures are logged and suppressed.

### Persist exhausted transport failures

Use `onSendExhausted` when you want to store failed dispatches for later replay instead of letting producer-side business logic fail immediately.

```typescript
import { HttpTransport, createWorkflowsClient } from "@abshahin/workflows-sdk";

const client = createWorkflowsClient({
  transport: new HttpTransport({
    baseUrl: "https://workflows.example.com",
    authToken: "secret",
  }),
  onSendExhausted: async ({ events, options, error, attempts }) => {
    console.error("Persist failed workflow dispatch", {
      attempts,
      error: error.message,
      eventCount: events.length,
      traceId: options?.traceId,
    });
  },
});
```

When persistence succeeds, the client returns `{ ids: [] }` instead of throwing.

## Event domains

### Email events

- `email/reset-password`
- `email/new-account-credentials`
- `email/change-email-verification`
- `email/verification`
- `email/cart-recovery`
- `email/invitation`
- `email/enrollment-confirmation`
- `email/trial-reminder`
- `email/payment-receipt`
- `email/withdrawal-status`
- `email/failed-payment-alert`
- `email/refund-confirmation`

### Notification events

- `notification/create`
- `notification/create-for-customer`
- `notification/bulk-create`

### Payment events

- `payment/process-payout`

### WhatsApp events

- `whatsapp/send-template`

## Exports

| Export                                                                                            | Description                   |
| ------------------------------------------------------------------------------------------------- | ----------------------------- |
| `createWorkflowsClient`                                                                           | Factory for `WorkflowsClient` |
| `WorkflowsClient`                                                                                 | Core dispatch client          |
| `HttpTransport`                                                                                   | HTTP transport adapter        |
| `EmailJobs`                                                                                       | Email job facade              |
| `NotificationJobs`                                                                                | Notification job facade       |
| `PaymentJobs`                                                                                     | Payment job facade            |
| `WhatsappJobs`                                                                                    | WhatsApp job facade           |
| `EMAIL_EVENTS` / `NOTIFICATION_EVENTS` / `PAYMENT_EVENTS`                                         | Event name constants          |
| `WHATSAPP_EVENTS`                                                                                 | WhatsApp event constants      |
| `withRetry` / `getBackoffDelay` / `DEFAULT_RETRY_POLICY`                                          | Retry helpers                 |
| `deriveIdempotencyKey` / `generateEventId`                                                        | Idempotency helpers           |
| `WorkflowError` / `WorkflowSendError` / `WorkflowValidationError` / `WorkflowRetryExhaustedError` | Typed errors                  |

## Sub-path exports

The SDK exposes sub-path imports for tree-shaking or isolated usage:

```typescript
import {
  EMAIL_EVENTS,
  type ResetPasswordEmailData,
} from "@abshahin/workflows-sdk/contracts";
import {
  deriveIdempotencyKey,
  withRetry,
} from "@abshahin/workflows-sdk/helpers";
```

## Migration from Inngest

The job facades are designed to make migration straightforward, but there is one important API difference.

Previous Inngest usage relied on static methods:

```diff
- EmailJobs.sendResetPasswordEmail(data);
+ emailJobs.sendResetPasswordEmail(data);
```

The SDK uses instance-based facades so transports, feature flags, and fallback behavior can be injected once at application bootstrap.

## License

MIT. Add the `LICENSE` file in the published `workflows-sdk` repository.
