// ─── @shahin/workflows-sdk ───────────────────────────────────────────────────
// Public API surface.

// Core
export {
  WorkflowError,
  WorkflowRetryExhaustedError,
  WorkflowSendError,
  WorkflowValidationError,
} from "./core/errors";
export type {
  RetryPolicy,
  SendExhaustedContext,
  SendOptions,
  SendResult,
  WorkflowDispatchEvent,
  WorkflowTransport,
  WorkflowsClientConfig,
} from "./core/types";

// Client
export { WorkflowsClient, createWorkflowsClient } from "./client/index";

// Transports
export { HttpTransport, type HttpTransportConfig } from "./transports/index";

// Jobs (producer API parity)
export { EmailJobs } from "./jobs/email-jobs";
export { NotificationJobs } from "./jobs/notification-jobs";
export { PaymentJobs } from "./jobs/payment-jobs";

// Contracts
export {
  EMAIL_EVENTS,
  NOTIFICATION_EVENTS,
  PAYMENT_EVENTS,
  type BulkCreateNotificationsData,
  type BulkCreateNotificationsEvent,
  type CartRecoveryEmailData,
  type CartRecoveryEmailEvent,
  type ChangeEmailVerificationData,
  type ChangeEmailVerificationEvent,
  type CreateCustomerNotificationData,
  type CreateCustomerNotificationEvent,
  type CreateNotificationData,
  // Notification event types
  type CreateNotificationEvent,
  // Notification data types
  type CreateNotificationPayload,
  type EmailEvent,
  type EmailEventName,
  type EnrollmentConfirmationEmailData,
  type EnrollmentConfirmationEmailEvent,
  // Payment data types
  type FailedPaymentAlertEmailData,
  type FailedPaymentAlertEmailEvent,
  type InvitationEmailData,
  type InvitationEmailEvent,
  type NewAccountCredentialsEmailData,
  type NewAccountCredentialsEmailEvent,
  type NotificationEvent,
  type NotificationEventName,
  // Payment event types
  type PaymentEvent,
  type PaymentEventName,
  type PaymentReceiptEmailData,
  type PaymentReceiptEmailEvent,
  type ProcessPayoutData,
  type ProcessPayoutEvent,
  type RefundConfirmationEmailData,
  type RefundConfirmationEmailEvent,
  // Email data types
  type ResetPasswordEmailData,
  // Email event types
  type ResetPasswordEmailEvent,
  type TrialReminderEmailData,
  type TrialReminderEmailEvent,
  type VerificationEmailData,
  type VerificationEmailEvent,
  type WithdrawalStatusEmailData,
  type WithdrawalStatusEmailEvent,
} from "./contracts/index";

// Helpers
export { deriveIdempotencyKey, generateEventId } from "./helpers/idempotency";
export {
  DEFAULT_RETRY_POLICY,
  getBackoffDelay,
  withRetry,
} from "./helpers/retry";
