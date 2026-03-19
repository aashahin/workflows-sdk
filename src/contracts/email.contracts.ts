// ─── Email Event Contracts ───────────────────────────────────────────────────
// Typed contracts for all email workflow events.
// These mirror the current Inngest event types exactly for drop-in parity.

import type { WorkflowDispatchEvent } from "../core/types";

// ─── Payload types ───────────────────────────────────────────────────────────

export interface ResetPasswordEmailData {
  email: string;
  userName: string;
  otpCode: string;
  tenantName?: string;
  tenantId?: string;
}

export interface NewAccountCredentialsEmailData {
  email: string;
  userName: string;
  temporaryPassword: string;
  loginUrl: string;
  tenantName: string;
  tenantId?: string;
}

export interface ChangeEmailVerificationData {
  newEmail: string;
  oldEmail: string;
  userName: string;
  otpCode: string;
  tenantName?: string;
  tenantId?: string;
}

export interface VerificationEmailData {
  email: string;
  otpCode: string;
  tenantName?: string;
  tenantId?: string;
}

export interface CartRecoveryEmailData {
  toEmail: string;
  subject: string;
  userName: string;
  cartItems: Array<{
    name: string;
    price: number | string; // Prisma.Decimal serializes to string across boundaries
    quantity: number;
  }>;
  cartTotal: number | string;
  recoveryUrl: string;
  recoveryToken: string;
  tenantName: string;
  tenantId?: string;
}

export interface InvitationEmailData {
  email: string;
  inviterName: string;
  tenantName: string;
  role: string;
  invitationUrl: string;
  expiresAt: string; // ISO date string — serialized across boundary
  tenantId?: string;
}

export interface EnrollmentConfirmationEmailData {
  email: string;
  courseName: string;
  userName: string;
  tenantName: string;
  courseUrl?: string;
  tenantId?: string;
}

export interface TrialReminderEmailData {
  email: string;
  userName: string;
  planName: string;
  planPrice: number;
  currency: string;
  daysRemaining: number;
  billingUrl: string;
  tenantId?: string;
}

export interface PaymentReceiptEmailData {
  email: string;
  customerName: string;
  amount: number | string;
  currency: string;
  transactionId: string;
  items: Array<{ name: string; price: number | string; quantity: number }>;
  tenantName: string;
  tenantId?: string;
}

export interface WithdrawalStatusEmailData {
  email: string;
  ownerName: string;
  amount: number | string;
  currency: string;
  status: "success" | "failed" | "canceled";
  transactionId: string;
  tenantName: string;
  tenantId?: string;
}

export interface FailedPaymentAlertEmailData {
  email: string;
  adminName: string;
  customerName: string;
  amount: number | string;
  currency: string;
  transactionId: string;
  reason?: string;
  tenantName: string;
  tenantId?: string;
}

export interface RefundConfirmationEmailData {
  email: string;
  customerName: string;
  amount: number | string;
  currency: string;
  transactionId: string;
  tenantName: string;
  tenantId?: string;
}

// ─── Event name constants ────────────────────────────────────────────────────

export const EMAIL_EVENTS = {
  RESET_PASSWORD: "email/reset-password",
  NEW_ACCOUNT_CREDENTIALS: "email/new-account-credentials",
  CHANGE_EMAIL_VERIFICATION: "email/change-email-verification",
  VERIFICATION: "email/verification",
  CART_RECOVERY: "email/cart-recovery",
  INVITATION: "email/invitation",
  ENROLLMENT_CONFIRMATION: "email/enrollment-confirmation",
  TRIAL_REMINDER: "email/trial-reminder",
  PAYMENT_RECEIPT: "email/payment-receipt",
  WITHDRAWAL_STATUS: "email/withdrawal-status",
  FAILED_PAYMENT_ALERT: "email/failed-payment-alert",
  REFUND_CONFIRMATION: "email/refund-confirmation",
} as const;

export type EmailEventName = (typeof EMAIL_EVENTS)[keyof typeof EMAIL_EVENTS];

// ─── Event types ─────────────────────────────────────────────────────────────

export type ResetPasswordEmailEvent = WorkflowDispatchEvent<
  typeof EMAIL_EVENTS.RESET_PASSWORD,
  ResetPasswordEmailData
>;

export type NewAccountCredentialsEmailEvent = WorkflowDispatchEvent<
  typeof EMAIL_EVENTS.NEW_ACCOUNT_CREDENTIALS,
  NewAccountCredentialsEmailData
>;

export type ChangeEmailVerificationEvent = WorkflowDispatchEvent<
  typeof EMAIL_EVENTS.CHANGE_EMAIL_VERIFICATION,
  ChangeEmailVerificationData
>;

export type VerificationEmailEvent = WorkflowDispatchEvent<
  typeof EMAIL_EVENTS.VERIFICATION,
  VerificationEmailData
>;

export type CartRecoveryEmailEvent = WorkflowDispatchEvent<
  typeof EMAIL_EVENTS.CART_RECOVERY,
  CartRecoveryEmailData
>;

export type InvitationEmailEvent = WorkflowDispatchEvent<
  typeof EMAIL_EVENTS.INVITATION,
  InvitationEmailData
>;

export type EnrollmentConfirmationEmailEvent = WorkflowDispatchEvent<
  typeof EMAIL_EVENTS.ENROLLMENT_CONFIRMATION,
  EnrollmentConfirmationEmailData
>;

export type TrialReminderEmailEvent = WorkflowDispatchEvent<
  typeof EMAIL_EVENTS.TRIAL_REMINDER,
  TrialReminderEmailData
>;

export type PaymentReceiptEmailEvent = WorkflowDispatchEvent<
  typeof EMAIL_EVENTS.PAYMENT_RECEIPT,
  PaymentReceiptEmailData
>;

export type WithdrawalStatusEmailEvent = WorkflowDispatchEvent<
  typeof EMAIL_EVENTS.WITHDRAWAL_STATUS,
  WithdrawalStatusEmailData
>;

export type FailedPaymentAlertEmailEvent = WorkflowDispatchEvent<
  typeof EMAIL_EVENTS.FAILED_PAYMENT_ALERT,
  FailedPaymentAlertEmailData
>;

export type RefundConfirmationEmailEvent = WorkflowDispatchEvent<
  typeof EMAIL_EVENTS.REFUND_CONFIRMATION,
  RefundConfirmationEmailData
>;

/** Union of all email events. */
export type EmailEvent =
  | ResetPasswordEmailEvent
  | NewAccountCredentialsEmailEvent
  | ChangeEmailVerificationEvent
  | VerificationEmailEvent
  | CartRecoveryEmailEvent
  | InvitationEmailEvent
  | EnrollmentConfirmationEmailEvent
  | TrialReminderEmailEvent
  | PaymentReceiptEmailEvent
  | WithdrawalStatusEmailEvent
  | FailedPaymentAlertEmailEvent
  | RefundConfirmationEmailEvent;
