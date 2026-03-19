// ─── Notification Event Contracts ────────────────────────────────────────────
// Typed contracts for all notification workflow events.

import type { WorkflowDispatchEvent } from "../core/types";

// ─── Payload types ───────────────────────────────────────────────────────────

/**
 * Shape of a notification object.
 * This is kept intentionally loose to avoid coupling the SDK package to
 * backend-specific Prisma types.  The backend's `TCreateNotification` type
 * should be assignable to this.
 */
export interface CreateNotificationPayload {
  title: string;
  message: string;
  type?: string;
  linkUrl?: string;
  [key: string]: unknown;
}

export interface CreateNotificationData {
  tenantId: string;
  userId: string;
  notification: CreateNotificationPayload;
}

export interface CreateCustomerNotificationData {
  tenantId: string;
  customerId: string;
  notification: CreateNotificationPayload;
}

export interface BulkCreateNotificationsData {
  tenantId: string;
  notification: CreateNotificationPayload;
  userIds?: string[];
  customerIds?: string[];
}

// ─── Event name constants ────────────────────────────────────────────────────

export const NOTIFICATION_EVENTS = {
  CREATE: "notification/create",
  CREATE_FOR_CUSTOMER: "notification/create-for-customer",
  BULK_CREATE: "notification/bulk-create",
} as const;

export type NotificationEventName =
  (typeof NOTIFICATION_EVENTS)[keyof typeof NOTIFICATION_EVENTS];

// ─── Event types ─────────────────────────────────────────────────────────────

export type CreateNotificationEvent = WorkflowDispatchEvent<
  typeof NOTIFICATION_EVENTS.CREATE,
  CreateNotificationData
>;

export type CreateCustomerNotificationEvent = WorkflowDispatchEvent<
  typeof NOTIFICATION_EVENTS.CREATE_FOR_CUSTOMER,
  CreateCustomerNotificationData
>;

export type BulkCreateNotificationsEvent = WorkflowDispatchEvent<
  typeof NOTIFICATION_EVENTS.BULK_CREATE,
  BulkCreateNotificationsData
>;

/** Union of all notification events. */
export type NotificationEvent =
  | CreateNotificationEvent
  | CreateCustomerNotificationEvent
  | BulkCreateNotificationsEvent;
