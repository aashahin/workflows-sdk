// ─── Notification Jobs ───────────────────────────────────────────────────────
// Drop-in replacement for the Inngest-backed NotificationJobs class.
// Same method signatures and return types — only the transport changes.

import type { WorkflowsClient } from "../client/workflows-client";
import {
  NOTIFICATION_EVENTS,
  type CreateNotificationPayload,
} from "../contracts/notification.contracts";
import type { SendOptions, SendResult } from "../core/types";

/** Options passed from backend call sites. */
interface JobOptions {
  delay?: number;
  priority?: number; // reserved; no-op for now
}

/**
 * Facade that mirrors the Inngest `NotificationJobs` API shape 1:1.
 */
export class NotificationJobs {
  constructor(private client: WorkflowsClient) {}

  /**
   * Add a single notification to the queue.
   */
  async addNotification(
    tenantId: string,
    userId: string,
    notification: CreateNotificationPayload,
    options?: JobOptions,
  ): Promise<SendResult> {
    return this.client.send(
      {
        name: NOTIFICATION_EVENTS.CREATE,
        data: { tenantId, userId, notification },
      },
      toSendOptions(options),
    );
  }

  /**
   * Add a single notification for a customer to the queue.
   */
  async addNotificationForCustomer(
    tenantId: string,
    customerId: string,
    notification: CreateNotificationPayload,
    options?: JobOptions,
  ): Promise<SendResult> {
    return this.client.send(
      {
        name: NOTIFICATION_EVENTS.CREATE_FOR_CUSTOMER,
        data: { tenantId, customerId, notification },
      },
      toSendOptions(options),
    );
  }

  /**
   * Add bulk notifications to the queue.
   */
  async addBulkNotification(
    tenantId: string,
    notification: CreateNotificationPayload,
    userIds?: string[],
    customerIds?: string[],
    options?: JobOptions,
  ): Promise<SendResult> {
    return this.client.send(
      {
        name: NOTIFICATION_EVENTS.BULK_CREATE,
        data: { tenantId, notification, userIds, customerIds },
      },
      toSendOptions(options),
    );
  }

  /**
   * Add multiple individual notifications to the queue.
   * When any item has a per-event delay, events are sent individually
   * to preserve each item's delay. Otherwise, they are batched.
   */
  async addMultipleNotifications(
    tenantId: string,
    notifications: Array<{
      userId: string;
      notification: CreateNotificationPayload;
      options?: JobOptions;
    }>,
  ): Promise<SendResult> {
    const hasIndividualDelays = notifications.some((n) => n.options?.delay);

    // Per-event delays require individual sends
    if (hasIndividualDelays) {
      const allIds: string[] = [];
      for (const item of notifications) {
        const result = await this.client.send(
          {
            name: NOTIFICATION_EVENTS.CREATE,
            data: {
              tenantId,
              userId: item.userId,
              notification: item.notification,
            },
          },
          toSendOptions(item.options),
        );
        allIds.push(...result.ids);
      }
      return { ids: allIds };
    }

    // No delays — batch send
    const events = notifications.map((item) => ({
      name: NOTIFICATION_EVENTS.CREATE,
      data: {
        tenantId,
        userId: item.userId,
        notification: item.notification,
      },
    }));
    return this.client.send(events);
  }
}

// ─── Internal ────────────────────────────────────────────────────────────────

function toSendOptions(options?: JobOptions): SendOptions | undefined {
  if (!options?.delay) return undefined;
  return { delay: options.delay };
}
