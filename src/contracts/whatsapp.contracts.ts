// ─── WhatsApp Event Contracts ───────────────────────────────────────────────
// Typed contracts for WhatsApp workflow events.

import type { WorkflowDispatchEvent } from "../core/types";

// ─── Payload types ─────────────────────────────────────────────────────────

export interface SendWhatsappTemplateData {
  tenantId: string;
  recipientPhone: string;
  templateKey: string;
  triggerEvent: string;
  variables: Record<string, string>;
  customerId?: string;
}

// ─── Event name constants ──────────────────────────────────────────────────

export const WHATSAPP_EVENTS = {
  SEND_TEMPLATE: "whatsapp/send-template",
} as const;

export type WhatsappEventName =
  (typeof WHATSAPP_EVENTS)[keyof typeof WHATSAPP_EVENTS];

// ─── Event types ───────────────────────────────────────────────────────────

export type SendWhatsappTemplateEvent = WorkflowDispatchEvent<
  typeof WHATSAPP_EVENTS.SEND_TEMPLATE,
  SendWhatsappTemplateData
>;

/** Union of all WhatsApp events. */
export type WhatsappEvent = SendWhatsappTemplateEvent;