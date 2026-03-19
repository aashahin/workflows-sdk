// ─── Payment Event Contracts ─────────────────────────────────────────────────
// Typed contracts for payment workflow events (payout orchestration).

import type { WorkflowDispatchEvent } from "../core/types";

// ─── Payload types ───────────────────────────────────────────────────────────

export interface ProcessPayoutData {
    tenantId: string;
    transactionId: string;
    walletId: string;
    amount: number | string;
    currency: string;
    [key: string]: unknown;
}

// ─── Event name constants ────────────────────────────────────────────────────

export const PAYMENT_EVENTS = {
    PROCESS_PAYOUT: "payment/process-payout",
} as const;

export type PaymentEventName =
    (typeof PAYMENT_EVENTS)[keyof typeof PAYMENT_EVENTS];

// ─── Event types ─────────────────────────────────────────────────────────────

export type ProcessPayoutEvent = WorkflowDispatchEvent<
    typeof PAYMENT_EVENTS.PROCESS_PAYOUT,
    ProcessPayoutData
>;

/** Union of all payment events. */
export type PaymentEvent = ProcessPayoutEvent;
