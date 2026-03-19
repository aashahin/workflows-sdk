// ─── Payment Jobs ────────────────────────────────────────────────────────────
// Job facade for payment workflow events (payout orchestration).

import type { WorkflowsClient } from "../client/workflows-client";
import type { SendResult } from "../core/types";
import {
    PAYMENT_EVENTS,
    type ProcessPayoutData,
} from "../contracts/payment.contracts";

/**
 * Facade for dispatching payment workflow events.
 */
export class PaymentJobs {
    constructor(private client: WorkflowsClient) { }

    /**
     * Dispatch a payout processing workflow.
     * Orchestrates: validate → process → notify.
     */
    async processPayout(data: ProcessPayoutData): Promise<SendResult> {
        return this.client.send({
            name: PAYMENT_EVENTS.PROCESS_PAYOUT,
            data,
        });
    }
}
