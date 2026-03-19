import type { WorkflowsClient } from "../client/workflows-client";
import {
  EMAIL_EVENTS,
  type CartRecoveryEmailData,
  type ChangeEmailVerificationData,
  type EnrollmentConfirmationEmailData,
  type FailedPaymentAlertEmailData,
  type InvitationEmailData,
  type NewAccountCredentialsEmailData,
  type PaymentReceiptEmailData,
  type RefundConfirmationEmailData,
  type ResetPasswordEmailData,
  type TrialReminderEmailData,
  type VerificationEmailData,
  type WithdrawalStatusEmailData,
} from "../contracts/email.contracts";
import type { SendOptions, SendResult } from "../core/types";

/** Options passed from backend call sites. */
interface JobOptions {
  delay?: number;
}

/**
 * Facade that mirrors the Inngest `EmailJobs` API shape 1:1.
 */
export class EmailJobs {
  constructor(
    private client: WorkflowsClient,
    private isEnabled: () => boolean = () => true,
  ) {}

  async sendResetPasswordEmail(
    data: ResetPasswordEmailData,
    options?: JobOptions,
  ): Promise<SendResult> {
    if (!this.isEnabled()) {
      console.warn("Email disabled — skipping reset password email");
      return { ids: [] };
    }

    return this.client.send(
      {
        name: EMAIL_EVENTS.RESET_PASSWORD,
        data,
      },
      toSendOptions(options),
    );
  }

  async sendVerificationEmail(
    data: VerificationEmailData,
    options?: JobOptions,
  ): Promise<SendResult> {
    if (!this.isEnabled()) {
      console.warn("Email disabled — skipping verification email");
      return { ids: [] };
    }

    return this.client.send(
      {
        name: EMAIL_EVENTS.VERIFICATION,
        data,
      },
      toSendOptions(options),
    );
  }

  async sendChangeEmailVerification(
    data: ChangeEmailVerificationData,
    options?: JobOptions,
  ): Promise<SendResult> {
    if (!this.isEnabled()) {
      console.warn("Email disabled — skipping email change verification");
      return { ids: [] };
    }

    return this.client.send(
      {
        name: EMAIL_EVENTS.CHANGE_EMAIL_VERIFICATION,
        data,
      },
      toSendOptions(options),
    );
  }

  async sendNewAccountCredentials(
    data: NewAccountCredentialsEmailData,
  ): Promise<SendResult> {
    if (!this.isEnabled()) {
      console.warn("Email disabled — skipping new account credentials email");
      return { ids: [] };
    }

    return this.client.send({
      name: EMAIL_EVENTS.NEW_ACCOUNT_CREDENTIALS,
      data,
    });
  }

  async sendInvitationEmail(data: InvitationEmailData): Promise<SendResult> {
    if (!this.isEnabled()) {
      console.warn("Email disabled — skipping invitation email");
      return { ids: [] };
    }

    return this.client.send({
      name: EMAIL_EVENTS.INVITATION,
      data,
    });
  }

  async sendEnrollmentConfirmationEmail(
    data: EnrollmentConfirmationEmailData,
  ): Promise<SendResult> {
    if (!this.isEnabled()) {
      console.warn("Email disabled — skipping enrollment confirmation email");
      return { ids: [] };
    }

    return this.client.send({
      name: EMAIL_EVENTS.ENROLLMENT_CONFIRMATION,
      data,
    });
  }

  async sendCartRecoveryEmail(
    data: {
      toEmail: string;
      subject: string;
      data: {
        userName: string;
        cartItems: CartRecoveryEmailData["cartItems"];
        cartTotal: CartRecoveryEmailData["cartTotal"];
        recoveryUrl: string;
      };
      recoveryToken: string;
      tenantName: string;
      tenantId?: string;
    },
    options?: JobOptions,
  ): Promise<SendResult> {
    if (!this.isEnabled()) {
      console.warn("Email disabled — skipping cart recovery email");
      return { ids: [] };
    }

    return this.client.send(
      {
        name: EMAIL_EVENTS.CART_RECOVERY,
        data: {
          toEmail: data.toEmail,
          subject: data.subject,
          userName: data.data.userName,
          cartItems: data.data.cartItems,
          cartTotal: data.data.cartTotal,
          recoveryUrl: data.data.recoveryUrl,
          recoveryToken: data.recoveryToken,
          tenantName: data.tenantName,
          tenantId: data.tenantId,
        },
      },
      toSendOptions(options),
    );
  }

  /**
   * Trial ending reminder.
   */
  async sendTrialEndingReminder(
    data: TrialReminderEmailData,
  ): Promise<SendResult> {
    if (!this.isEnabled()) {
      console.warn("Email disabled — skipping trial reminder email");
      return { ids: [] };
    }

    return this.client.send({
      name: EMAIL_EVENTS.TRIAL_REMINDER,
      data,
    });
  }

  // ─── Payment Emails ──────────────────────────────────────────────────────

  async sendPaymentReceiptEmail(
    data: PaymentReceiptEmailData,
  ): Promise<SendResult> {
    if (!this.isEnabled()) {
      console.warn("Email disabled — skipping payment receipt email");
      return { ids: [] };
    }

    return this.client.send({
      name: EMAIL_EVENTS.PAYMENT_RECEIPT,
      data,
    });
  }

  async sendWithdrawalStatusEmail(
    data: WithdrawalStatusEmailData,
  ): Promise<SendResult> {
    if (!this.isEnabled()) {
      console.warn("Email disabled — skipping withdrawal status email");
      return { ids: [] };
    }

    return this.client.send({
      name: EMAIL_EVENTS.WITHDRAWAL_STATUS,
      data,
    });
  }

  async sendFailedPaymentAlertEmail(
    data: FailedPaymentAlertEmailData,
  ): Promise<SendResult> {
    if (!this.isEnabled()) {
      console.warn("Email disabled — skipping failed payment alert email");
      return { ids: [] };
    }

    return this.client.send({
      name: EMAIL_EVENTS.FAILED_PAYMENT_ALERT,
      data,
    });
  }

  async sendRefundConfirmationEmail(
    data: RefundConfirmationEmailData,
  ): Promise<SendResult> {
    if (!this.isEnabled()) {
      console.warn("Email disabled — skipping refund confirmation email");
      return { ids: [] };
    }

    return this.client.send({
      name: EMAIL_EVENTS.REFUND_CONFIRMATION,
      data,
    });
  }
}

// ─── Internal ────────────────────────────────────────────────────────────────

function toSendOptions(options?: JobOptions): SendOptions | undefined {
  if (!options?.delay) return undefined;
  return { delay: options.delay };
}
