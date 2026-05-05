import type { WorkflowsClient } from "../client/workflows-client";
import {
  WHATSAPP_EVENTS,
  type SendWhatsappTemplateData,
} from "../contracts/whatsapp.contracts";
import type { SendOptions, SendResult } from "../core/types";

interface JobOptions {
  delay?: number;
}

export class WhatsappJobs {
  constructor(private client: WorkflowsClient) {}

  async sendTemplateMessage(
    data: SendWhatsappTemplateData,
    options?: JobOptions,
  ): Promise<SendResult> {
    return this.client.send(
      {
        name: WHATSAPP_EVENTS.SEND_TEMPLATE,
        data,
      },
      toSendOptions(options),
    );
  }
}

function toSendOptions(options?: JobOptions): SendOptions | undefined {
  if (!options?.delay) return undefined;
  return { delay: options.delay };
}