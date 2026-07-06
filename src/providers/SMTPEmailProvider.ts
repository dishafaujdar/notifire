import nodemailer, { type Transporter } from 'nodemailer';
import type SMTPTransport from 'nodemailer/lib/smtp-transport/index.js';
import type { EmailMessage, JobResult } from '../types.js';
import type { ChannelProvider } from './ChannelProvider.js';

export class SMTPEmailProvider implements ChannelProvider<EmailMessage> {
  readonly name = 'smtp';
  private readonly transporter: Transporter<SMTPTransport.SentMessageInfo>;
  private readonly from: string;

  constructor(config: SMTPTransport.Options & { from: string }) {
    this.from = config.from;
    this.transporter = nodemailer.createTransport(config);
  }

  async send(message: EmailMessage): Promise<JobResult> {
    try {
      await this.transporter.sendMail({
        from: this.from,
        to: message.to,
        subject: message.subject,
        html: message.html
      });

      return { ok: true };
    } catch (error) {
      return {
        ok: false,
        retry: true,
        error: error instanceof Error ? error.message : String(error)
      };
    }
  }
}
