export type Channel = 'email';

export interface ChannelHandler {
  channel: Channel;
  process(job: NotificationJob): Promise<JobResult>;
}

export interface Workflow {
  trigger: string;
  steps: { channel: Channel; templateId: string }[];
}

export interface NotificationJob {
  id: string;
  trigger: string;
  channel: Channel;
  recipient: { email: string };
  data: Record<string, unknown>;
  templateId: string;
}

export interface EmailMessage {
  to: string;
  subject: string;
  html: string;
}

export interface TriggerPayload {
  recipient: { email: string };
  data: Record<string, unknown>;
}

export type JobResult =
  | { ok: true }
  | { ok: false; retry: boolean; error: string };
