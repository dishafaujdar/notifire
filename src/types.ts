export type Channel = 'email' | 'in_app';

export interface InAppMessage {
  recipientId: string;
  title: string;
  body: string;
  data?: Record<string, unknown>;
}


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
  recipient: { email?: string; recipientId?: string };
  data: Record<string, unknown>;
  templateId: string;
}

export interface EmailMessage {
  to: string;
  subject: string;
  html: string;
}

export interface TriggerPayload {
  recipient: { email?: string; recipientId?: string };
  data: Record<string, unknown>;
}

export type JobResult =
  | { ok: true }
  | { ok: false; retry: boolean; error: string };
