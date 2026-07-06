export interface Workflow {
  trigger: string;
  steps: { channel: 'email'; templateId: string }[];
}

export interface NotificationJob {
  id: string;
  trigger: string;
  channel: 'email';
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
