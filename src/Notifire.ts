// Notifire.ts — thinned down
import { randomUUID } from 'node:crypto';
import { InMemoryQueueAdapter } from './queue/InMemoryQueueAdapter.js';
import { Worker } from './workers/worker.js';
import { EmailHandler } from './handlers/EmailHandler.js';
import type { QueueAdapter } from './queue/QueueAdapter.js';
import type { ChannelProvider } from './providers/ChannelProvider.js';
import type { EmailMessage, InAppMessage, NotificationJob, TriggerPayload, Workflow } from './types.js';

interface NotifireConfig {
  queue?: QueueAdapter;
  provider: {
    email: ChannelProvider<EmailMessage>;
    inApp?: ChannelProvider<InAppMessage>;
  };
  templatesDir: string;
}

export class Notifire {
  private readonly queue: QueueAdapter;
  private readonly workflows = new Map<string, Workflow>();
  private readonly worker: Worker;
  private started = false;

  constructor(config: NotifireConfig) {
    this.queue = config.queue ?? new InMemoryQueueAdapter();
    this.worker = new Worker(this.queue, [
      new EmailHandler(config.provider.email, config.templatesDir)
    ]);
  }

  defineWorkflow(workflow: Workflow): void {
    this.workflows.set(workflow.trigger, workflow);
  }

  start(): void {
    if (this.started) return;
    this.started = true;
    this.worker.start();
  }

  async trigger(triggerName: string, payload: TriggerPayload): Promise<void> {
    const workflow = this.workflows.get(triggerName);
    if (!workflow) {
      throw new Error(`Workflow "${triggerName}" is not registered.`);
    }
    if (!isRecord(payload?.data)) {
      throw new Error('Notification payload data must be an object.');
    }
    if (!payload.recipient?.email) {
      throw new Error('Notification payload recipient.email is required.');
    }

    const recipients = Array.isArray(payload.recipient) ? payload.recipient : [payload.recipient];
    const CHUNK_SIZE = 5000;
  

    for (const step of workflow.steps) {
      for (let i = 0; i < recipients.length; i += CHUNK_SIZE) {
        const chunk = recipients.slice(i, i + CHUNK_SIZE);
        const jobs: NotificationJob[] = chunk.map((recipient) => ({
          id: randomUUID(),
          trigger: workflow.trigger,
          channel: step.channel,
          recipient,
          data: payload.data,
          templateId: step.templateId
        }));
        await this.queue.enqueueBatch(jobs); // needs a batch method on QueueAdapter, not N calls to enqueue()
      }
    }
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}