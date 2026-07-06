import { randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { join, normalize } from 'node:path';
import Handlebars from 'handlebars';
import { InMemoryQueueAdapter } from './queue/InMemoryQueueAdapter.js';
import type { QueueAdapter } from './queue/QueueAdapter.js';
import type { ChannelProvider } from './providers/ChannelProvider.js';
import type { EmailMessage, JobResult, NotificationJob, TriggerPayload, Workflow } from './types.js';

interface NotifyreConfig {
  queue?: QueueAdapter;
  provider: {
    email: ChannelProvider<EmailMessage>;
    // Phase 2: SMS and push providers will be registered alongside email here.
  };
  templatesDir: string;
}

interface ParsedTemplate {
  subjectTemplate: string;
  htmlTemplate: string;
}

export class Notifyre {
  private readonly queue: QueueAdapter;
  private readonly provider: NotifyreConfig['provider'];
  private readonly templatesDir: string;
  private readonly workflows = new Map<string, Workflow>();
  private started = false;

  constructor(config: NotifyreConfig) {
    this.queue = config.queue ?? new InMemoryQueueAdapter();
    this.provider = config.provider;
    this.templatesDir = config.templatesDir;
  }

  defineWorkflow(workflow: Workflow): void {
    this.workflows.set(workflow.trigger, workflow);
  }

  start(): void {
    if (this.started) {
      return;
    }

    this.started = true;
    this.queue.consume((job) => this.processJob(job));
  }

  async trigger(triggerName: string, payload: TriggerPayload): Promise<void> {
    const workflow = this.workflows.get(triggerName);
    if (!workflow) {
      throw new Error(`Workflow "${triggerName}" is not registered.`);
    }

    if (!isRecord(payload?.data)) {
      throw new Error('Notification payload data must be an object.');
    }

    // Phase 2: chunked batch insert logic for fan-out to multiple recipients will go here.
    for (const step of workflow.steps) {
      const job: NotificationJob = {
        id: randomUUID(),
        trigger: workflow.trigger,
        channel: step.channel,
        recipient: payload.recipient,
        data: payload.data,
        templateId: step.templateId
      };

      await this.queue.enqueue(job);
    }
  }

  private async processJob(job: NotificationJob): Promise<JobResult> {
    try {
      const template = await this.loadTemplate(job.templateId);
      const subject = Handlebars.compile(template.subjectTemplate)(job.data);
      const html = Handlebars.compile(template.htmlTemplate)(job.data);

      // Phase 2: full-jitter retry will wrap provider.send().
      const result = await this.provider.email.send({
        to: job.recipient.email,
        subject,
        html
      });

      // Phase 2: an idempotency/delivery-log insert using ON CONFLICT DO NOTHING will happen here.
      console.log(JSON.stringify({
        event: 'notifyre.delivery',
        jobId: job.id,
        trigger: job.trigger,
        templateId: job.templateId,
        provider: this.provider.email.name,
        status: result.ok ? 'sent' : 'failed',
        retry: result.ok ? undefined : result.retry,
        error: result.ok ? undefined : result.error
      }));

      return result;
    } catch (error) {
      return {
        ok: false,
        retry: false,
        error: error instanceof Error ? error.message : 'Unknown error'
      };
    }
  }

  private async loadTemplate(templateId: string): Promise<ParsedTemplate> {
    const filePath = join(this.templatesDir, normalize(templateId));
    const source = await readFile(filePath, 'utf8');
    return parseTemplate(source, templateId);
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function parseTemplate(source: string, templateId: string): ParsedTemplate {
  // Templates keep subject support in frontmatter so each email stays in a single .hbs file.
  const match = source.match(/^---\n([\s\S]*?)\n---\n([\s\S]*)$/);
  if (!match) {
    throw new Error(`Template "${templateId}" must start with frontmatter containing a subject.`);
  }

  const subjectLine = match[1]
    .split('\n')
    .map((line) => line.trim())
    .find((line) => line.startsWith('subject:'));

  if (!subjectLine) {
    throw new Error(`Template "${templateId}" is missing a subject frontmatter field.`);
  }

  return {
    subjectTemplate: subjectLine.slice('subject:'.length).trim().replace(/^['"]|['"]$/g, ''),
    htmlTemplate: match[2]
  };
}
