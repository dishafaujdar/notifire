import { readFile } from 'node:fs/promises';
import { join, normalize } from 'node:path';
import Handlebars from 'handlebars';
import type { ChannelHandler, InAppMessage, JobResult, NotificationJob } from '../types.js';
import type { ChannelProvider } from '../providers/ChannelProvider.js';

interface CompiledInAppTemplate {
  title: Handlebars.TemplateDelegate;
  body: Handlebars.TemplateDelegate;
}

export class InAppHandler implements ChannelHandler {
  readonly channel = 'in_app' as const;
  private readonly cache = new Map<string, CompiledInAppTemplate>();

  constructor(
    private readonly provider: ChannelProvider<InAppMessage>,
    private readonly templatesDir: string
  ) {}

  async process(job: NotificationJob): Promise<JobResult> {
    let template: CompiledInAppTemplate;
    try {
      template = await this.getCompiledTemplate(job.templateId);
    } catch (error) {
      // template/config errors are permanent — same classification as EmailHandler
      return { ok: false, retry: false, error: errMsg(error) };
    }

    try {
      const title = template.title(job.data);
      const body = template.body(job.data);
      return await this.provider.send({
        recipientId: job.recipient.recipientId!,
        title,
        body,
        data: job.data
      });
    } catch (error) {
      // provider errors (Supabase insert failing) are the transient case
      return { ok: false, retry: true, error: errMsg(error) };
    }
  }

  private async getCompiledTemplate(templateId: string): Promise<CompiledInAppTemplate> {
    const cached = this.cache.get(templateId);
    if (cached) return cached;

    const filePath = join(this.templatesDir, normalize(templateId));
    const source = await readFile(filePath, 'utf8');
    const match = source.match(/^---\n([\s\S]*?)\n---\n([\s\S]*)$/);
    if (!match) throw new Error(`Template "${templateId}" must start with frontmatter containing a title.`);

    const titleLine = match[1].split('\n').map(l => l.trim()).find(l => l.startsWith('title:'));
    if (!titleLine) throw new Error(`Template "${templateId}" is missing a title frontmatter field.`);

    const compiled: CompiledInAppTemplate = {
      title: Handlebars.compile(titleLine.slice('title:'.length).trim().replace(/^['"]|['"]$/g, '')),
      body: Handlebars.compile(match[2].trim())
    };
    this.cache.set(templateId, compiled);
    return compiled;
  }
}

function errMsg(error: unknown): string {
  return error instanceof Error ? error.message : 'Unknown error';
}