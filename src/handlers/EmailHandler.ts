import { readFile } from 'node:fs/promises';
import { join, normalize } from 'node:path';
import Handlebars from 'handlebars';
import type { ChannelProvider } from '../providers/ChannelProvider.js';
import type { ChannelHandler, EmailMessage, JobResult, NotificationJob } from '../types.js';

interface CompiledTemplate {
  subject: Handlebars.TemplateDelegate;
  html: Handlebars.TemplateDelegate;
}

interface ParsedTemplate {
    subjectTemplate: string;
    htmlTemplate: string;
}  

export class EmailHandler implements ChannelHandler {
    readonly channel = 'email' as const;
    private readonly templateCache = new Map<string, CompiledTemplate>();

    constructor(
        private readonly provider: ChannelProvider<EmailMessage>,
        private readonly templatesDir: string
      ) {}
    
    async process(job: NotificationJob): Promise<JobResult> {
        let template: CompiledTemplate;
        try {
            template = await this.getCompiledTemplate(job.templateId);
        } catch (error) {
            return { ok: false, retry: true, error: 'Failed to compile template' };
        }

        try {
        const subject = template.subject(job.data);
        const html = template.html(job.data);
        const result = await this.provider.send({
                to: job.recipient.email,
                subject,
                html });
        logDelivery(job, this.provider.name, result);
        return result
        } catch (error) {
            return { ok: false, retry: true, error: errMsg(error) };
        }
    }

    private async getCompiledTemplate(templateId: string): Promise<CompiledTemplate> {
        const cached = this.templateCache.get(templateId);
        if (cached) return cached;
    
        const filePath = join(this.templatesDir, normalize(templateId));
        const source = await readFile(filePath, 'utf8');
        const parsed = parseTemplate(source, templateId);
        const compiled: CompiledTemplate = {
          subject: Handlebars.compile(parsed.subjectTemplate),
          html: Handlebars.compile(parsed.htmlTemplate)
        };
        this.templateCache.set(templateId, compiled);
        return compiled;
      }
    }
    
    function errMsg(error: unknown): string {
      return error instanceof Error ? error.message : 'Unknown error';
    }

    function logDelivery(job: NotificationJob, providerName: string, result: JobResult) {
        console.log(JSON.stringify({
          event: 'notifire.delivery',
          jobId: job.id,
          trigger: job.trigger,
          templateId: job.templateId,
          provider: providerName,
          status: result.ok ? 'sent' : 'failed',
          retry: result.ok ? undefined : result.retry,
          error: result.ok ? undefined : result.error
        }))
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