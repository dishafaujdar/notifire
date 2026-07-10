import type { QueueAdapter } from '../queue/QueueAdapter.js';
import type { ChannelHandler, JobResult, NotificationJob } from '../types.js';

export class Worker {
  private readonly handlers = new Map<string, ChannelHandler>();

  constructor(private readonly queue: QueueAdapter, handlers: ChannelHandler[]) {
    for (const handler of handlers) {
      this.handlers.set(handler.channel, handler);
    }
  }

  start(): void {
    this.queue.consume((job) => this.dispatch(job));
  }

  private async dispatch(job: NotificationJob): Promise<JobResult> {
    const handler = this.handlers.get(job.channel);
    if (!handler) {
      return { ok: false, retry: false, error: `No handler registered for channel "${job.channel}"` };
    }

    try {
      return await handler.process(job);
    } catch (error) {
      return { ok: false, retry: true, error: error instanceof Error ? error.message : 'Unknown error' };
    }
  }
}