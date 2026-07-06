import type { JobResult, NotificationJob } from '../types.js';
import type { QueueAdapter } from './QueueAdapter.js';

export class InMemoryQueueAdapter implements QueueAdapter {
  private readonly jobs: NotificationJob[] = [];
  private handler?: (job: NotificationJob) => Promise<JobResult>;
  private draining = false;
  private stopped = false;
  private inFlight?: Promise<void>;

  async enqueue(job: NotificationJob): Promise<void> {
    this.jobs.push(job);
    // Phase 2: a Postgres queue using FOR UPDATE SKIP LOCKED will replace this in-memory array.
    this.drain();
  }

  consume(handler: (job: NotificationJob) => Promise<JobResult>): void {
    this.handler = handler;
    this.drain();
  }

  async stop(): Promise<void> {
    this.stopped = true;
    await this.inFlight;
  }

  private drain(): void {
    if (!this.handler || this.draining || this.stopped) {
      return;
    }

    this.draining = true;

    while (this.jobs.length > 0) {
      const job = this.jobs.shift();
      if (!job) {
        continue;
      }

      this.inFlight = this.handler(job)
        .finally(() => {
          this.draining = false;
          this.inFlight = undefined;
          this.drain();
        })
        .then(() => undefined);
      return;
    }

    this.draining = false;
  }
}
