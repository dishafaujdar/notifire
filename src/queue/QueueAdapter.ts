import type { JobResult, NotificationJob } from '../types.js';

export interface QueueAdapter {
  enqueue(job: NotificationJob): Promise<void>;
  enqueueBatch(jobs: NotificationJob[]): Promise<void>;
  consume(handler: (job: NotificationJob) => Promise<JobResult>): void;
  stop(): Promise<void>;
}