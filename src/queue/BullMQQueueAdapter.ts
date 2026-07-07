import { Queue, QueueEvents, UnrecoverableError, Worker } from 'bullmq';
import type { ConnectionOptions, Job, MinimalJob } from 'bullmq';
import type { JobResult, NotificationJob } from '../types.js';
import type { QueueAdapter } from './QueueAdapter.js';

type DeadLetterReason = 'attempts_exhausted' | 'stalled_exceeded';

interface BullMQQueueAdapterOptions {
  redisUrl: string;
  concurrency?: number;
  maxAttempts?: number;
  retryDelayMs?: number;
  onError?: (error: unknown) => void;
  onDeadLetter?: (job: NotificationJob, reason: DeadLetterReason, error: string) => void;
}

const QUEUE_NAME = 'notifire_jobs';
const LOCK_DURATION_MS = 30_000;

export class BullMQQueueAdapter implements QueueAdapter {
  private readonly queue: Queue<NotificationJob>;
  private readonly queueEvents: QueueEvents;
  private readonly connection: ConnectionOptions;
  private readonly concurrency: number;
  private readonly maxAttempts: number;
  private readonly retryDelayMs: number;
  private worker?: Worker<NotificationJob, void, string>;

  constructor(private readonly options: BullMQQueueAdapterOptions) {
    this.concurrency = options.concurrency ?? 1;
    this.maxAttempts = options.maxAttempts ?? 5;
    this.retryDelayMs = options.retryDelayMs ?? 1_000;

    if (!Number.isInteger(this.concurrency) || this.concurrency < 1) {
      throw new Error('BullMQ queue concurrency must be a positive integer.');
    }

    if (!Number.isInteger(this.maxAttempts) || this.maxAttempts < 1) {
      throw new Error('BullMQ maxAttempts must be a positive integer.');
    }

    this.connection = {
      url: options.redisUrl,
      maxRetriesPerRequest: null
    };

    const queueOptions = {
      connection: this.connection,
      defaultJobOptions: {
        attempts: this.maxAttempts,
        backoff: { type: 'notifire-full-jitter', delay: this.retryDelayMs },
        // Keep Redis memory bounded for successful jobs; failed jobs are retained
        // until the dead-letter hook has made them visible outside Redis.
        removeOnComplete: { count: 1_000 },
        removeOnFail: false as never
      }
    };

    this.queue = new Queue<NotificationJob>(QUEUE_NAME, queueOptions);
    this.queueEvents = new QueueEvents(QUEUE_NAME, {
      connection: this.connection
    });
    this.queue.on('error', (error) => this.options.onError?.(error));
    this.queueEvents.on('error', (error) => this.options.onError?.(error));
  }

  async enqueue(job: NotificationJob): Promise<void> {
    // BullMQ dedupes at enqueue time by jobId. Postgres gets equivalent
    // protection from the notifire_jobs primary key, but at the SQL insert layer.
    await this.queue.add(job.trigger, job, {
      jobId: job.id
    });
  }

  consume(handler: (job: NotificationJob) => Promise<JobResult>): void {
    if (this.worker) {
      throw new Error('BullMQ queue already has a consumer.');
    }

    this.worker = new Worker<NotificationJob, void, string>(
      QUEUE_NAME,
      async (job) => {
        const result = await handler(job.data);
        if (result.ok) {
          return;
        }

        if (!result.retry) {
          throw new UnrecoverableError(result.error);
        }

        throw new Error(result.error);
      },
      {
        connection: this.connection,
        concurrency: this.concurrency,
        // 30s must exceed p99 provider.send() latency. If real handlers exceed
        // this, BullMQ can mark healthy work as stalled and run it again.
        lockDuration: LOCK_DURATION_MS,
        maxStalledCount: 1,
        settings: {
          backoffStrategy: fullJitterBackoff
        },
        // Keep Redis memory bounded for successful jobs; failed jobs are retained
        // until the dead-letter hook has made them visible outside Redis.
        removeOnComplete: { count: 1_000 },
        removeOnFail: false as never
      }
    );

    this.worker.on('error', (error) => this.options.onError?.(error));
    this.worker.on('failed', (job, error, previousStatus) => {
      if (!job) {
        this.options.onError?.(error);
        return;
      }

      if (!isDeadLetter(job, previousStatus)) {
        return;
      }

      const reason: DeadLetterReason =
        previousStatus === 'active' && job.attemptsMade < (job.opts.attempts ?? this.maxAttempts)
          ? 'stalled_exceeded'
          : 'attempts_exhausted';

      this.options.onDeadLetter?.(job.data, reason, error.message);
    });
  }

  async stop(): Promise<void> {
    await this.worker?.close();
    this.worker = undefined;
    await this.queueEvents.close();
    await this.queue.close();
  }
}

function fullJitterBackoff(attemptsMade: number, _type?: string, _error?: Error, job?: MinimalJob): number {
  const baseDelay = Number(job?.opts.backoff && typeof job.opts.backoff === 'object' ? job.opts.backoff.delay : 1_000);
  const cappedDelay = Math.min(30_000, baseDelay * 2 ** Math.max(0, attemptsMade - 1));
  return Math.floor(Math.random() * cappedDelay);
}

function isDeadLetter(job: Job<NotificationJob>, previousStatus: string): boolean {
  const attempts = job.opts.attempts ?? 1;
  return job.attemptsMade >= attempts || previousStatus === 'active';
}
