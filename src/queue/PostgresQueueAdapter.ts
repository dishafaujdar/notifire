import { randomUUID } from 'node:crypto';
import type { Pool, PoolClient } from 'pg';
import type { JobResult, NotificationJob } from '../types.js';
import { PostgresJobStore } from './PostgresJobStore.js';
import { PostgresLeaseReaper } from './PostgresLeaseReaper.js';
import type { QueueAdapter } from './QueueAdapter.js';

interface PostgresQueueAdapterOptions {
  appPool: Pool;
  workerPool: Pool;
  concurrency?: number;
  pollIntervalMs?: number;
  retryDelayMs?: number;
  leaseTimeoutMs?: number;
  reaperIntervalMs?: number;
  onError?: (error: unknown) => void;
}

export class PostgresQueueAdapter implements QueueAdapter {
  private readonly enqueueStore: PostgresJobStore;
  private readonly workerStore: PostgresJobStore;
  private readonly workerId = randomUUID();
  private readonly concurrency: number;
  private readonly pollIntervalMs: number;
  private readonly retryDelayMs: number;
  private readonly reaper: PostgresLeaseReaper;
  private readonly waiters = new Set<() => void>();
  private handler?: (job: NotificationJob) => Promise<JobResult>;
  private listener?: PoolClient;
  private workerTasks: Promise<void>[] = [];
  private stopped = false;

  constructor(private readonly options: PostgresQueueAdapterOptions) {
    this.concurrency = options.concurrency ?? 1;
    this.pollIntervalMs = options.pollIntervalMs ?? 2_000;
    this.retryDelayMs = options.retryDelayMs ?? 1_000;

    if (!Number.isInteger(this.concurrency) || this.concurrency < 1) {
      throw new Error('Postgres queue concurrency must be a positive integer.');
    }

    const workerPoolMax = options.workerPool.options.max ?? 10;
    if (workerPoolMax < this.concurrency + 1) {
      throw new Error(`Worker pool max must be at least concurrency + 1 (${this.concurrency + 1}).`);
    }

    this.enqueueStore = new PostgresJobStore(options.appPool);
    this.workerStore = new PostgresJobStore(options.workerPool);
    this.reaper = new PostgresLeaseReaper(this.workerStore, {
      intervalMs: options.reaperIntervalMs,
      leaseTimeoutMs: options.leaseTimeoutMs,
      onError: options.onError
    });
  }

  async enqueue(job: NotificationJob): Promise<void> {
    await this.enqueueStore.enqueue(job);
  }

  consume(handler: (job: NotificationJob) => Promise<JobResult>): void {
    if (this.handler) {
      throw new Error('Postgres queue already has a consumer.');
    }

    this.handler = handler;
    this.reaper.start();
    this.workerTasks = Array.from({ length: this.concurrency }, (_, index) =>
      this.workerLoop(`${this.workerId}:${index}`)
    );
    void this.startListener();
  }

  async stop(): Promise<void> {
    if (this.stopped) {
      return;
    }

    this.stopped = true;
    this.reaper.stop();
    this.wakeWorkers();

    if (this.listener) {
      this.listener.removeAllListeners('notification');
      try {
        await this.listener.query('UNLISTEN notifire_jobs');
      } finally {
        this.listener.release();
        this.listener = undefined;
      }
    }

    await Promise.all(this.workerTasks);
    this.workerTasks = [];
  }

  private async startListener(): Promise<void> {
    try {
      const listener = await this.options.workerPool.connect();
      if (this.stopped) {
        listener.release();
        return;
      }

      this.listener = listener;
      listener.on('notification', (message) => {
        if (message.channel === 'notifire_jobs') {
          this.wakeWorkers();
        }
      });
      await listener.query('LISTEN notifire_jobs');
      this.wakeWorkers();
    } catch (error) {
      this.options.onError?.(error);
    }
  }

  private async workerLoop(workerId: string): Promise<void> {
    while (!this.stopped) {
      try {
        const job = await this.workerStore.claim(workerId);
        if (!job) {
          await this.waitForWork();
          continue;
        }

        const result = await this.runHandler(job);
        await this.workerStore.settle(job.id, workerId, result, this.retryDelayMs);
      } catch (error) {
        this.options.onError?.(error);
        await this.waitForWork();
      }
    }
  }

  private async runHandler(job: NotificationJob): Promise<JobResult> {
    try {
      return await this.handler!(job);
    } catch (error) {
      return {
        ok: false,
        retry: true,
        error: error instanceof Error ? error.message : String(error)
      };
    }
  }

  private waitForWork(): Promise<void> {
    if (this.stopped) {
      return Promise.resolve();
    }

    return new Promise((resolve) => {
      const wake = () => {
        clearTimeout(timer);
        this.waiters.delete(wake);
        resolve();
      };
      const timer = setTimeout(wake, this.pollIntervalMs);
      this.waiters.add(wake);
    });
  }

  private wakeWorkers(): void {
    for (const wake of [...this.waiters]) {
      wake();
    }
  }
}
