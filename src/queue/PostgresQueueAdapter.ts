import { randomUUID } from 'node:crypto';
import type { Pool, PoolClient } from '../../node_modules/@types/pg/index.js';
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
  maxAttempts?: number;
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
  private readonly maxAttempts: number;
  private readonly reaper: PostgresLeaseReaper;
  private readonly waiters = new Set<() => void>();
  private handler?: (job: NotificationJob) => Promise<JobResult>;
  private listener?: PoolClient;
  private listenerReconnectTask?: Promise<void>;
  private workerTasks: Promise<void>[] = [];
  private stopped = false;

  constructor(private readonly options: PostgresQueueAdapterOptions) {
    this.concurrency = options.concurrency ?? 1;
    this.pollIntervalMs = options.pollIntervalMs ?? 2_000;
    this.retryDelayMs = options.retryDelayMs ?? 1_000;
    this.maxAttempts = options.maxAttempts ?? 5;

    if (!Number.isInteger(this.concurrency) || this.concurrency < 1) {
      throw new Error('Postgres queue concurrency must be a positive integer.');
    }

    const workerPoolMax = options.workerPool.options.max ?? 10;
    if (workerPoolMax < this.concurrency + 2) {
      throw new Error(`Worker pool max must be at least concurrency + 2 (${this.concurrency + 2}).`);
    }

    this.enqueueStore = new PostgresJobStore(options.appPool);
    this.workerStore = new PostgresJobStore(options.workerPool);
    this.reaper = new PostgresLeaseReaper(this.workerStore, {
      intervalMs: options.reaperIntervalMs,
      leaseTimeoutMs: options.leaseTimeoutMs,
      maxAttempts: this.maxAttempts,
      onError: options.onError
    });
  }

  async enqueue(job: NotificationJob): Promise<void> {
    await this.enqueueStore.enqueue(job);
  }

  async enqueueBatch(jobs: NotificationJob[]): Promise<void> {
    await this.enqueueStore.enqueueBatch(jobs);
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
      this.listener.removeAllListeners('error');
      this.listener.removeAllListeners('end');
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
      if (this.stopped || this.listener) {
        return;
      }

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
      listener.on('error', (error) => {
        this.options.onError?.(error);
        this.dropListener(listener);
        if (!this.stopped) {
          void this.reconnectListener();
        }
      });
      listener.on('end', () => {
        this.dropListener(listener);
        if (!this.stopped) {
          void this.reconnectListener();
        }
      });
      await listener.query('LISTEN notifire_jobs');
      this.wakeWorkers();
    } catch (error) {
      this.options.onError?.(error);
      if (!this.stopped) {
        void this.reconnectListener();
      }
    }
  }

  private reconnectListener(attempt = 0): Promise<void> {
    this.listenerReconnectTask ??= this.reconnectListenerLoop(attempt).finally(() => {
      this.listenerReconnectTask = undefined;
    });
    return this.listenerReconnectTask;
  }

  private async reconnectListenerLoop(attempt: number): Promise<void> {
    const maxDelayMs = 30_000;
    let nextAttempt = attempt;

    while (!this.stopped && !this.listener) {
      const exponentialDelayMs = Math.min(maxDelayMs, 1_000 * 2 ** nextAttempt);
      const jitteredDelayMs = exponentialDelayMs * (0.5 + Math.random() * 0.5);
      await sleep(jitteredDelayMs);
      if (this.stopped || this.listener) {
        return;
      }

      await this.startListener();
      nextAttempt += 1;
    }
  }

  private dropListener(listener: PoolClient): void {
    if (this.listener !== listener) {
      return;
    }

    listener.removeAllListeners('notification');
    listener.removeAllListeners('error');
    listener.removeAllListeners('end');
    listener.release();
    this.listener = undefined;
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
        const delay = backoffMs(job.attempts, this.retryDelayMs);
        await this.workerStore.settle(job.id, workerId, result, delay, this.maxAttempts);
      } catch (error) {
        this.options.onError?.(error);
        await this.waitForWork();
      }
    }

    function backoffMs(attempts: number, baseMs: number, capMs = 30_000): number {
      const exponential = Math.min(capMs, baseMs * 2 ** attempts);
      return Math.random() * exponential;
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

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, ms);
    timer.unref?.();
  });
}
