import { EventEmitter } from 'node:events';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { BullMQQueueAdapter } from '../src/queue/BullMQQueueAdapter.js';
import type { NotificationJob } from '../src/types.js';

const job: NotificationJob = {
  id: '2b74e1e8-692d-4897-b5ff-25f96e44586f',
  trigger: 'otp.requested',
  channel: 'email',
  recipient: { email: 'person@example.com' },
  data: { code: '123456' },
  templateId: 'otp-email.hbs'
};

const queueAdd = vi.fn();
const queueClose = vi.fn();
const queueEventsClose = vi.fn();
const workerClose = vi.fn();
let workerInstance: EventEmitter & { close: typeof workerClose };
let workerProcessor: ((bullJob: { data: NotificationJob }) => Promise<void>) | undefined;

vi.mock('bullmq', async () => {
  class UnrecoverableError extends Error {
    constructor(message: string) {
      super(message);
      this.name = 'UnrecoverableError';
    }
  }

  return {
    Queue: vi.fn().mockImplementation(() => Object.assign(new EventEmitter(), {
      add: queueAdd,
      close: queueClose
    })),
    QueueEvents: vi.fn().mockImplementation(() => Object.assign(new EventEmitter(), {
      close: queueEventsClose
    })),
    Worker: vi.fn().mockImplementation((_name, processor) => {
      workerProcessor = processor;
      workerInstance = Object.assign(new EventEmitter(), { close: workerClose });
      return workerInstance;
    }),
    UnrecoverableError
  };
});

describe('BullMQQueueAdapter', () => {
  beforeEach(() => {
    queueAdd.mockResolvedValue(undefined);
    queueClose.mockResolvedValue(undefined);
    queueEventsClose.mockResolvedValue(undefined);
    workerClose.mockResolvedValue(undefined);
    workerProcessor = undefined;
    vi.clearAllMocks();
  });

  it('enqueues with job.id as BullMQ jobId for enqueue-time deduplication', async () => {
    const adapter = new BullMQQueueAdapter({ redisUrl: 'redis://localhost:6379' });

    await adapter.enqueue(job);
    await adapter.stop();

    expect(queueAdd).toHaveBeenCalledWith(job.trigger, job, { jobId: job.id });
  });

  it('maps retryable and non-retryable JobResult values to BullMQ failure semantics', async () => {
    const retrying = new BullMQQueueAdapter({ redisUrl: 'redis://localhost:6379' });
    retrying.consume(async () => ({ ok: false, retry: true, error: 'Temporary' }));
    await expect(workerProcessor?.({ data: job })).rejects.toThrow('Temporary');
    await retrying.stop();

    const permanent = new BullMQQueueAdapter({ redisUrl: 'redis://localhost:6379' });
    permanent.consume(async () => ({ ok: false, retry: false, error: 'Permanent' }));
    await expect(workerProcessor?.({ data: job })).rejects.toMatchObject({ name: 'UnrecoverableError' });
    await permanent.stop();
  });

  it('reports dead letters with a reason', async () => {
    const onDeadLetter = vi.fn();
    const adapter = new BullMQQueueAdapter({
      redisUrl: 'redis://localhost:6379',
      maxAttempts: 3,
      onDeadLetter
    });

    adapter.consume(async () => ({ ok: true }));
    workerInstance.emit('failed', {
      data: job,
      attemptsMade: 3,
      opts: { attempts: 3 }
    }, new Error('exhausted'), 'delayed');
    await adapter.stop();

    expect(onDeadLetter).toHaveBeenCalledWith(job, 'attempts_exhausted', 'exhausted');
  });
});
