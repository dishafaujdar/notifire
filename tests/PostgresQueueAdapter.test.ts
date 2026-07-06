import { EventEmitter } from 'node:events';
import { describe, expect, it, vi } from 'vitest';
import { PostgresQueueAdapter } from '../src/queue/PostgresQueueAdapter.js';
import type { NotificationJob } from '../src/types.js';

const job: NotificationJob = {
  id: '2b74e1e8-692d-4897-b5ff-25f96e44586f',
  trigger: 'otp.requested',
  channel: 'email',
  recipient: { email: 'person@example.com' },
  data: { code: '123456' },
  templateId: 'otp-email.hbs'
};

describe('PostgresQueueAdapter', () => {
  it('requires worker capacity for workers and the listener', () => {
    expect(() => new PostgresQueueAdapter({
      appPool: poolMock(1) as never,
      workerPool: poolMock(2) as never,
      concurrency: 2
    })).toThrow('Worker pool max must be at least concurrency + 1 (3).');
  });

  it('claims, handles and settles jobs across concurrent workers', async () => {
    let claims = 0;
    const listener = listenerMock();
    const workerPool = poolMock(3, async (sql: string) => {
      if (sql.includes('WITH candidate')) {
        claims += 1;
        return { rows: claims === 1 ? [{ payload: job }] : [] };
      }
      if (sql.includes("status = 'completed'")) {
        return { rows: [{ id: job.id }] };
      }
      return { rows: [] };
    }, listener);
    const handler = vi.fn(async () => ({ ok: true as const }));
    const adapter = new PostgresQueueAdapter({
      appPool: poolMock(1) as never,
      workerPool: workerPool as never,
      concurrency: 2,
      pollIntervalMs: 10_000
    });

    adapter.consume(handler);
    await waitFor(() => expect(handler).toHaveBeenCalledWith(job));
    await adapter.stop();

    expect(listener.query).toHaveBeenCalledWith('LISTEN notifire_jobs');
    expect(listener.query).toHaveBeenCalledWith('UNLISTEN notifire_jobs');
    expect(listener.release).toHaveBeenCalledOnce();
  });
});

function poolMock(
  max: number,
  queryImplementation: (sql: string, values?: unknown[]) => Promise<{ rows: unknown[] }> = async () => ({ rows: [] }),
  listener = listenerMock()
) {
  return {
    options: { max },
    query: vi.fn(queryImplementation),
    connect: vi.fn(async () => listener)
  };
}

function listenerMock() {
  const emitter = new EventEmitter();
  return Object.assign(emitter, {
    query: vi.fn(async () => ({ rows: [] })),
    release: vi.fn()
  });
}

async function waitFor(assertion: () => void): Promise<void> {
  const deadline = Date.now() + 500;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      assertion();
      return;
    } catch (error) {
      lastError = error;
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
  }
  throw lastError;
}
