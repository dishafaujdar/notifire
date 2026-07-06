import { describe, expect, it, vi } from 'vitest';
import { PostgresJobStore } from '../src/queue/PostgresJobStore.js';
import type { NotificationJob } from '../src/types.js';

const job: NotificationJob = {
  id: '2b74e1e8-692d-4897-b5ff-25f96e44586f',
  trigger: 'otp.requested',
  channel: 'email',
  recipient: { email: 'person@example.com' },
  data: { code: '123456' },
  templateId: 'otp-email.hbs'
};

describe('PostgresJobStore', () => {
  it('inserts a job with queue metadata', async () => {
    const query = vi.fn(async (_sql: string, _values?: unknown[]) => ({ rows: [] }));
    const store = new PostgresJobStore({ query } as never);

    await store.enqueue(job, { priority: 10, scheduledAt: new Date('2026-07-07T00:00:00Z') });

    expect(query).toHaveBeenCalledOnce();
    expect(query.mock.calls[0]?.[0]).toContain('INSERT INTO notifyre_jobs');
    expect(query.mock.calls[0]?.[0]).toContain("pg_notify('notifyre_jobs'");
    expect(query.mock.calls[0]?.[1]).toEqual([
      job.id,
      JSON.stringify(job),
      'email',
      10,
      new Date('2026-07-07T00:00:00Z')
    ]);
  });

  it('claims one available job with SKIP LOCKED', async () => {
    const query = vi.fn(async (_sql: string, _values?: unknown[]) => ({ rows: [{ payload: job }] }));
    const store = new PostgresJobStore({ query } as never);

    await expect(store.claim('worker-1')).resolves.toEqual(job);

    expect(query.mock.calls[0]?.[0]).toContain('FOR UPDATE SKIP LOCKED');
    expect(query.mock.calls[0]?.[1]).toEqual(['worker-1']);
  });

  it('returns undefined when no job is available', async () => {
    const query = vi.fn(async (_sql: string, _values?: unknown[]) => ({ rows: [] }));
    const store = new PostgresJobStore({ query } as never);

    await expect(store.claim('worker-1')).resolves.toBeUndefined();
  });

  it.each([
    [{ ok: true as const }, "status = 'completed'", [job.id, 'worker-1']],
    [{ ok: false as const, retry: true, error: 'Temporary' }, "status = 'pending'", [job.id, 'worker-1', 5_000, 'Temporary']],
    [{ ok: false as const, retry: false, error: 'Permanent' }, "status = 'failed'", [job.id, 'worker-1', 'Permanent']]
  ])('settles a claimed job from its result', async (result, expectedStatus, expectedValues) => {
    const query = vi.fn(async (_sql: string, _values?: unknown[]) => ({ rows: [{ id: job.id }] }));
    const store = new PostgresJobStore({ query } as never);

    await expect(store.settle(job.id, 'worker-1', result, 5_000)).resolves.toBe(true);

    expect(query.mock.calls[0]?.[0]).toContain(expectedStatus);
    expect(query.mock.calls[0]?.[0]).toContain('claimed_by = $2');
    expect(query.mock.calls[0]?.[1]).toEqual(expectedValues);
  });

  it('does not settle a job no longer owned by the worker', async () => {
    const query = vi.fn(async (_sql: string, _values?: unknown[]) => ({ rows: [] }));
    const store = new PostgresJobStore({ query } as never);

    await expect(store.settle(job.id, 'stale-worker', { ok: true })).resolves.toBe(false);
  });

  it('reclaims expired leases and returns the reclaimed count', async () => {
    const query = vi.fn(async (_sql: string, _values?: unknown[]) => ({
      rows: [{ id: job.id }, { id: 'f0b80daa-8199-41c4-a10d-06da70a85808' }]
    }));
    const store = new PostgresJobStore({ query } as never);

    await expect(store.reclaimExpired(30_000)).resolves.toBe(2);

    expect(query.mock.calls[0]?.[0]).toContain("claimed_at < now()");
    expect(query.mock.calls[0]?.[1]).toEqual([30_000]);
  });
});
