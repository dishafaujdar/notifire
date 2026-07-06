import { describe, expect, it, vi } from 'vitest';
import { PostgresLeaseReaper } from '../src/queue/PostgresLeaseReaper.js';

describe('PostgresLeaseReaper', () => {
  it('runs reclamation with the configured lease timeout', async () => {
    const reclaimExpired = vi.fn(async () => 2);
    const reaper = new PostgresLeaseReaper(
      { reclaimExpired } as never,
      { leaseTimeoutMs: 30_000 }
    );

    await expect(reaper.runOnce()).resolves.toBe(2);
    expect(reclaimExpired).toHaveBeenCalledWith(30_000);
  });

  it('reports errors without rejecting the timer loop', async () => {
    const error = new Error('database unavailable');
    const onError = vi.fn();
    const reaper = new PostgresLeaseReaper(
      { reclaimExpired: vi.fn(async () => { throw error; }) } as never,
      { onError }
    );

    await expect(reaper.runOnce()).resolves.toBe(0);
    expect(onError).toHaveBeenCalledWith(error);
  });
});
