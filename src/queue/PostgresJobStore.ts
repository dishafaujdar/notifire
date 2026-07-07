import type { Pool } from 'pg';
import type { JobResult, NotificationJob } from '../types.js';

type Queryable = Pick<Pool, 'query'>;

interface EnqueueOptions {
  priority?: number;
  scheduledAt?: Date;
}

interface JobRow {
  payload: NotificationJob;
}

interface CountRow {
  id: string;
}

export class PostgresJobStore {
  constructor(private readonly database: Queryable) {}

  async enqueue(job: NotificationJob, options: EnqueueOptions = {}): Promise<void> {
    await this.database.query(
      `WITH inserted AS (
         INSERT INTO notifire_jobs
           (id, payload, channel, priority, scheduled_at)
         VALUES ($1, $2::jsonb, $3, $4, $5)
         RETURNING id
       )
       SELECT pg_notify('notifire_jobs', id::text)
       FROM inserted`,
      [
        job.id,
        JSON.stringify(job),
        job.channel,
        options.priority ?? 0,
        options.scheduledAt ?? new Date()
      ]
    );
  }

  async claim(workerId: string): Promise<NotificationJob | undefined> {
    const result = await this.database.query<JobRow>(
      `WITH candidate AS (
         SELECT id
         FROM notifire_jobs
         WHERE status = 'pending'
           AND scheduled_at <= now()
         ORDER BY priority DESC, scheduled_at, created_at
         FOR UPDATE SKIP LOCKED
         LIMIT 1
       )
       UPDATE notifire_jobs AS job
       SET status = 'claimed',
           claimed_at = now(),
           claimed_by = $1,
           attempts = attempts + 1
       FROM candidate
       WHERE job.id = candidate.id
       RETURNING job.payload`,
      [workerId]
    );

    return result.rows[0]?.payload;
  }

  async settle(
    jobId: string,
    workerId: string,
    result: JobResult,
    retryDelayMs = 1_000,
    maxAttempts = 5
  ): Promise<boolean> {
    if (result.ok) {
      return this.updateClaim(
        `UPDATE notifire_jobs
         SET status = 'completed',
             completed_at = now(),
             claimed_at = NULL,
             claimed_by = NULL,
             last_error = NULL
         WHERE id = $1
           AND status = 'claimed'
           AND claimed_by = $2
         RETURNING id`,
        [jobId, workerId]
      );
    }

    if (result.retry) {
      return this.updateClaim(
        `UPDATE notifire_jobs
         SET status = CASE WHEN attempts >= $5 THEN 'dead_letter'::notifire_job_status ELSE 'pending'::notifire_job_status END,
             scheduled_at = now() + ($3 * interval '1 millisecond'),
             claimed_at = NULL,
             claimed_by = NULL,
             last_error = $4
         WHERE id = $1
           AND status = 'claimed'
           AND claimed_by = $2
         RETURNING id`,
        [jobId, workerId, retryDelayMs, result.error, maxAttempts]
      );
    }

    return this.updateClaim(
      `UPDATE notifire_jobs
       SET status = 'failed',
           claimed_at = NULL,
           claimed_by = NULL,
           last_error = $3
       WHERE id = $1
         AND status = 'claimed'
         AND claimed_by = $2
       RETURNING id`,
      [jobId, workerId, result.error]
    );
  }

  async reclaimExpired(leaseTimeoutMs = 30_000, maxAttempts = 5): Promise<number> {
    const result = await this.database.query<CountRow>(
      `UPDATE notifire_jobs
       SET attempts = attempts + 1,
           status = CASE WHEN attempts + 1 >= $2 THEN 'dead_letter'::notifire_job_status ELSE 'pending'::notifire_job_status END,
           scheduled_at = now(),
           claimed_at = NULL,
           claimed_by = NULL
       WHERE status = 'claimed'
         AND claimed_at < now() - ($1 * interval '1 millisecond')
       RETURNING id`,
      [leaseTimeoutMs, maxAttempts]
    );

    return result.rows.length;
  }

  private async updateClaim(sql: string, values: unknown[]): Promise<boolean> {
    const result = await this.database.query<CountRow>(sql, values);
    return result.rows.length === 1;
  }
}
