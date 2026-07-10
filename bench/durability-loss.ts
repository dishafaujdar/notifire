
import 'dotenv/config';
import { randomUUID } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import { dirname } from 'node:path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { Pool, type PoolClient } from 'pg';
import { Queue } from 'bullmq';
import { PostgresJobStore } from '../src/queue/PostgresJobStore.js';
import type { NotificationJob } from '../src/index.js';

const execFileAsync = promisify(execFile);
const jobCount = Number(process.env.DURABILITY_JOB_COUNT ?? 1_000);
const postgresUrl = process.env.TEST_POSTGRES_URL;
const redisUrl = process.env.TEST_REDIS_URL;
const postgresContainer = process.env.POSTGRES_CONTAINER;
const redisContainer = process.env.REDIS_CONTAINER;
const outputPath = 'bench/results/durability-loss.md';

interface DurabilityResult {
  backend: string;
  mode: string;
  enqueued: number;
  recoverable: number;
  lost: number;
}

async function main(): Promise<void> {
  if (!postgresUrl || !redisUrl || !postgresContainer || !redisContainer) {
    throw new Error('Set TEST_POSTGRES_URL, TEST_REDIS_URL, POSTGRES_CONTAINER, and REDIS_CONTAINER.');
  }

  const results: DurabilityResult[] = [];
  results.push(await runPostgres('synchronous_commit=on', 'on'));
  results.push(await runPostgres('synchronous_commit=off', 'off'));
  // FIX (bug 4): these two Redis modes now actually reconfigure the server via
  // redis-cli CONFIG SET before the run, instead of just relabeling the same config.
  results.push(await runRedis('appendfsync=everysec (default)', ['appendfsync', 'everysec']));
  results.push(await runRedis('appendfsync=always', ['appendfsync', 'always']));
  await writeMarkdown(results);
}

async function runPostgres(mode: string, synchronousCommit: 'on' | 'off'): Promise<DurabilityResult> {
  const pool = new Pool({ connectionString: postgresUrl, max: 4 });
  let enqueued = 0;

  // FIX (bug 1): pin ONE connection for the entire run instead of using pool.query()
  // for every insert. SET is session-scoped — if enqueue() draws a random connection
  // from the pool each time, most inserts never see the SET at all.
  let client = await pool.connect();
  await client.query(`SET synchronous_commit = ${synchronousCommit}`);
  // FIX (bug 3): clear leftover rows from a prior mode's run before this one starts.
  await client.query('TRUNCATE notifire_jobs');
  let store = new PostgresJobStore(client);

  try {
    for (; enqueued < jobCount; enqueued += 1) {
      await store.enqueue(makeJob(`postgres-${mode}-${enqueued}`)); // label passed for readability, id is a real UUID inside makeJob()

      if (enqueued === Math.floor(jobCount / 2)) {
        client.release(true); // discard — this connection is about to die anyway
        await killAndRestart(postgresContainer!);
        // FIX (bug 2): wait for Postgres to actually accept connections again
        // before resuming the loop, instead of immediately throwing into the catch.
        await waitForPostgresReady(postgresUrl!);

        client = await pool.connect(); // fresh connection post-restart
        await client.query(`SET synchronous_commit = ${synchronousCommit}`); // re-apply — session setting doesn't survive a new connection
        store = new PostgresJobStore(client);
      }
    }
  } catch (error) {
    // No second restart here — the mid-loop kill above IS the intended crash event.
    // Anything landing here is a genuinely unexpected failure, worth seeing as-is.
    console.error(`[postgres:${mode}] unexpected error at enqueued=${enqueued}:`, error);
  } finally {
    client.release(true);
    await pool.end().catch(() => undefined);
  }

  const recoverablePool = new Pool({ connectionString: postgresUrl, max: 2 });
  const recoverable = Number(
    (await recoverablePool.query('SELECT count(*)::int AS count FROM notifire_jobs')).rows[0]?.count ?? 0
  );
  await recoverablePool.end();
  return { backend: 'Postgres', mode, enqueued, recoverable, lost: Math.max(0, enqueued - recoverable) };
}

async function runRedis(mode: string, config?: [string, string]): Promise<DurabilityResult> {
  if (config) {
    await execFileAsync('docker', ['exec', redisContainer!, 'redis-cli', 'CONFIG', 'SET', ...config]);
  }

  let queue = new Queue<NotificationJob>('notifire_jobs', {
    connection: { url: redisUrl, maxRetriesPerRequest: null }
  });
  // FIX (bug 3): clear leftover jobs from a prior mode's run before this one starts.
  await queue.obliterate({ force: true }).catch(() => undefined);

  let enqueued = 0;
  try {
    for (; enqueued < jobCount; enqueued += 1) {
      await queue.add('otp.requested', makeJob(`redis-${mode}-${enqueued}`), {
        jobId: `redis-${mode}-${enqueued}`
      });

      if (enqueued === Math.floor(jobCount / 2)) {
        await queue.close();
        await killAndRestart(redisContainer!);
        // FIX (bug 2): wait for Redis to actually accept connections again.
        await waitForRedisReady(redisUrl!);
        queue = new Queue<NotificationJob>('notifire_jobs', {
          connection: { url: redisUrl, maxRetriesPerRequest: null }
        });
      }
    }
  } catch (error) {
    console.error(`[redis:${mode}] unexpected error at enqueued=${enqueued}:`, error);
  } finally {
    await queue.close().catch(() => undefined);
  }

  const recoverableQueue = new Queue<NotificationJob>('notifire_jobs', {
    connection: { url: redisUrl, maxRetriesPerRequest: null }
  });
  const counts = await recoverableQueue.getJobCounts('waiting', 'delayed', 'active', 'failed', 'completed');
  await recoverableQueue.close();
  const recoverable = Object.values(counts).reduce((sum, value) => sum + value, 0);
  return { backend: 'BullMQ/Redis', mode, enqueued, recoverable, lost: Math.max(0, enqueued - recoverable) };
}

async function killAndRestart(container: string): Promise<void> {
  await execFileAsync('docker', ['kill', '-s', 'KILL', container]).catch(() => undefined);
  await execFileAsync('docker', ['start', container]);
}

// FIX (bug 2): poll until a real query succeeds, instead of assuming `docker start`
// resolving means the database is ready to accept connections.
async function waitForPostgresReady(url: string, timeoutMs = 15_000): Promise<void> {
  const start = Date.now();
  let lastError: unknown;
  while (Date.now() - start < timeoutMs) {
    const probe = new Pool({ connectionString: url, max: 1 });
    try {
      await probe.query('SELECT 1');
      await probe.end();
      return;
    } catch (error) {
      lastError = error;
      await probe.end().catch(() => undefined);
      await sleep(300);
    }
  }
  throw new Error(`Postgres did not become ready within ${timeoutMs}ms: ${String(lastError)}`);
}

async function waitForRedisReady(url: string, timeoutMs = 15_000): Promise<void> {
  const start = Date.now();
  let lastError: unknown;
  while (Date.now() - start < timeoutMs) {
    const probe = new Queue('healthcheck', { connection: { url, maxRetriesPerRequest: null } });
    try {
      await probe.waitUntilReady();
      await probe.close();
      return;
    } catch (error) {
      lastError = error;
      await probe.close().catch(() => undefined);
      await sleep(300);
    }
  }
  throw new Error(`Redis did not become ready within ${timeoutMs}ms: ${String(lastError)}`);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function writeMarkdown(results: DurabilityResult[]): Promise<void> {
  const lines = [
    '# Durability Loss',
    '',
    '| Backend | Mode | Enqueued before failure | Recoverable after restart | Lost |',
    '|---|---|---:|---:|---:|',
    ...results.map((row) => `| ${row.backend} | ${row.mode} | ${row.enqueued} | ${row.recoverable} | ${row.lost} |`)
  ];
  await mkdir(dirname(outputPath), { recursive: true });
  await writeFile(outputPath, `${lines.join('\n')}\n`);
}

function makeJob(label: string): NotificationJob {
  return {
    id: randomUUID(), // FIX: schema requires a real uuid; the descriptive label goes in `data` instead
    trigger: 'otp.requested',
    channel: 'email',
    recipient: { email: 'person@example.com' },
    data: { code: '123456', label },
    templateId: 'otp-email.hbs'
  };
}

void main();