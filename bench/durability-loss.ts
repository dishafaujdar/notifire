import { mkdir, writeFile } from 'node:fs/promises';
import { dirname } from 'node:path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { Pool } from 'pg';
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
  results.push(await runRedis('default persistence'));
  results.push(await runRedis('AOF appendfsync=always'));
  await writeMarkdown(results);
}

async function runPostgres(mode: string, synchronousCommit: 'on' | 'off'): Promise<DurabilityResult> {
  const pool = new Pool({ connectionString: postgresUrl, max: 4 });
  const store = new PostgresJobStore(pool);
  let enqueued = 0;

  try {
    await pool.query(`SET synchronous_commit = ${synchronousCommit}`);
    for (; enqueued < jobCount; enqueued += 1) {
      await store.enqueue(makeJob(`postgres-${mode}-${enqueued}`));
      if (enqueued === Math.floor(jobCount / 2)) {
        await killAndRestart(postgresContainer);
      }
    }
  } catch {
    await killAndRestart(postgresContainer);
  } finally {
    await pool.end().catch(() => undefined);
  }

  const recoverablePool = new Pool({ connectionString: postgresUrl, max: 2 });
  const recoverable = Number((await recoverablePool.query('SELECT count(*)::int AS count FROM notifire_jobs')).rows[0]?.count ?? 0);
  await recoverablePool.end();
  return { backend: 'Postgres', mode, enqueued, recoverable, lost: Math.max(0, enqueued - recoverable) };
}

async function runRedis(mode: string): Promise<DurabilityResult> {
  const queue = new Queue<NotificationJob>('notifire_jobs', {
    connection: { url: redisUrl, maxRetriesPerRequest: null }
  });
  let enqueued = 0;

  try {
    for (; enqueued < jobCount; enqueued += 1) {
      await queue.add('otp.requested', makeJob(`redis-${mode}-${enqueued}`), { jobId: `redis-${mode}-${enqueued}` });
      if (enqueued === Math.floor(jobCount / 2)) {
        await killAndRestart(redisContainer);
      }
    }
  } catch {
    await killAndRestart(redisContainer);
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

function makeJob(id: string): NotificationJob {
  return {
    id,
    trigger: 'otp.requested',
    channel: 'email',
    recipient: { email: 'person@example.com' },
    data: { code: '123456' },
    templateId: 'otp-email.hbs'
  };
}

void main();
