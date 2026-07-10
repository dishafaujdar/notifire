import { mkdir, writeFile } from 'node:fs/promises';
import { dirname } from 'node:path';
import { performance } from 'node:perf_hooks';
import { Pool } from 'pg';
import { BullMQQueueAdapter, PostgresQueueAdapter, type NotificationJob, type QueueAdapter } from '../src/index.js';

const workerCounts = [1, 4, 8, 16];
const jobCount = Number(process.env.BENCH_JOB_COUNT ?? 5_000);
const postgresUrl = process.env.TEST_POSTGRES_URL;
const redisUrl = process.env.TEST_REDIS_URL;
const outputPath = 'bench/results/adapter-comparison.md';

interface BenchResult {
  adapter: string;
  workers: number;
  jobs: number;
  p50: number;
  p95: number;
  p99: number;
  wallClockMs: number;
  jobsPerSecond: number;
  errors: number;
}

async function main(): Promise<void> {
  if (!postgresUrl || !redisUrl) {
    throw new Error('Set TEST_POSTGRES_URL and TEST_REDIS_URL before running the adapter benchmark.');
  }

  const results: BenchResult[] = [];
  for (const workers of workerCounts) {
    results.push(await runOne('Postgres', workers, () => {
      const appPool = new Pool({ connectionString: postgresUrl, max: 4 });
      const workerPool = new Pool({ connectionString: postgresUrl, max: workers + 2 });
      return {
        adapter: new PostgresQueueAdapter({ appPool, workerPool, concurrency: workers }),
        cleanup: async () => {
          await appPool.end();
          await workerPool.end();
        }
      };
    }));

    results.push(await runOne('BullMQ', workers, () => ({
      adapter: new BullMQQueueAdapter({ redisUrl, concurrency: workers }),
      cleanup: async () => {}
    })));
  }

  await writeMarkdown(results);
}

async function runOne(
  adapterName: string,
  workers: number,
  makeAdapter: () => { adapter: QueueAdapter; cleanup: () => Promise<void> }
): Promise<BenchResult> {
  const { adapter, cleanup } = makeAdapter();
  const startedAt = new Map<string, number>();
  const latencies: number[] = [];
  let completed = 0;
  let errors = 0;
  let resolveDrained: (() => void) | undefined;
  const drained = new Promise<void>((resolve) => {
    resolveDrained = resolve;
  });

  adapter.consume(async (job) => {
    await sleep(Math.max(0, gaussian(80, 20)));
    latencies.push(performance.now() - (startedAt.get(job.id) ?? performance.now()));
    completed += 1;
    if (completed === jobCount) {
      resolveDrained?.();
    }
    return { ok: true };
  });

  const wallStart = performance.now();
  for (let index = 0; index < jobCount; index += 1) {
    const job = makeJob(`${adapterName}-${workers}-${index}`);
    startedAt.set(job.id, performance.now());
    try {
      await adapter.enqueue(job);
    } catch {
      errors += 1;
    }
  }

  await drained;
  const wallClockMs = performance.now() - wallStart;
  await adapter.stop();
  await cleanup();

  latencies.sort((a, b) => a - b);
  return {
    adapter: adapterName,
    workers,
    jobs: jobCount,
    p50: percentile(latencies, 0.5),
    p95: percentile(latencies, 0.95),
    p99: percentile(latencies, 0.99),
    wallClockMs,
    jobsPerSecond: jobCount / (wallClockMs / 1_000),
    errors
  };
}

async function writeMarkdown(results: BenchResult[]): Promise<void> {
  const lines = [
    '# Adapter Comparison',
    '',
    '| Adapter | Workers | Jobs | p50 ms | p95 ms | p99 ms | Drain ms | Jobs/sec | Errors |',
    '|---|---:|---:|---:|---:|---:|---:|---:|---:|',
    ...results.map((row) =>
      `| ${row.adapter} | ${row.workers} | ${row.jobs} | ${row.p50.toFixed(1)} | ${row.p95.toFixed(1)} | ${row.p99.toFixed(1)} | ${row.wallClockMs.toFixed(1)} | ${row.jobsPerSecond.toFixed(1)} | ${row.errors} |`
    )
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

function percentile(values: number[], p: number): number {
  return values[Math.min(values.length - 1, Math.floor(values.length * p))] ?? 0;
}

function gaussian(mean: number, stddev: number): number {
  const u = 1 - Math.random();
  const v = Math.random();
  return mean + stddev * Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

void main();
