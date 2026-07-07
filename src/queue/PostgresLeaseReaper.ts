import { PostgresJobStore } from './PostgresJobStore.js';

interface PostgresLeaseReaperOptions {
  intervalMs?: number;
  leaseTimeoutMs?: number;
  maxAttempts?: number;
  onError?: (error: unknown) => void;
}

export class PostgresLeaseReaper {
  private timer?: ReturnType<typeof setInterval>;
  private running = false;

  constructor(
    private readonly store: PostgresJobStore,
    private readonly options: PostgresLeaseReaperOptions = {}
  ) {}

  start(): void {
    if (this.timer) {
      return;
    }

    const intervalMs = this.options.intervalMs ?? 10_000;
    this.timer = setInterval(() => void this.runOnce(), intervalMs);
    this.timer.unref();
  }

  stop(): void {
    if (!this.timer) {
      return;
    }

    clearInterval(this.timer);
    this.timer = undefined;
  }

  async runOnce(): Promise<number> {
    if (this.running) {
      return 0;
    }

    this.running = true;
    try {
      return await this.store.reclaimExpired(this.options.leaseTimeoutMs ?? 30_000, this.options.maxAttempts ?? 5);
    } catch (error) {
      this.options.onError?.(error);
      return 0;
    } finally {
      this.running = false;
    }
  }
}
