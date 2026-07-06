import type { JobResult } from '../types.js';

export interface ChannelProvider<T = unknown> {
  name: string;
  send(message: T): Promise<JobResult>;
}
