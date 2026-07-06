import { resolve } from 'node:path';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { Notifire } from '../src/Notifire.js';
import type { ChannelProvider } from '../src/providers/ChannelProvider.js';
import type { QueueAdapter } from '../src/queue/QueueAdapter.js';
import type { EmailMessage } from '../src/types.js';

const templatesDir = resolve('templates');

describe('Notifire', () => {
  let provider: ChannelProvider<EmailMessage>;

  beforeEach(() => {
    provider = {
      name: 'test-email',
      send: vi.fn(async () => ({ ok: true as const }))
    };
  });

  it('throws if workflow is not registered', async () => {
    const notifire = new Notifire({ templatesDir, provider: { email: provider } });

    await expect(notifire.trigger('missing.workflow', {
      recipient: { email: 'person@example.com' },
      data: {}
    })).rejects.toThrow('Workflow "missing.workflow" is not registered.');
  });

  it.each([
    ['missing', undefined],
    ['malformed', null]
  ])('throws if data is %s and never enqueues', async (_caseName, data) => {
    const queue: QueueAdapter = {
      enqueue: vi.fn(async () => undefined),
      consume: vi.fn(),
      stop: vi.fn(async () => undefined)
    };
    const notifire = new Notifire({ queue, templatesDir, provider: { email: provider } });
    notifire.defineWorkflow({
      trigger: 'otp.requested',
      steps: [{ channel: 'email', templateId: 'otp-email.hbs' }]
    });

    await expect(notifire.trigger('otp.requested', {
      recipient: { email: 'person@example.com' },
      data: data as unknown as Record<string, unknown>
    })).rejects.toThrow('Notification payload data must be an object.');

    expect(queue.enqueue).not.toHaveBeenCalled();
  });

  it('renders OTP HTML with interpolated data', async () => {
    const sentMessages: EmailMessage[] = [];
    const send = vi.fn(async (message: EmailMessage) => {
      sentMessages.push(message);
      return { ok: true as const };
    });
    const notifire = new Notifire({
      templatesDir,
      provider: { email: { name: 'test-email', send } }
    });
    notifire.defineWorkflow({
      trigger: 'otp.requested',
      steps: [{ channel: 'email', templateId: 'otp-email.hbs' }]
    });

    notifire.start();
    await notifire.trigger('otp.requested', {
      recipient: { email: 'person@example.com' },
      data: { code: '908172', expiresInSec: 180 }
    });
    await waitFor(() => expect(send).toHaveBeenCalled());

    expect(sentMessages[0]?.html).toContain('908172');
    expect(sentMessages[0]?.subject).toBe('Your Notifire verification code');
  });

  it('renders subscription HTML with interpolated data', async () => {
    const sentMessages: EmailMessage[] = [];
    const send = vi.fn(async (message: EmailMessage) => {
      sentMessages.push(message);
      return { ok: true as const };
    });
    const notifire = new Notifire({
      templatesDir,
      provider: { email: { name: 'test-email', send } }
    });
    notifire.defineWorkflow({
      trigger: 'subscription.confirmed',
      steps: [{ channel: 'email', templateId: 'subscription-welcome.hbs' }]
    });

    notifire.start();
    await notifire.trigger('subscription.confirmed', {
      recipient: { email: 'person@example.com' },
      data: { planName: 'Team', renewsOn: '2026-08-01' }
    });
    await waitFor(() => expect(send).toHaveBeenCalled());

    expect(sentMessages[0]?.html).toContain('Team');
    expect(sentMessages[0]?.subject).toBe('Your Team subscription is confirmed');
  });
});

async function waitFor(assertion: () => void): Promise<void> {
  const startedAt = Date.now();
  let lastError: unknown;

  while (Date.now() - startedAt < 500) {
    try {
      assertion();
      return;
    } catch (error) {
      lastError = error;
      await new Promise((resolveTimeout) => setTimeout(resolveTimeout, 10));
    }
  }

  throw lastError;
}
