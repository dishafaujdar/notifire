import { resolve } from 'node:path';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { Notifyre } from '../src/Notifyre.js';
import type { ChannelProvider } from '../src/providers/ChannelProvider.js';
import type { QueueAdapter } from '../src/queue/QueueAdapter.js';
import type { EmailMessage } from '../src/types.js';

const templatesDir = resolve('templates');

describe('Notifyre', () => {
  let provider: ChannelProvider<EmailMessage>;

  beforeEach(() => {
    provider = {
      name: 'test-email',
      send: vi.fn(async () => ({ ok: true as const }))
    };
  });

  it('throws if workflow is not registered', async () => {
    const notifyre = new Notifyre({ templatesDir, provider: { email: provider } });

    await expect(notifyre.trigger('missing.workflow', {
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
    const notifyre = new Notifyre({ queue, templatesDir, provider: { email: provider } });
    notifyre.defineWorkflow({
      trigger: 'otp.requested',
      steps: [{ channel: 'email', templateId: 'otp-email.hbs' }]
    });

    await expect(notifyre.trigger('otp.requested', {
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
    const notifyre = new Notifyre({
      templatesDir,
      provider: { email: { name: 'test-email', send } }
    });
    notifyre.defineWorkflow({
      trigger: 'otp.requested',
      steps: [{ channel: 'email', templateId: 'otp-email.hbs' }]
    });

    notifyre.start();
    await notifyre.trigger('otp.requested', {
      recipient: { email: 'person@example.com' },
      data: { code: '908172', expiresInSec: 180 }
    });
    await waitFor(() => expect(send).toHaveBeenCalled());

    expect(sentMessages[0]?.html).toContain('908172');
    expect(sentMessages[0]?.subject).toBe('Your Notifyre verification code');
  });

  it('renders subscription HTML with interpolated data', async () => {
    const sentMessages: EmailMessage[] = [];
    const send = vi.fn(async (message: EmailMessage) => {
      sentMessages.push(message);
      return { ok: true as const };
    });
    const notifyre = new Notifyre({
      templatesDir,
      provider: { email: { name: 'test-email', send } }
    });
    notifyre.defineWorkflow({
      trigger: 'subscription.confirmed',
      steps: [{ channel: 'email', templateId: 'subscription-welcome.hbs' }]
    });

    notifyre.start();
    await notifyre.trigger('subscription.confirmed', {
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
