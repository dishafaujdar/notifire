import { describe, expect, it, vi } from 'vitest';
import { InAppHandler } from '../src/handlers/InAppHandler.js';
import type { InAppMessage } from '../src/types.js';
import type { ChannelProvider } from '../src/providers/ChannelProvider.js';

const mockProvider: ChannelProvider<InAppMessage> = {
  name: 'test-in-app',
  send: vi.fn()
};

describe('InAppHandler', () => {
  it('renders title and body from template + data', async () => {
    vi.mocked(mockProvider.send).mockResolvedValueOnce({ ok: true });
    const handler = new InAppHandler(mockProvider, 'templates');

    await handler.process({
      id: '1', trigger: 'meeting.booked', channel: 'in_app',
      recipient: { recipientId: 'disha' },
      data: { name: 'Alex', title: 'Intro call', start: '2026-08-01T10:00:00Z' },
      templateId: 'meeting-booked-in-app.hbs'
    });

    expect(mockProvider.send).toHaveBeenCalledWith(
      expect.objectContaining({ body: expect.stringContaining('Alex') })
    );
  });

  it('returns retry:false when template is missing', async () => {
    const handler = new InAppHandler(mockProvider, 'templates');
    const result = await handler.process({
      id: '1', trigger: 'x', channel: 'in_app',
      recipient: { recipientId: 'disha' }, data: {},
      templateId: 'does-not-exist.hbs'
    });
    expect(result).toMatchObject({ ok: false, retry: false });
  });

  it('returns retry:true when provider.send throws', async () => {
    vi.mocked(mockProvider.send).mockRejectedValueOnce(new Error('Supabase down'));
    const handler = new InAppHandler(mockProvider, 'templates');
    const result = await handler.process({
      id: '1', trigger: 'meeting.booked', channel: 'in_app',
      recipient: { recipientId: 'disha' },
      data: { name: 'Alex', title: 'Intro call', start: 'now' },
      templateId: 'meeting-booked-in-app.hbs'
    });
    expect(result).toMatchObject({ ok: false, retry: true });
  });
});