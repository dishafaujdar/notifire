import { beforeEach, describe, expect, it, vi } from 'vitest';
import nodemailer from 'nodemailer';
import { SMTPEmailProvider } from '../src/providers/SMTPEmailProvider.js';

// Mock the whole nodemailer module — we don't want a real SMTP connection in tests,
// only to verify SMTPEmailProvider calls sendMail with the right shape and handles
// its resolve/reject correctly.
vi.mock('nodemailer', () => ({
  default: {
    createTransport: vi.fn()
  }
}));

describe('SMTPEmailProvider', () => {
  const sendMail = vi.fn();

  beforeEach(() => {
    sendMail.mockReset();
    vi.mocked(nodemailer.createTransport).mockReturnValue({
      sendMail
    } as never);
  });

  it('maps EmailMessage + config.from onto nodemailer.sendMail correctly', async () => {
    sendMail.mockResolvedValueOnce({ messageId: 'abc123' });

    const provider = new SMTPEmailProvider({
      host: 'smtp.test.dev',
      port: 587,
      from: 'notifire@test.dev'
    });

    await provider.send({
      to: 'person@example.com',
      subject: 'Your code',
      html: '<p>908172</p>'
    });

    expect(sendMail).toHaveBeenCalledWith({
      from: 'notifire@test.dev',
      to: 'person@example.com',
      subject: 'Your code',
      html: '<p>908172</p>'
    });
  });

  it('returns { ok: true } when sendMail resolves', async () => {
    sendMail.mockResolvedValueOnce({ messageId: 'abc123' });

    const provider = new SMTPEmailProvider({
      host: 'smtp.test.dev',
      port: 587,
      from: 'notifire@test.dev'
    });

    const result = await provider.send({
      to: 'person@example.com',
      subject: 'Your code',
      html: '<p>908172</p>'
    });

    expect(result).toEqual({ ok: true });
  });

  it('returns { ok: false, retry: true, error } when sendMail throws an Error', async () => {
    sendMail.mockRejectedValueOnce(new Error('Connection timed out'));

    const provider = new SMTPEmailProvider({
      host: 'smtp.test.dev',
      port: 587,
      from: 'notifire@test.dev'
    });

    const result = await provider.send({
      to: 'person@example.com',
      subject: 'Your code',
      html: '<p>908172</p>'
    });

    expect(result).toEqual({
      ok: false,
      retry: true,
      error: 'Connection timed out'
    });
  });

  it('stringifies non-Error rejections instead of throwing on the .message access', async () => {
    // guards against a rejection value that isn't an Error instance at all —
    // error instanceof Error check in the provider needs to hold up here
    sendMail.mockRejectedValueOnce('raw string rejection');

    const provider = new SMTPEmailProvider({
      host: 'smtp.test.dev',
      port: 587,
      from: 'notifire@test.dev'
    });

    const result = await provider.send({
      to: 'person@example.com',
      subject: 'Your code',
      html: '<p>908172</p>'
    });

    expect(result).toEqual({
      ok: false,
      retry: true,
      error: 'raw string rejection'
    });
  });

  it('exposes name = "smtp" for delivery-log tagging', () => {
    const provider = new SMTPEmailProvider({
      host: 'smtp.test.dev',
      port: 587,
      from: 'notifire@test.dev'
    });

    expect(provider.name).toBe('smtp');
  });
});