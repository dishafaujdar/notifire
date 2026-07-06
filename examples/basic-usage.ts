import { resolve } from 'node:path';
import { Notifyre, SMTPEmailProvider } from '../src/index.js';

const notifyre = new Notifyre({
  templatesDir: resolve('templates'),
  provider: {
    email: new SMTPEmailProvider({
      host: 'localhost',
      port: 1025,
      secure: false,
      from: 'Notifyre <no-reply@example.com>'
    })
  }
});

notifyre.defineWorkflow({
  trigger: 'otp.requested',
  steps: [{ channel: 'email', templateId: 'otp-email.hbs' }]
});

notifyre.defineWorkflow({
  trigger: 'subscription.confirmed',
  steps: [{ channel: 'email', templateId: 'subscription-welcome.hbs' }]
});

notifyre.start();

await notifyre.trigger('otp.requested', {
  recipient: { email: 'user@example.com' },
  data: { code: '123456', expiresInSec: 300 }
});
