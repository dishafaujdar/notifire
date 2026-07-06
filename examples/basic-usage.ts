import { resolve } from 'node:path';
import { Notifire, SMTPEmailProvider } from '../src/index.js';

const notifire = new Notifire({
  templatesDir: resolve('templates'),
  provider: {
    email: new SMTPEmailProvider({
      host: 'localhost',
      port: 1025,
      secure: false,
      from: 'Notifire <no-reply@example.com>'
    })
  }
});

notifire.defineWorkflow({
  trigger: 'otp.requested',
  steps: [{ channel: 'email', templateId: 'otp-email.hbs' }]
});

notifire.defineWorkflow({
  trigger: 'subscription.confirmed',
  steps: [{ channel: 'email', templateId: 'subscription-welcome.hbs' }]
});

notifire.start();

await notifire.trigger('otp.requested', {
  recipient: { email: 'user@example.com' },
  data: { code: '123456', expiresInSec: 300 }
});
