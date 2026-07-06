# notifyre-core

Self-hosted notification library for Node.js. Phase 1 supports email delivery through SMTP, Handlebars templates, and an in-memory queue.

## Installation

```sh
npm install notifyre-core
```

## Example

```ts
import { resolve } from 'node:path';
import { Notifyre, SMTPEmailProvider } from 'notifyre-core';

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

await notifyre.trigger('subscription.confirmed', {
  recipient: { email: 'user@example.com' },
  data: { planName: 'Team', renewsOn: '2026-08-01' }
});
```

Templates use frontmatter for the subject and HTML for the body:

```hbs
---
subject: "Your {{planName}} subscription is confirmed"
---
<p>Your {{planName}} plan is active.</p>
```
