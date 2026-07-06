# notifire-core

Self-hosted notification library for Node.js.

## Installation

```sh
npm install notifire-core
```

## Example

```ts
import { resolve } from 'node:path';
import { Notifire, SMTPEmailProvider } from 'notifire-core';

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

await notifire.trigger('subscription.confirmed', {
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
