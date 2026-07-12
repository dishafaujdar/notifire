# Notifire

A self-hosted, durable notification library for Node.js with workflow-based email, SMS, OTP, and push notifications.


## Installation

```bash
npm install notifire
```

---

## Quick Start

```ts
import { resolve } from 'node:path';
import { Notifire, SMTPEmailProvider } from 'notifire';

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
  steps: [
    {
      channel: 'email',
      templateId: 'otp-email.hbs'
    }
  ]
});

notifire.start();

await notifire.trigger('otp.requested', {
  recipient: {
    email: 'user@example.com'
  },
  data: {
    code: '123456'
  }
});
```

---

## Templates

Templates use Handlebars with frontmatter.

```hbs
---
subject: "Your OTP Code"
---

<p>Your verification code is <strong>{{code}}</strong>.</p>
```

---

## Queue Backends

### PostgreSQL (recommended)

- Durable (WAL-backed)
- Lease-based worker recovery
- Concurrent workers using `FOR UPDATE SKIP LOCKED`
- Dead-letter queue
- Automatic retries
- Idem

### BullMQ

Redis-backed queue implementing the same QueueAdapter interface.

---

## Providers

Current

- SMTP

Planned

- Twilio
- Resend
- AWS SES
- FCM

---

## Features

- Workflow-driven notifications
- Handlebars templates with frontmatter subjects
- Durable PostgreSQL queue (default)
- Optional BullMQ adapter
- Concurrent workers
- Automatic retries & dead-letter queue
- Worker crash recovery using leases
- Pluggable providers (SMTP, Twilio, etc.)

---

## License

MIT