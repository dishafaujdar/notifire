CREATE TYPE notifire_job_status AS ENUM (
  'pending',
  'claimed',
  'completed',
  'failed'
);

CREATE TABLE notifire_jobs (
  id uuid PRIMARY KEY,
  payload jsonb NOT NULL,
  channel text NOT NULL,
  status notifire_job_status NOT NULL DEFAULT 'pending',
  priority integer NOT NULL DEFAULT 0,
  attempts integer NOT NULL DEFAULT 0,
  scheduled_at timestamptz NOT NULL DEFAULT now(),
  claimed_at timestamptz,
  claimed_by text,
  completed_at timestamptz,
  last_error text,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX notifire_jobs_claim_idx
  ON notifire_jobs (priority DESC, scheduled_at, created_at)
  WHERE status = 'pending';
