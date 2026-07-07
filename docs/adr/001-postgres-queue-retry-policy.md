# ADR 001: Postgres queue retry policy

## Status

Accepted

## Context

Notifire currently targets low-volume self-hosted usage, roughly a few thousand notifications per day. Provider outages and transient SMTP failures still need retries, but the queue does not yet need a full backoff policy engine.

## Decision

Retryable job failures use a flat `retryDelayMs` delay, defaulting to 1 second. Jobs are moved to `dead_letter` after `maxAttempts`, defaulting to 5.

This is a deliberate simplification for the current scale. It keeps queue behavior easy to reason about while the core library is still small.

## Consequences

A provider outage can schedule many retries for the same time, so a recovering provider may see a burst of work. Before higher-volume production use, replace the flat retry delay with exponential backoff plus jitter.
