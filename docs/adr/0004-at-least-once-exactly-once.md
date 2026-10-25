# ADR 0004: At-least-once delivery, exactly-once results

**Status:** accepted

## Context
Kinesis and Kafka consumers can deliver a record more than once (retries, rebalances,
producer redelivery). Exactly-once transport is expensive and fragile across system
boundaries.

## Decision
Accept at-least-once delivery and make the sinks idempotent. Checkpoint offsets only after
the Iceberg commit; deduplicate on `event_id` in silver; recompute affected minutes rather
than incrementing counters. Late events are flagged by watermark and kept, never dropped.

## Consequences
* A replay of any window, or a crash mid-batch, converges to the same silver state. The test
  suite proves this: `test_redelivered_stream_events_do_not_double_count`.
* Bronze holds duplicates by design, which keeps it an exact audit of what was delivered.
