# Schema v2 — telemetry change feed and subscriptions

Adds four tables on top of v1. `Base.metadata.create_all` creates them on start;
no destructive migration is needed.

## telemetry_seq
One row (`id=1`) holding the last allocated feed sequence. Writers lock this
row (`SELECT ... FOR UPDATE`) inside their own transaction, so sequence order
equals commit order. Rolled-back writers release both the row change and the
sequence bump.

## telemetry_changes
Append-only change log, `seq BIGINT PRIMARY KEY`. `op` is `insert` or `update`;
`payload` is the full post-change telemetry row (JSON); `telemetry_id` is
indexed. A change row exists only for committed writes.

## telemetry_subscriptions
`phase` is `history` (first pass over existing rows) or `changes`.
`backfill_high` is the highest telemetry id at cut-over, `backfill_cursor` the
last delivered history id, `seq_start` the feed sequence at cut-over,
`seq_cursor` the last acknowledged change sequence. Subscriptions read the live
table in slices; no per-subscription copy of telemetry is made.

## telemetry_subscription_batches
Persisted delivery units. `events` is the exact JSON array handed downstream,
`kind` is `history` or `changes`, `acked_at` marks downstream confirmation.
Progress advances only on ack; an unacked batch is resent unchanged after a
restart.
