# Workspace runtime 0.12.0+harbor.1

This fork keeps PostgreSQL runs, checkpoints and idempotency receipts authoritative. Redis accelerates dispatch and streaming. PostgreSQL discovery runs every five seconds even while Redis delivers other jobs.

## Scheduling and authorization

Cron creation and authenticated PATCH retain a minimal principal (identity, roles/scopes, authentication state), never its credential. Execution resolves the assistant to a registered graph and authorizes that graph. `assistant_id` remains the caller's value for compatibility; handlers should use the trusted `graph_id`. Set `SCHEDULED_PRINCIPAL_RESOLVER=package.module:function` to revalidate service roles against current application policy. The callable receives keyword arguments `user`, `graph_id`, `source`; returning `None` revokes execution. Timer resumes use the same policy.

Legacy crons without a principal block until an authenticated PATCH rebinds them. Permanent authorization failures expose a bounded error code; transient errors retry with backoff. `last_run_id` and `last_enqueued_at` describe scheduled admissions, not business completion. `blocked`, `consecutive_failures` and `retry_at` explain failures.

## Recovery and cleanup

Ephemeral cleanup requires the target to be the latest successful run, an idle thread, no active runs or pending/blocked wakeups, no parent/child recovery relationship, and no manual state write. Admission and cleanup share a PostgreSQL advisory lock. Cleanup commits an intent before deleting checkpoints; a sweeper finishes interrupted deletions. Writers receive 409 `thread_cleanup_pending` after an intent commits. Interrupted/error/timeout state is retained. Independent idempotency receipts survive successful ephemeral deletion.

Migration `e9a10923a001` adds fields and an index. It does not purge history or checkpoints. Downgrade refuses pending cleanup intents. Emergency rollback must stop admission/schedulers, drain active work, finish and verify zero pending intents, then restore compatible images. Do not restore old database snapshots over newly admitted work. Prefer retaining the new additive schema and the fixed runtime when reverting application code. If the runtime image itself must fall back to 0.11.1, set `RUN_MIGRATIONS_ON_STARTUP=false`: the older Alembic package cannot resolve the new revision. The isolated rollback drill verified this combination with zero cleanup intents; restore scheduler state only after readiness and authorization checks.

## Bounded observability

`GET /health` and `/ready` perform database, checkpoint and store reads with deadlines; critical failure returns 503. Redis failure or memory exhaustion reports degraded dispatch; PostgreSQL dispatch remains active. Backlog and scheduler diagnostics are included. `ENABLE_PROMETHEUS_METRICS=true` exposes runtime metrics. `/live` only tests process liveness.

Run `error_details` contains bounded leaf types, categories, optional node/status code and stable location fingerprints. Arbitrary exception messages and provider responses are not persisted in new run error summaries. This does not scrub pre-existing application logs.

SSE replay defaults: 600 seconds, 8 MiB per run, 128 MiB total, 10,000 events per run. Byte accounting includes a fixed per-event allowance, not Redis's entire allocator overhead; set a larger Redis memory limit and a container limit above that. Redis evicts only replay keys within this budget. An expired/evicted `Last-Event-ID` receives 409 `replay_unavailable`; reload the run and thread state before starting a new live subscription. Never resubmit the execution to repair a stream. Local slow consumers are bounded and disconnected explicitly.

## Upstream provenance

Reviewed upstream main `71ec081e87f026a9579510f70ea23d3fb559977b` (2026-09-20) and release 0.10.5. Selective changes: #601 wait/join errors; #623 local wait timeout; #617 checkpoint alias; #598 replay TTL/counter cleanup; #603 atomic metadata merge; #602 partial assistant PATCH; #498 v2 context; #568 input.respond state updates; #566 configurable search limit; #579/#619 entity ID validation; #620 Docker port mapping. Fork-specific admission, authorization, idempotency and persistent wakeups remain in place. Thread TTL deletion, bulk cancel and unmerged candidate PRs are excluded.
