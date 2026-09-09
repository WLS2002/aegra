# Reliable application execution (maintained fork)

Version `0.11.0+harbor.1` adds database-backed request identity, child runs and timed wakeups to the upstream 0.10.4 API. API and CLI use the same version. The database migrations add `run_requests` and `run_wakeups`; they do not reinterpret application business data.

## Idempotent run requests

Send `Idempotency-Key` (1–200 characters) on threaded or stateless create, stream or wait requests. The server scopes the key to the authenticated user and thread and hashes the validated request. Repeating the same request returns the original run; changing the body returns 409. A retained request tombstone also rejects replay after its run was deleted. Stateless keyed requests derive a stable thread and retain it so acknowledgement loss can be retried.

Keep the same body and key after a network timeout. Use a new key for a new operation. These keys prevent duplicate run creation; application tools still need their own business and external side-effect idempotency.

## Child runs

Inside an executing Graph call `aegra_api.runtime.create_child` with a stable child key, assistant ID and explicit input. This returns the normal `Run` model. The server derives the user and parent from its trusted execution context, authorizes the child, and commits the child and request identity together. A replay returns the original child even when the parent failed after child creation.

The normal run response includes optional `parent`. `GET /threads/{thread_id}/children` lists the authorized children (limit up to 100, offset pagination). A child resumed from a checkpoint retains the parent stored by AEGRA; clients cannot forge this relationship through configuration.

## Durable waiting

Inside a Graph call `wait_until()` with a timezone-aware datetime. It raises a LangGraph interrupt with marker `aegra.wait_until.v1`. AEGRA records the checkpoint interrupt and due time in PostgreSQL. The wake scheduler uses an idempotent continuation request, checks that the interrupted run is still current, and resumes the matching checkpoint. Concurrent scheduler ticks or process restarts do not create duplicate continuations. Cancelling a waiting run cancels its pending wakeups.

This is an execution service, not an in-process Graph sleep. Application-specific retry periods and stopping conditions remain in the Graph. Ordinary human interrupts are not automatically resumed.

## Cron occurrences and recovery

Each cron occurrence has a deterministic request key and thread identity. The scheduler advances an acknowledged occurrence without recreating its run after acknowledgement loss. Run preparation and wake consumption share database transaction and lock ordering. In Redis execution mode a committed pending run whose Redis submission fails remains available to the existing lease recovery service.

Deploy applications with their business schedules disabled until their own cutover checks pass. Enabling or changing a cron remains an explicit administrative operation. Use isolated PostgreSQL for integration and restart tests; do not resume incompatible application checkpoints after a Graph redesign.
