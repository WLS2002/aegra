# Runtime orchestration capabilities

This fork keeps business decisions in application graphs. Runtime capabilities provide durable execution identities, request deduplication, child creation and timed continuation. These additions are under implementation and have not yet been released.

## Idempotent creation

Threaded and stateless create, wait and stream endpoints accept `Idempotency-Key` (1-200 nonblank characters). The key is scoped to the authenticated user and thread; stateless requests derive a stable thread from the user and key. Reusing the key with different request parameters returns 409. A retained receipt prevents recreating a run after retention cleanup; such a retry also returns 409. Retain the thread with `on_completion: keep` when later result lookup is needed.

The run and its receipt commit together. In Redis mode a failed queue push leaves the committed pending run available to the existing lease reaper. This is durable admission, not a guarantee that application side effects happen exactly once. Business tools still need operation receipts and explicit handling of uncertain external results.

Cron occurrences use a stable identity based on cron ID and scheduled UTC timestamp. A retained receipt advances a retried occurrence even if its stateless run has already been deleted. Failed setup rolls back; it does not delete possibly committed work.

## Graph capabilities

`aegra_api.runtime.execution_identity()` returns the execution identity installed by the executor. Caller-supplied configurable IDs do not create this context.

`await create_child(assistant_id, key=..., input=...)` requires an active parent run and applies run-creation and assistant-read authorization. A key identifies one child within the parent thread across resumes. Child requests use the same transactional deduplication as HTTP requests. The returned run contains `parent` with the original parent run, thread and graph identifiers.

`GET /threads/{thread_id}/children` is a fork extension authorized as `threads.read`. It checks parent ownership and applies `threads.read` to each returned child thread. Results are owner-filtered and paginated with `limit` (1-100) and `offset`.

## Timed waits

Store an absolute, timezone-aware deadline in graph state, then call `wait_until(deadline)` in the waiting node. LangGraph persists an explicit runtime timer interrupt. AEGRA saves its interrupt ID, checkpoint ID and deadline in the same transaction that finalizes the interrupted run.

The wake scheduler resumes only due timer interrupts from the latest interrupted run and unchanged checkpoint. It consumes timers in the continuation-creation transaction. Ordinary human interrupts, cancelled waits and obsolete checkpoints are not automatically resumed. Cancelling an interrupted run cancels its pending timers. `WAKEUPS_ENABLED` controls the scheduler; `WAKEUP_POLL_INTERVAL_SECONDS` defaults to 5 seconds. Application crons may remain disabled while graph waits are available.

Nodes replay on resume; operations preceding the interrupt must be idempotent. Production crash recovery requires Redis workers and persistent LangGraph checkpoints. Runtime tests cover PostgreSQL admission and timer concurrency; full deployment and restart acceptance remains outstanding.
