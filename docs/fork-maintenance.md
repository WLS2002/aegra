# Maintenance fork

This fork starts from upstream Aegra v0.10.4. Keep `upstream` pointed to the original repository and `origin` pointed to the maintained fork. Preserve the upstream license and history. Keep application graphs, credentials and deployment-specific records outside this repository.

## Patch 1: atomic reject admission

An explicit `multitask_strategy="reject"` returns HTTP 409 when the same user's Thread has a pending or running Run. Terminal Runs do not block admission. The check applies to the shared preparation path used by background, wait, streaming and scheduled runs.

A PostgreSQL transaction advisory lock serializes admission by Thread ID across API instances, from before the active-run check until the new pending Run is committed. All strategies participate in the lock; their execution semantics otherwise remain unchanged. This also serializes initial requests for a Thread that does not yet exist. PostgreSQL's default READ COMMITTED isolation supplies the post-lock visibility of earlier commits. No schema migration or new dependency is required.

This patch does not implement enqueue, interrupt or rollback strategies, change the default strategy, or provide exclusion across different Threads. A rejected scheduled firing follows the existing scheduler retry behavior. A committed Run whose dispatch fails remains pending and therefore blocks reject admission until recovered or cancelled.

## Releases and upstream updates

Pin the upstream base, fork commit, built wheel, image digest and matching client version in deployment records. Build the API wheel from this checkout and install it with `--no-deps` over an image using the validated dependency set. Do not copy files into a running container.

Fetch upstream updates into a review branch. Check whether upstream supersedes each patch, run regression and isolated integration tests, then build an immutable candidate image. Validate cancellation, Cron and concurrent admission in both local and Redis worker modes before promoting. Keep the prior image for rollback; database migrations require a separate recovery plan.

Upstream concurrent-strategy work: https://github.com/aegra/aegra/pull/462. Evaluate it before expanding this patch.

## Validation of 0.10.4+harbor.1

Ruff lint and format pass across the workspace. API unit/integration tests: 1,906 passed, one existing database-dependent test skipped. CLI tests: 192 passed. Full ty checking reports 56 pre-existing diagnostics, identical to unmodified v0.10.4 with the same locked dependencies; no new diagnostic was introduced.

Four targeted E2E cases pass in Redis worker mode with two API instances and in single-instance LocalExecutor mode. They cover 20 simultaneous requests to existing/new Threads (one accepted, 19 conflicts), stream/wait conflicts, cancellation, success/error terminal states, independent Threads and bound Cron overlap. These tests use the synthetic echo fixture from Agent Harbor sandbox, with CRON_ALLOW_SECONDS_SCHEDULE=true for short scheduling tests. Set FORK_TEST_URLS to comma-separated isolated API URLs and FORK_TEST_TOKEN to the test credential.

Bootstrap a fresh database with one migration process before starting additional API instances. Multi-instance schema initialization is an upstream concern outside this patch. The overlay Dockerfile accepts BASE_IMAGE for a validated runtime with Python at /app/.venv/bin/python, and FORK_REVISION for provenance. Its default base name is a local placeholder; supply your validated image explicitly.
