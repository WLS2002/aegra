"""Unit tests for stateless (thread-free) run endpoints."""

import asyncio
import contextlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from fastapi.responses import StreamingResponse
from psycopg import Error as PsycopgError
from redis import RedisError
from sse_starlette import EventSourceResponse

from aegra_api.api.stateless_runs import (
    _background_cleanup_tasks,
    stateless_create_run,
    stateless_stream_run,
    stateless_wait_for_run,
)
from aegra_api.models import Run, RunCreate, User
from aegra_api.services.run_cleanup import (
    cleanup_after_background_run as _cleanup_after_background_run,
)
from aegra_api.services.run_cleanup import (
    delete_thread_by_id as _delete_thread_by_id,
)


class TestDeleteThreadById:
    async def test_failed_setup_only_requests_empty_thread_cleanup(self) -> None:
        with patch("aegra_api.services.run_cleanup._request_cleanup", new_callable=AsyncMock) as cleanup:
            await _delete_thread_by_id("thread", "owner")
        cleanup.assert_awaited_once_with("thread", "owner", None)

    async def test_checkpoint_failure_remains_retryable(self) -> None:
        with (
            patch("aegra_api.services.run_cleanup._request_cleanup", new_callable=AsyncMock, side_effect=PsycopgError),
            pytest.raises(PsycopgError),
        ):
            await _delete_thread_by_id("thread", "owner")


class TestCleanupAfterBackgroundRun:
    async def test_completed_wait_rechecks_under_deletion_gate(self) -> None:
        with (
            patch("aegra_api.services.run_cleanup.executor.wait_for_completion", new_callable=AsyncMock) as wait,
            patch("aegra_api.services.run_cleanup._request_cleanup", new_callable=AsyncMock) as cleanup,
        ):
            await _cleanup_after_background_run("run", "thread", "owner")
        wait.assert_awaited_once_with("run", timeout=3600.0)
        cleanup.assert_awaited_once_with("thread", "owner", "run")

    @pytest.mark.parametrize("failure", [TimeoutError, asyncio.CancelledError, PsycopgError, RedisError])
    async def test_failed_wait_preserves_checkpoint(self, failure: type[BaseException]) -> None:
        with (
            patch("aegra_api.services.run_cleanup.executor.wait_for_completion", new_callable=AsyncMock, side_effect=failure),
            patch("aegra_api.services.run_cleanup._request_cleanup", new_callable=AsyncMock) as cleanup,
        ):
            await _cleanup_after_background_run("run", "thread", "owner")
        cleanup.assert_not_awaited()

    async def test_checkpoint_failure_is_deferred(self) -> None:
        with (
            patch("aegra_api.services.run_cleanup.executor.wait_for_completion", new_callable=AsyncMock),
            patch("aegra_api.services.run_cleanup._request_cleanup", new_callable=AsyncMock, side_effect=PsycopgError) as cleanup,
        ):
            await _cleanup_after_background_run("run", "thread", "owner")
        cleanup.assert_awaited_once()


class TestStatelessWaitForRun:
    """Tests for POST /runs/wait."""

    @pytest.fixture
    def mock_user(self) -> User:
        return User(identity="test-user", scopes=[])

    @pytest.mark.asyncio
    async def test_delegates_and_deletes_thread(self, mock_user: User) -> None:
        """Delegates to wait_for_run and deletes ephemeral thread after stream."""
        import json

        expected_output = {"result": "done"}
        request = RunCreate(assistant_id="agent", input={"msg": "hi"})

        # wait_for_run now returns a StreamingResponse, so mock it accordingly
        mock_response = StreamingResponse(
            iter([json.dumps(expected_output).encode()]),
            media_type="application/json",
            headers={"Content-Location": "/threads/eph-thread-1/runs/run-wait-1"},
        )

        with (
            patch("aegra_api.api.stateless_runs.uuid4", return_value="eph-thread-1"),
            patch(
                "aegra_api.api.stateless_runs.wait_for_run",
                new_callable=AsyncMock,
                return_value=mock_response,
            ) as mock_wait,
            patch(
                "aegra_api.api.stateless_runs.delete_thread_by_id",
                new_callable=AsyncMock,
            ) as mock_delete,
            patch(
                "aegra_api.api.stateless_runs.cleanup_thread_if_safe",
                new_callable=AsyncMock,
            ) as mock_cleanup,
        ):
            result = await stateless_wait_for_run(request, mock_user)

            # Result is a StreamingResponse; consume to trigger cleanup
            assert isinstance(result, StreamingResponse)
            body = b""
            async for chunk in result.body_iterator:
                body += chunk if isinstance(chunk, bytes) else chunk.encode()

            assert json.loads(body) == expected_output
            mock_wait.assert_called_once_with("eph-thread-1", request, mock_user)
            mock_delete.assert_not_called()
            mock_cleanup.assert_awaited_once_with("run-wait-1", "eph-thread-1", mock_user.identity)

    @pytest.mark.asyncio
    async def test_keeps_thread_when_requested(self, mock_user: User) -> None:
        """Thread is preserved when on_completion='keep'."""
        import json

        request = RunCreate(assistant_id="agent", input={"msg": "hi"}, on_completion="keep")

        mock_response = StreamingResponse(
            iter([json.dumps({}).encode()]),
            media_type="application/json",
        )

        with (
            patch("aegra_api.api.stateless_runs.uuid4", return_value="eph-thread-2"),
            patch(
                "aegra_api.api.stateless_runs.wait_for_run",
                new_callable=AsyncMock,
                return_value=mock_response,
            ),
            patch(
                "aegra_api.api.stateless_runs.delete_thread_by_id",
                new_callable=AsyncMock,
            ) as mock_delete,
        ):
            result = await stateless_wait_for_run(request, mock_user)

        # Returns original response unchanged (no wrapper)
        assert result is mock_response
        mock_delete.assert_not_called()

    @pytest.mark.asyncio
    async def test_cleans_up_on_error(self, mock_user: User) -> None:
        """Thread is deleted even when wait_for_run raises."""
        request = RunCreate(assistant_id="agent", input={"msg": "hi"})

        with (
            patch("aegra_api.api.stateless_runs.uuid4", return_value="eph-thread-3"),
            patch(
                "aegra_api.api.stateless_runs.wait_for_run",
                new_callable=AsyncMock,
                side_effect=RuntimeError("boom"),
            ),
            patch(
                "aegra_api.api.stateless_runs.delete_thread_by_id",
                new_callable=AsyncMock,
            ) as mock_delete,
            pytest.raises(RuntimeError, match="boom"),
        ):
            await stateless_wait_for_run(request, mock_user)

        mock_delete.assert_called_once_with("eph-thread-3", mock_user.identity)

    @pytest.mark.asyncio
    async def test_cleanup_failure_does_not_mask_original_error(self, mock_user: User) -> None:
        """If _delete_thread_by_id raises during cleanup, the original error propagates."""
        request = RunCreate(assistant_id="agent", input={"msg": "hi"})

        with (
            patch("aegra_api.api.stateless_runs.uuid4", return_value="eph-thread-err"),
            patch(
                "aegra_api.api.stateless_runs.wait_for_run",
                new_callable=AsyncMock,
                side_effect=RuntimeError("original"),
            ),
            patch(
                "aegra_api.api.stateless_runs.delete_thread_by_id",
                new_callable=AsyncMock,
                side_effect=OSError("cleanup failed"),
            ),
            pytest.raises(RuntimeError, match="original"),
        ):
            await stateless_wait_for_run(request, mock_user)


class TestStatelessStreamRun:
    """Tests for POST /runs/stream."""

    @pytest.fixture
    def mock_user(self) -> User:
        return User(identity="test-user", scopes=[])

    @pytest.mark.asyncio
    async def test_delegates_and_wraps_body_for_cleanup(self, mock_user: User) -> None:
        """Delegates to create_and_stream_run and wraps iterator for cleanup."""
        request = RunCreate(assistant_id="agent", input={"msg": "hi"})

        async def _fake_body() -> AsyncIterator[bytes]:
            yield b"event: data\n\n"

        inner_close_handler = AsyncMock()
        mock_response = EventSourceResponse(
            _fake_body(),
            headers={"Content-Location": "/threads/t/runs/r"},
            client_close_handler_callable=inner_close_handler,
        )

        with (
            patch("aegra_api.api.stateless_runs.uuid4", return_value="eph-thread-4"),
            patch(
                "aegra_api.api.stateless_runs.create_and_stream_run",
                new_callable=AsyncMock,
                return_value=mock_response,
            ) as mock_stream,
            patch(
                "aegra_api.api.stateless_runs.delete_thread_by_id",
                new_callable=AsyncMock,
            ) as mock_delete,
            patch(
                "aegra_api.api.stateless_runs.cleanup_thread_if_safe",
                new_callable=AsyncMock,
            ) as mock_cleanup,
        ):
            result = await stateless_stream_run(request, mock_user)

            assert isinstance(result, EventSourceResponse)
            mock_stream.assert_called_once_with("eph-thread-4", request, mock_user)
            # Outer response must re-expose the inner close handler so real
            # http.disconnect still cancels the run.
            assert result.client_close_handler_callable is inner_close_handler

            # Consume the iterator to trigger cleanup (must be inside mock context)
            chunks: list[bytes] = []
            async for chunk in result.body_iterator:
                chunks.append(chunk)

            assert len(chunks) > 0
        mock_delete.assert_not_called()
        mock_cleanup.assert_awaited_once_with("r", "eph-thread-4", mock_user.identity)

    @pytest.mark.asyncio
    async def test_passes_through_when_keep(self, mock_user: User) -> None:
        """Returns original response unchanged when on_completion='keep'."""
        request = RunCreate(assistant_id="agent", input={"msg": "hi"}, on_completion="keep")

        async def _fake_body() -> AsyncIterator[bytes]:
            yield b"event: data\n\n"

        mock_response = EventSourceResponse(_fake_body())

        with (
            patch("aegra_api.api.stateless_runs.uuid4", return_value="eph-thread-5"),
            patch(
                "aegra_api.api.stateless_runs.create_and_stream_run",
                new_callable=AsyncMock,
                return_value=mock_response,
            ),
            patch(
                "aegra_api.api.stateless_runs.delete_thread_by_id",
                new_callable=AsyncMock,
            ) as mock_delete,
        ):
            result = await stateless_stream_run(request, mock_user)

        # Should return original response, not wrapped
        assert result is mock_response
        mock_delete.assert_not_called()

    @pytest.mark.asyncio
    async def test_cleans_up_thread_when_delegation_raises(self, mock_user: User) -> None:
        """Thread is deleted if create_and_stream_run raises (e.g. assistant not found)."""
        request = RunCreate(assistant_id="agent", input={"msg": "hi"})

        with (
            patch("aegra_api.api.stateless_runs.uuid4", return_value="eph-thread-err"),
            patch(
                "aegra_api.api.stateless_runs.create_and_stream_run",
                new_callable=AsyncMock,
                side_effect=RuntimeError("setup failed"),
            ),
            patch(
                "aegra_api.api.stateless_runs.delete_thread_by_id",
                new_callable=AsyncMock,
            ) as mock_delete,
            pytest.raises(RuntimeError, match="setup failed"),
        ):
            await stateless_stream_run(request, mock_user)

        mock_delete.assert_called_once_with("eph-thread-err", mock_user.identity)

    @pytest.mark.asyncio
    async def test_early_disconnect_keeps_thread(self, mock_user: User) -> None:
        """Client disconnect before stream completion must NOT delete the thread.

        Regression: deleting the thread here would cancel active runs via
        ``_delete_thread_by_id`` and break the ``on_disconnect="continue"``
        contract. The wrapper must mirror ``stateless_wait_for_run`` and only
        delete on normal completion.
        """
        request = RunCreate(assistant_id="agent", input={"msg": "hi"}, on_disconnect="continue")

        async def _fake_body() -> AsyncIterator[bytes]:
            yield b"event: metadata\n\n"
            # Client disconnect — outer EventSourceResponse cancels the iterator
            raise asyncio.CancelledError

        mock_response = EventSourceResponse(
            _fake_body(),
            headers={"Content-Location": "/threads/t/runs/r"},
        )

        with (
            patch("aegra_api.api.stateless_runs.uuid4", return_value="eph-thread-disc"),
            patch(
                "aegra_api.api.stateless_runs.create_and_stream_run",
                new_callable=AsyncMock,
                return_value=mock_response,
            ),
            patch(
                "aegra_api.api.stateless_runs.delete_thread_by_id",
                new_callable=AsyncMock,
            ) as mock_delete,
        ):
            result = await stateless_stream_run(request, mock_user)

            # Drain until CancelledError bubbles up from the iterator
            with contextlib.suppress(asyncio.CancelledError):
                async for _chunk in result.body_iterator:
                    pass

        mock_delete.assert_not_called()

    @pytest.mark.asyncio
    async def test_slow_client_disconnect_with_finished_run_schedules_cleanup(
        self,
        mock_user: User,
    ) -> None:
        """Slow-client / dead-proxy abort after the run finished must clean up.

        Regression for the leak described in review: ``completed = False``
        is also reached when sse-starlette aborts the body iterator on a
        send-timeout. If the broker reports the run finished, there's
        nothing left to resume — the wrapper must schedule a deferred
        delete instead of leaking the ephemeral thread.
        """
        request = RunCreate(assistant_id="agent", input={"msg": "hi"})

        async def _fake_body() -> AsyncIterator[bytes]:
            yield b"event: metadata\n\n"
            # Mid-stream abort, but by this point the broker reports finished.
            raise asyncio.CancelledError

        mock_response = EventSourceResponse(
            _fake_body(),
            headers={"Content-Location": "/threads/eph-thread-slow/runs/run-finished"},
        )

        finished_broker = MagicMock()
        finished_broker.is_finished.return_value = True

        with (
            patch("aegra_api.api.stateless_runs.uuid4", return_value="eph-thread-slow"),
            patch(
                "aegra_api.api.stateless_runs.create_and_stream_run",
                new_callable=AsyncMock,
                return_value=mock_response,
            ),
            patch(
                "aegra_api.api.stateless_runs.broker_manager.get_broker",
                return_value=finished_broker,
            ),
            patch(
                "aegra_api.api.stateless_runs.delete_thread_by_id",
                new_callable=AsyncMock,
            ) as mock_delete,
            patch(
                "aegra_api.api.stateless_runs.cleanup_thread_if_safe",
                new_callable=AsyncMock,
            ) as mock_cleanup,
        ):
            # Snapshot the cleanup-task set BEFORE running the scenario so
            # we only await tasks created by this test, not stragglers from
            # prior tests in the same session (which may belong to a
            # defunct event loop).
            tasks_before = set(_background_cleanup_tasks)

            result = await stateless_stream_run(request, mock_user)

            with contextlib.suppress(asyncio.CancelledError):
                async for _chunk in result.body_iterator:
                    pass

            new_tasks = [task for task in _background_cleanup_tasks if task not in tasks_before]
            if new_tasks:
                await asyncio.gather(*new_tasks, return_exceptions=True)

        mock_delete.assert_not_called()
        mock_cleanup.assert_awaited_once_with("run-finished", "eph-thread-slow", mock_user.identity)

    @pytest.mark.asyncio
    async def test_slow_client_disconnect_with_active_run_keeps_thread(
        self,
        mock_user: User,
    ) -> None:
        """Slow-client abort while the run is still active must NOT clean up.

        Symmetric to ``test_slow_client_disconnect_with_finished_run_schedules_cleanup``:
        when ``broker.is_finished()`` is False (run still running), the
        wrapper must keep the thread so the caller can still resume.
        Otherwise we'd cancel-via-deletion an in-flight execution and
        break the ``on_disconnect="continue"`` contract.
        """
        request = RunCreate(assistant_id="agent", input={"msg": "hi"}, on_disconnect="continue")

        async def _fake_body() -> AsyncIterator[bytes]:
            yield b"event: metadata\n\n"
            raise asyncio.CancelledError

        mock_response = EventSourceResponse(
            _fake_body(),
            headers={"Content-Location": "/threads/eph-thread-active/runs/run-active"},
        )

        active_broker = MagicMock()
        active_broker.is_finished.return_value = False  # run still in flight

        with (
            patch("aegra_api.api.stateless_runs.uuid4", return_value="eph-thread-active"),
            patch(
                "aegra_api.api.stateless_runs.create_and_stream_run",
                new_callable=AsyncMock,
                return_value=mock_response,
            ),
            patch(
                "aegra_api.api.stateless_runs.broker_manager.get_broker",
                return_value=active_broker,
            ),
            patch(
                "aegra_api.api.stateless_runs.delete_thread_by_id",
                new_callable=AsyncMock,
            ) as mock_delete,
        ):
            tasks_before = set(_background_cleanup_tasks)

            result = await stateless_stream_run(request, mock_user)

            with contextlib.suppress(asyncio.CancelledError):
                async for _chunk in result.body_iterator:
                    pass

            new_tasks = [task for task in _background_cleanup_tasks if task not in tasks_before]
            if new_tasks:
                await asyncio.gather(*new_tasks, return_exceptions=True)

        mock_delete.assert_not_called()
        assert not new_tasks, "Should not schedule cleanup when run still active"

    @pytest.mark.asyncio
    async def test_slow_client_disconnect_without_run_id_keeps_thread(
        self,
        mock_user: User,
    ) -> None:
        """Missing Content-Location header → slow-client cleanup branch is skipped.

        ``_extract_run_id_from_headers`` returns None when it can't parse
        the header. The wrapper falls through to the keep-thread branch
        rather than guessing — failing closed avoids false-positive
        deletions that would race with the still-running execution.
        """
        request = RunCreate(assistant_id="agent", input={"msg": "hi"}, on_disconnect="continue")

        async def _fake_body() -> AsyncIterator[bytes]:
            yield b"event: metadata\n\n"
            raise asyncio.CancelledError

        # No Content-Location → run_id extraction returns None.
        mock_response = EventSourceResponse(_fake_body(), headers={})

        with (
            patch("aegra_api.api.stateless_runs.uuid4", return_value="eph-thread-noid"),
            patch(
                "aegra_api.api.stateless_runs.create_and_stream_run",
                new_callable=AsyncMock,
                return_value=mock_response,
            ),
            patch(
                "aegra_api.api.stateless_runs.broker_manager.get_broker",
            ) as mock_get_broker,
            patch(
                "aegra_api.api.stateless_runs.delete_thread_by_id",
                new_callable=AsyncMock,
            ) as mock_delete,
        ):
            tasks_before = set(_background_cleanup_tasks)

            result = await stateless_stream_run(request, mock_user)

            with contextlib.suppress(asyncio.CancelledError):
                async for _chunk in result.body_iterator:
                    pass

            new_tasks = [task for task in _background_cleanup_tasks if task not in tasks_before]
            if new_tasks:
                await asyncio.gather(*new_tasks, return_exceptions=True)

        mock_delete.assert_not_called()
        # Without a run_id we must not consult the broker — the slow-client
        # branch is gated on `run_id is not None` precisely to skip this.
        mock_get_broker.assert_not_called()
        assert not new_tasks, "Should not schedule cleanup when run_id unavailable"

    @pytest.mark.asyncio
    async def test_stream_cleanup_failure_is_logged_not_raised(self, mock_user: User) -> None:
        """If _delete_thread_by_id raises during stream cleanup, it is logged but not propagated."""
        request = RunCreate(assistant_id="agent", input={"msg": "hi"})

        async def _fake_body() -> AsyncIterator[bytes]:
            yield b"event: data\n\n"

        mock_response = EventSourceResponse(_fake_body())

        with (
            patch("aegra_api.api.stateless_runs.uuid4", return_value="eph-thread-cleanup"),
            patch(
                "aegra_api.api.stateless_runs.create_and_stream_run",
                new_callable=AsyncMock,
                return_value=mock_response,
            ),
            patch(
                "aegra_api.api.stateless_runs.delete_thread_by_id",
                new_callable=AsyncMock,
                side_effect=OSError("cleanup failed"),
            ),
        ):
            result = await stateless_stream_run(request, mock_user)

            # Consuming the iterator should not raise despite cleanup failure
            chunks: list[bytes] = []
            async for chunk in result.body_iterator:
                chunks.append(chunk)

            assert len(chunks) > 0


class TestStatelessCreateRun:
    """Tests for POST /runs."""

    @pytest.fixture
    def mock_user(self) -> User:
        return User(identity="test-user", scopes=[])

    @pytest.fixture
    def mock_session(self) -> AsyncMock:
        session = AsyncMock()
        session.refresh = AsyncMock()
        session.add = MagicMock()
        return session

    @pytest.mark.asyncio
    async def test_delegates_and_schedules_cleanup(self, mock_user: User, mock_session: AsyncMock) -> None:
        """Delegates to create_run and schedules background cleanup."""
        run_id = str(uuid4())
        request = RunCreate(assistant_id="agent", input={"msg": "hi"})

        mock_run = Run(
            run_id=run_id,
            thread_id="eph-thread-6",
            assistant_id="agent",
            status="pending",
            input={"msg": "hi"},
            user_id=mock_user.identity,
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
        )

        with (
            patch("aegra_api.api.stateless_runs.uuid4", return_value="eph-thread-6"),
            patch(
                "aegra_api.api.stateless_runs.create_run",
                new_callable=AsyncMock,
                return_value=mock_run,
            ) as mock_create,
            patch("aegra_api.api.stateless_runs.asyncio.create_task") as mock_create_task,
        ):
            result = await stateless_create_run(request, mock_user, mock_session)

        assert result.run_id == run_id
        mock_create.assert_called_once_with("eph-thread-6", request, mock_user, mock_session)
        mock_create_task.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_cleanup_when_keep(self, mock_user: User, mock_session: AsyncMock) -> None:
        """No background cleanup task when on_completion='keep'."""
        run_id = str(uuid4())
        request = RunCreate(assistant_id="agent", input={"msg": "hi"}, on_completion="keep")

        mock_run = Run(
            run_id=run_id,
            thread_id="eph-thread-7",
            assistant_id="agent",
            status="pending",
            input={"msg": "hi"},
            user_id=mock_user.identity,
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
        )

        with (
            patch("aegra_api.api.stateless_runs.uuid4", return_value="eph-thread-7"),
            patch(
                "aegra_api.api.stateless_runs.create_run",
                new_callable=AsyncMock,
                return_value=mock_run,
            ),
            patch("aegra_api.api.stateless_runs.asyncio.create_task") as mock_create_task,
        ):
            result = await stateless_create_run(request, mock_user, mock_session)

        assert result.run_id == run_id
        mock_create_task.assert_not_called()

    @pytest.mark.asyncio
    async def test_cleans_up_thread_when_delegation_raises(self, mock_user: User, mock_session: AsyncMock) -> None:
        """Thread is deleted if create_run raises after auto-creating the thread."""
        request = RunCreate(assistant_id="agent", input={"msg": "hi"})

        with (
            patch("aegra_api.api.stateless_runs.uuid4", return_value="eph-thread-err"),
            patch(
                "aegra_api.api.stateless_runs.create_run",
                new_callable=AsyncMock,
                side_effect=RuntimeError("create failed"),
            ),
            patch(
                "aegra_api.api.stateless_runs.delete_thread_by_id",
                new_callable=AsyncMock,
            ) as mock_delete,
            pytest.raises(RuntimeError, match="create failed"),
        ):
            await stateless_create_run(request, mock_user, mock_session)

        mock_delete.assert_called_once_with("eph-thread-err", mock_user.identity)
