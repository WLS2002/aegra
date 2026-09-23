"""Persist scheduled principals, diagnostics and safe deletion intent.

Revision ID: e9a10923a001
Revises: e9a10909a002
"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "e9a10923a001"
down_revision = "e9a10909a002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "thread", sa.Column("cleanup_protected", sa.Boolean(), nullable=False, server_default=sa.text("false"))
    )
    op.add_column("thread", sa.Column("cleanup_run_id", sa.Text(), nullable=True))
    op.add_column("runs", sa.Column("error_details", postgresql.JSONB(), nullable=True))
    op.add_column("crons", sa.Column("principal", postgresql.JSONB(), nullable=True))
    op.add_column("crons", sa.Column("last_run_id", sa.Text(), nullable=True))
    op.add_column("crons", sa.Column("last_enqueued_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("crons", sa.Column("last_error_code", sa.Text(), nullable=True))
    op.add_column("crons", sa.Column("consecutive_failures", sa.Integer(), nullable=False, server_default="0"))
    op.add_column("crons", sa.Column("retry_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("crons", sa.Column("blocked", sa.Boolean(), nullable=False, server_default=sa.false()))
    op.create_index(
        "ix_thread_cleanup_pending",
        "thread",
        ["cleanup_run_id"],
        postgresql_where=sa.text("cleanup_run_id IS NOT NULL"),
    )


def downgrade() -> None:
    # Operators must finish pending deletion intents before removing the gate.
    connection = op.get_bind()
    if connection.execute(sa.text("SELECT EXISTS(SELECT 1 FROM thread WHERE cleanup_run_id IS NOT NULL)")).scalar():
        raise RuntimeError("Complete pending thread cleanup before downgrade")
    op.drop_index("ix_thread_cleanup_pending", table_name="thread")
    for name in (
        "blocked",
        "retry_at",
        "consecutive_failures",
        "last_error_code",
        "last_enqueued_at",
        "last_run_id",
        "principal",
    ):
        op.drop_column("crons", name)
    op.drop_column("runs", "error_details")
    op.drop_column("thread", "cleanup_run_id")
    op.drop_column("thread", "cleanup_protected")
