"""Persist run request identities independently of run retention."""

import sqlalchemy as sa

from alembic import op

revision = "e9a10909a001"
down_revision = "b88bb61be638"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "run_requests",
        sa.Column("user_id", sa.Text(), primary_key=True),
        sa.Column("thread_id", sa.Text(), primary_key=True),
        sa.Column("key", sa.Text(), primary_key=True),
        sa.Column("request_hash", sa.Text(), nullable=False),
        sa.Column("run_id", sa.Text(), nullable=False),
    )
    op.create_index("ix_run_requests_run_id", "run_requests", ["run_id"])


def downgrade() -> None:
    op.drop_table("run_requests")
