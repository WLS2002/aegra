"""Persist timer interrupts alongside finalized runs."""

import sqlalchemy as sa

from alembic import op

revision = "e9a10909a002"
down_revision = "e9a10909a001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "run_wakeups",
        sa.Column("run_id", sa.Text(), sa.ForeignKey("runs.run_id", ondelete="CASCADE"), primary_key=True),
        sa.Column("interrupt_id", sa.Text(), primary_key=True),
        sa.Column("checkpoint_id", sa.Text(), nullable=False),
        sa.Column("due_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
    )
    op.create_index("ix_run_wakeups_due", "run_wakeups", ["status", "due_at"])


def downgrade() -> None:
    op.drop_table("run_wakeups")
