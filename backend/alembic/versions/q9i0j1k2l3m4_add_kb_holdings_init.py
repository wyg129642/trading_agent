"""Track per-user auto-import state for the 持仓股票 knowledge-base folder.

Revision ID: q9i0j1k2l3m4
Revises: p8h9i0j1k2l3
Create Date: 2026-04-22 09:30:00.000000

Adds ``user_preferences.kb_holdings_initialized_at`` (nullable timestamp). A
NULL value means the user has never had their portfolio holdings imported
into the personal knowledge base; the first read of the personal KB tree
runs the auto-import and sets this timestamp. Non-null means "already done,
don't recreate" — so if the user later deletes the imported folder, we
respect that instead of resurrecting it on every fetch.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "q9i0j1k2l3m4"
down_revision: Union[str, None] = "p8h9i0j1k2l3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "user_preferences",
        sa.Column(
            "kb_holdings_initialized_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )


def downgrade() -> None:
    op.drop_column("user_preferences", "kb_holdings_initialized_at")
