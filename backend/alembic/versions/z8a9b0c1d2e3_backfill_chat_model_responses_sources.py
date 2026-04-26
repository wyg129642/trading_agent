"""Backfill chat_model_responses.sources column.

Schema-drift fix: ``ChatModelResponse.sources`` (JSONB) was added to the SQLA
model but no Alembic migration was ever recorded for it. Prod's DB was hand-
patched and has the column; staging (bootstrapped via ``init-staging`` →
``alembic upgrade head``) does NOT, which crashes the SSE chat send with
``UndefinedColumnError: column chat_model_responses.sources does not exist``.

This migration is additive and idempotent (``ADD COLUMN IF NOT EXISTS``), so
applying it on prod is a no-op and on staging adds the missing column.

Revision ID: z8a9b0c1d2e3
Revises: y7z8a9b0c1d2
Create Date: 2026-04-25 12:30:00.000000
"""
from typing import Sequence, Union

from alembic import op


revision: str = "z8a9b0c1d2e3"
down_revision: Union[str, None] = "y7z8a9b0c1d2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE chat_model_responses ADD COLUMN IF NOT EXISTS sources JSONB"
    )


def downgrade() -> None:
    # Intentionally a no-op. Dropping the column would be destructive on prod
    # (existing rows carry citation data), and the model still references it.
    pass
