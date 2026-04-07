"""Add concept_tags and industry_tags columns to analysis_results

Stores THS concept board tags (up to 3) and CITIC level-1 industry tags (1-3)
assigned by the LLM tagging phase after news passes the relevance filter.

Revision ID: d5b3c2e8f4a1
Revises: c4a2b1d9e3f7
Create Date: 2026-03-16 12:00:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision: str = 'd5b3c2e8f4a1'
down_revision: Union[str, None] = 'c4a2b1d9e3f7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('analysis_results', sa.Column('concept_tags', JSONB, server_default='[]', nullable=True))
    op.add_column('analysis_results', sa.Column('industry_tags', JSONB, server_default='[]', nullable=True))


def downgrade() -> None:
    op.drop_column('analysis_results', 'industry_tags')
    op.drop_column('analysis_results', 'concept_tags')
