"""Add AI Chat tables

Tables for multi-model LLM chat: conversations, messages,
model responses with ratings, and prompt templates.

Revision ID: j2b3c4d5e6f7
Revises: i1a2b3c4d5e6
Create Date: 2026-03-30 10:00:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB

revision: str = 'j2b3c4d5e6f7'
down_revision: Union[str, None] = 'i1a2b3c4d5e6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Chat conversations
    op.create_table(
        'chat_conversations',
        sa.Column('id', UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
        sa.Column('user_id', UUID(as_uuid=True), sa.ForeignKey('users.id', ondelete='CASCADE'), nullable=False),
        sa.Column('title', sa.String(200), server_default='新对话'),
        sa.Column('tags', JSONB, server_default='[]'),
        sa.Column('is_pinned', sa.Boolean, server_default='false'),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()')),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()')),
    )
    op.create_index('ix_chat_conv_user', 'chat_conversations', ['user_id'])
    op.create_index('ix_chat_conv_updated', 'chat_conversations', ['updated_at'])

    # Chat messages
    op.create_table(
        'chat_messages',
        sa.Column('id', UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
        sa.Column('conversation_id', UUID(as_uuid=True), sa.ForeignKey('chat_conversations.id', ondelete='CASCADE'), nullable=False),
        sa.Column('role', sa.String(20), nullable=False),
        sa.Column('content', sa.Text, server_default=''),
        sa.Column('attachments', JSONB, server_default='[]'),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()')),
    )
    op.create_index('ix_chat_msg_conv', 'chat_messages', ['conversation_id'])

    # Chat model responses
    op.create_table(
        'chat_model_responses',
        sa.Column('id', UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
        sa.Column('message_id', UUID(as_uuid=True), sa.ForeignKey('chat_messages.id', ondelete='CASCADE'), nullable=False),
        sa.Column('model_id', sa.String(100), nullable=False),
        sa.Column('model_name', sa.String(100), nullable=False),
        sa.Column('content', sa.Text, server_default=''),
        sa.Column('tokens_used', sa.Integer, nullable=True),
        sa.Column('latency_ms', sa.Integer, nullable=True),
        sa.Column('rating', sa.Integer, nullable=True),
        sa.Column('rating_comment', sa.Text, nullable=True),
        sa.Column('error', sa.Text, nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()')),
    )
    op.create_index('ix_chat_resp_msg', 'chat_model_responses', ['message_id'])
    op.create_index('ix_chat_resp_model', 'chat_model_responses', ['model_id'])
    op.create_index('ix_chat_resp_rating', 'chat_model_responses', ['rating'])

    # Chat prompt templates
    op.create_table(
        'chat_prompt_templates',
        sa.Column('id', UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
        sa.Column('user_id', UUID(as_uuid=True), sa.ForeignKey('users.id', ondelete='CASCADE'), nullable=True),
        sa.Column('name', sa.String(100), nullable=False),
        sa.Column('content', sa.Text, nullable=False),
        sa.Column('category', sa.String(50), server_default='general'),
        sa.Column('is_system', sa.Boolean, server_default='false'),
        sa.Column('usage_count', sa.Integer, server_default='0'),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()')),
    )
    op.create_index('ix_chat_tpl_user', 'chat_prompt_templates', ['user_id'])


def downgrade() -> None:
    op.drop_table('chat_prompt_templates')
    op.drop_table('chat_model_responses')
    op.drop_table('chat_messages')
    op.drop_table('chat_conversations')
