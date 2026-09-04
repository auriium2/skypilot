"""Add durable external provisions.

Revision ID: 022
Revises: 021
Create Date: 2026-08-30
"""
# pylint: disable=invalid-name
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = '022'
down_revision: Union[str, Sequence[str], None] = '021'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    bind = op.get_bind()
    if sa.inspect(bind).has_table('external_provisions'):
        return
    op.create_table(
        'external_provisions',
        sa.Column('id', sa.Text(), primary_key=True),
        sa.Column('job_id', sa.Integer(), nullable=False),
        sa.Column('task_id', sa.Integer(), nullable=False),
        sa.Column('attempt', sa.Integer(), nullable=False),
        sa.Column('cluster_name', sa.Text(), nullable=False),
        sa.Column('cluster_name_on_cloud', sa.Text(), nullable=False),
        sa.Column('provider', sa.Text(), nullable=False),
        sa.Column('project', sa.Text(), nullable=False),
        sa.Column('region', sa.Text(), nullable=False),
        sa.Column('zone', sa.Text(), nullable=False),
        sa.Column('provider_resource_id', sa.Text(), nullable=False),
        sa.Column('request_hash', sa.Text(), nullable=False),
        sa.Column('request_json', sa.Text(), nullable=False),
        sa.Column('state', sa.Text(), nullable=False),
        sa.Column('provider_state', sa.Text()),
        sa.Column('created_at', sa.Float(), nullable=False),
        sa.Column('updated_at', sa.Float(), nullable=False),
        sa.Column('active_at', sa.Float()),
        sa.Column('terminal_at', sa.Float()),
        sa.Column('next_poll_at', sa.Float(), nullable=False),
        sa.Column('poll_attempt', sa.Integer(), nullable=False,
                  server_default='0'),
        sa.Column('cancel_requested_at', sa.Float()),
        sa.Column('last_error_code', sa.Text()),
        sa.Column('last_error_message', sa.Text()),
        sa.UniqueConstraint('job_id', 'task_id', 'attempt'),
        sa.UniqueConstraint('provider', 'project', 'zone',
                            'provider_resource_id'),
    )
    op.create_index('ix_external_provisions_job_id',
                    'external_provisions', ['job_id'])
    op.create_index('ix_external_provisions_state',
                    'external_provisions', ['state'])
    op.create_index('ix_external_provisions_next_poll_at',
                    'external_provisions', ['next_poll_at'])


def downgrade():
    pass
