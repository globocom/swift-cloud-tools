"""empty message

Revision ID: 5dc45e5e94d5
Revises: aa45a647cbc3
Create Date: 2023-02-07 16:03:53.718585

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = '5dc45e5e94d5'
down_revision = 'aa45a647cbc3'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('transfer_container_paginated',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('project_id', sa.String(length=64), nullable=False),
    sa.Column('project_name', sa.String(length=64), nullable=False),
    sa.Column('container_name', sa.String(length=255), nullable=False),
    sa.Column('marker', sa.String(length=255), nullable=True),
    sa.Column('hostname', sa.String(length=255), nullable=True),
    sa.Column('environment', sa.String(length=10), nullable=False),
    sa.Column('object_count_swift', sa.Integer(), nullable=True),
    sa.Column('bytes_used_swift', mysql.BIGINT(unsigned=True), nullable=True),
    sa.Column('count_error', sa.Integer(), nullable=True),
    sa.Column('object_count_gcp', sa.Integer(), nullable=True),
    sa.Column('bytes_used_gcp', mysql.BIGINT(), nullable=True),
    sa.Column('initial_date', sa.DateTime(), nullable=True),
    sa.Column('final_date', sa.DateTime(), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('project_id', 'container_name', 'marker')
    )
    op.create_index(op.f('ix_transfer_container_paginated_project_id'), 'transfer_container_paginated', ['project_id'], unique=False)
    op.create_table('transfer_container_paginated_error',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('object_error', sa.String(length=255), nullable=False),
    sa.Column('transfer_container_paginated_id', sa.Integer(), nullable=True),
    sa.Column('created', sa.DateTime(), nullable=True),
    sa.ForeignKeyConstraint(['transfer_container_paginated_id'], ['transfer_container_paginated.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('transfer_container_paginated_error')
    op.drop_index(op.f('ix_transfer_container_paginated_project_id'), table_name='transfer_container_paginated')
    op.drop_table('transfer_container_paginated')
    # ### end Alembic commands ###
