"""empty message

Revision ID: 05c2bfd0b2f6
Revises: 
Create Date: 2022-10-11 11:37:34.195367

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = '05c2bfd0b2f6'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('transfer_container',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('project_id', sa.String(length=64), nullable=False),
    sa.Column('project_name', sa.String(length=64), nullable=False),
    sa.Column('container_name', sa.String(length=255), nullable=False),
    sa.Column('environment', sa.String(length=10), nullable=False),
    sa.Column('container_count_swift', sa.Integer(), nullable=True),
    sa.Column('object_count_swift', sa.Integer(), nullable=True),
    sa.Column('bytes_used_swift', mysql.BIGINT(), nullable=True),
    sa.Column('last_object', sa.String(length=255), nullable=True),
    sa.Column('count_error', sa.Integer(), nullable=True),
    sa.Column('container_count_gcp', sa.Integer(), nullable=True),
    sa.Column('object_count_gcp', sa.Integer(), nullable=True),
    sa.Column('bytes_used_gcp', mysql.BIGINT(), nullable=True),
    sa.Column('initial_date', sa.DateTime(), nullable=True),
    sa.Column('final_date', sa.DateTime(), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('project_id')
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('transfer_container')
    # ### end Alembic commands ###
