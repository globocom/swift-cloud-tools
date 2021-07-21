"""empty message

Revision ID: ac30be541b7c
Revises: 5cc75abf1bf9
Create Date: 2021-07-21 13:02:34.566685

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = 'ac30be541b7c'
down_revision = 'eeddec297e92'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('container_info',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('project_id', sa.String(length=64), nullable=False),
    sa.Column('container_name', sa.String(length=255), nullable=False),
    sa.Column('object_count', sa.Integer(), nullable=True),
    sa.Column('bytes_used', mysql.BIGINT(), nullable=True),
    sa.Column('updated', sa.DateTime(), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('project_id')
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('container_info')
    # ### end Alembic commands ###
