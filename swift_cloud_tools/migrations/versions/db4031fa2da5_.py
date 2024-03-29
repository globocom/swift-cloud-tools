"""empty message

Revision ID: db4031fa2da5
Revises: 15a2d968c5e7
Create Date: 2022-11-16 14:40:00.267267

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'db4031fa2da5'
down_revision = '15a2d968c5e7'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('project_container_hostname',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('project_id', sa.String(length=64), nullable=False),
    sa.Column('container_name', sa.String(length=255), nullable=False),
    sa.Column('hostname', sa.String(length=100), nullable=False),
    sa.Column('updated', sa.DateTime(), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('project_id', 'container_name', 'hostname')
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('project_container_hostname')
    # ### end Alembic commands ###
