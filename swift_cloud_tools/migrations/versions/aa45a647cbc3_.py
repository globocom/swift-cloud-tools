"""empty message

Revision ID: aa45a647cbc3
Revises: 4fafa4bc6d85
Create Date: 2022-11-29 11:56:52.205007

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'aa45a647cbc3'
down_revision = '4fafa4bc6d85'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index('project_id', table_name='project_container_hostname')
    op.create_unique_constraint(None, 'project_container_hostname', ['project_id', 'container_name'])
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, 'project_container_hostname', type_='unique')
    op.create_index('project_id', 'project_container_hostname', ['project_id', 'container_name', 'hostname'], unique=True)
    # ### end Alembic commands ###
