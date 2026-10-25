"""initial schema

Revision ID: 0001
Revises:
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

TS = sa.DateTime(timezone=True)


def upgrade() -> None:
    op.create_table(
        "pipelines",
        sa.Column("id", sa.String(32), primary_key=True),
        sa.Column("name", sa.String(200), nullable=False, unique=True),
        sa.Column("description", sa.Text),
        sa.Column("draft_spec", JSONB, nullable=False),
        sa.Column("draft_layout", JSONB, nullable=False),
        sa.Column("latest_version", sa.Integer, nullable=False),
        sa.Column("created_at", TS, nullable=False),
        sa.Column("updated_at", TS, nullable=False),
    )
    op.create_table(
        "pipeline_versions",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("pipeline_id", sa.String(32), sa.ForeignKey("pipelines.id", ondelete="CASCADE"), nullable=False),
        sa.Column("version", sa.Integer, nullable=False),
        sa.Column("spec", JSONB, nullable=False),
        sa.Column("ui_layout", JSONB, nullable=False),
        sa.Column("message", sa.Text),
        sa.Column("created_at", TS, nullable=False),
        sa.UniqueConstraint("pipeline_id", "version"),
    )
    op.create_index("ix_pipeline_versions_pipeline_id", "pipeline_versions", ["pipeline_id"])
    op.create_table(
        "connections",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(200), nullable=False, unique=True),
        sa.Column("type", sa.String(100), nullable=False),
        sa.Column("description", sa.Text),
        sa.Column("settings", JSONB, nullable=False),
        sa.Column("secrets", JSONB, nullable=False),
        sa.Column("created_at", TS, nullable=False),
        sa.Column("updated_at", TS, nullable=False),
    )
    op.create_table(
        "runs",
        sa.Column("id", sa.String(32), primary_key=True),
        sa.Column("pipeline_id", sa.String(32), sa.ForeignKey("pipelines.id", ondelete="CASCADE"), nullable=False),
        sa.Column("version", sa.Integer, nullable=False),
        sa.Column("env", sa.String(100), nullable=False),
        sa.Column("mode", sa.String(20), nullable=False),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("pid", sa.Integer),
        sa.Column("error", sa.Text),
        sa.Column("summary", JSONB, nullable=False),
        sa.Column("resolved_spec", JSONB, nullable=False),
        sa.Column("log_path", sa.Text),
        sa.Column("created_at", TS, nullable=False),
        sa.Column("started_at", TS),
        sa.Column("finished_at", TS),
    )
    op.create_index("ix_runs_pipeline_id", "runs", ["pipeline_id"])
    op.create_index("ix_runs_status", "runs", ["status"])
    op.create_table(
        "quality_results",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.String(32), sa.ForeignKey("runs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("pipeline_id", sa.String(200), nullable=False),
        sa.Column("env", sa.String(100), nullable=False),
        sa.Column("node_id", sa.String(200), nullable=False),
        sa.Column("quality_id", sa.String(200), nullable=False),
        sa.Column("check_id", sa.String(200), nullable=False),
        sa.Column("check_type", sa.String(100), nullable=False),
        sa.Column("level", sa.String(20), nullable=False),
        sa.Column("action", sa.String(20), nullable=False),
        sa.Column("total_rows", sa.BigInteger, nullable=False),
        sa.Column("failed_rows", sa.BigInteger),
        sa.Column("observed_value", sa.Float),
        sa.Column("passed", sa.Boolean, nullable=False),
        sa.Column("batch_id", sa.BigInteger),
        sa.Column("ts", sa.String(64), nullable=False),
    )
    op.create_index("ix_quality_results_run_id", "quality_results", ["run_id"])
    op.create_index("ix_quality_results_pipeline_id", "quality_results", ["pipeline_id"])


def downgrade() -> None:
    for table in ("quality_results", "runs", "connections", "pipeline_versions", "pipelines"):
        op.drop_table(table)
