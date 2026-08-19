"""Add versioned exact and near-text opportunity dedup evidence.

Revision ID: 20260809_0014
Revises: 20260809_0013
Create Date: 2026-08-09
"""
from hashlib import sha256
import re
from typing import Sequence
import unicodedata

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "20260809_0014"
down_revision: str | None = "20260809_0013"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

ALGORITHM_VERSION = "canonical-opportunity-dedup.v1"
WINDOW_SECONDS = 7 * 24 * 60 * 60
TOKEN_PATTERN = re.compile(r"[^\W_]+(?:[.+#-][^\W_]+)*", re.UNICODE)


def upgrade() -> None:
    op.add_column(
        "opportunity_analysis_links",
        sa.Column("dedup_relation", sa.String(length=16)),
    )
    op.add_column(
        "opportunity_analysis_links",
        sa.Column("dedup_algorithm_version", sa.String(length=64)),
    )
    op.add_column(
        "opportunity_analysis_links",
        sa.Column("normalized_text_sha256", sa.String(length=64)),
    )
    op.add_column(
        "opportunity_analysis_links",
        sa.Column("dedup_similarity", sa.Numeric(precision=6, scale=5)),
    )
    op.add_column(
        "opportunity_analysis_links",
        sa.Column("dedup_window_seconds", sa.Integer()),
    )
    op.add_column(
        "opportunity_analysis_links",
        sa.Column("matched_analysis_cache_id", postgresql.UUID(as_uuid=True)),
    )
    op.create_foreign_key(
        op.f(
            "fk_opportunity_analysis_links_matched_analysis_cache_id_opportunity_analysis_cache"
        ),
        "opportunity_analysis_links",
        "opportunity_analysis_cache",
        ["matched_analysis_cache_id"],
        ["id"],
        ondelete="RESTRICT",
    )

    bind = op.get_bind()
    rows = bind.execute(
        sa.text(
            "SELECT links.analysis_cache_id, cache.normalized_content "
            "FROM opportunity_analysis_links links "
            "JOIN opportunity_analysis_cache cache "
            "ON cache.id = links.analysis_cache_id"
        )
    ).mappings().all()
    for row in rows:
        normalized = _normalize(row["normalized_content"])
        bind.execute(
            sa.text(
                "UPDATE opportunity_analysis_links SET "
                "dedup_relation = 'canonical', "
                "dedup_algorithm_version = :algorithm_version, "
                "normalized_text_sha256 = :normalized_text_sha256, "
                "dedup_window_seconds = :window_seconds "
                "WHERE analysis_cache_id = :analysis_cache_id"
            ),
            {
                "analysis_cache_id": row["analysis_cache_id"],
                "algorithm_version": ALGORITHM_VERSION,
                "normalized_text_sha256": _hash(normalized),
                "window_seconds": WINDOW_SECONDS,
            },
        )

    for column in (
        "dedup_relation",
        "dedup_algorithm_version",
        "normalized_text_sha256",
        "dedup_window_seconds",
    ):
        op.alter_column("opportunity_analysis_links", column, nullable=False)

    op.create_check_constraint(
        op.f("ck_opportunity_analysis_links_dedup_relation_valid"),
        "opportunity_analysis_links",
        "dedup_relation IN ('canonical', 'exact_duplicate', 'near_duplicate')",
    )
    op.create_check_constraint(
        op.f("ck_opportunity_analysis_links_dedup_algorithm_version_safe"),
        "opportunity_analysis_links",
        "dedup_algorithm_version ~ '^[a-z0-9][a-z0-9_.-]{0,63}$'",
    )
    op.create_check_constraint(
        op.f("ck_opportunity_analysis_links_normalized_text_hash_valid"),
        "opportunity_analysis_links",
        "length(normalized_text_sha256) = 64 "
        "AND normalized_text_sha256 ~ '^[0-9a-f]{64}$'",
    )
    op.create_check_constraint(
        op.f("ck_opportunity_analysis_links_dedup_window_positive"),
        "opportunity_analysis_links",
        "dedup_window_seconds > 0",
    )
    op.create_check_constraint(
        op.f("ck_opportunity_analysis_links_dedup_evidence_consistent"),
        "opportunity_analysis_links",
        "(dedup_relation = 'canonical' AND dedup_similarity IS NULL "
        "AND matched_analysis_cache_id IS NULL) "
        "OR (dedup_relation = 'exact_duplicate' AND dedup_similarity = 1 "
        "AND matched_analysis_cache_id IS NOT NULL) "
        "OR (dedup_relation = 'near_duplicate' "
        "AND dedup_similarity > 0 AND dedup_similarity < 1 "
        "AND matched_analysis_cache_id IS NOT NULL)",
    )
    op.create_index(
        "ix_opportunity_analysis_links_normalized_text_window",
        "opportunity_analysis_links",
        ["normalized_text_sha256", "linked_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_opportunity_analysis_links_normalized_text_window",
        table_name="opportunity_analysis_links",
    )
    for constraint in (
        "ck_opportunity_analysis_links_dedup_evidence_consistent",
        "ck_opportunity_analysis_links_dedup_window_positive",
        "ck_opportunity_analysis_links_normalized_text_hash_valid",
        "ck_opportunity_analysis_links_dedup_algorithm_version_safe",
        "ck_opportunity_analysis_links_dedup_relation_valid",
    ):
        op.drop_constraint(
            op.f(constraint),
            "opportunity_analysis_links",
            type_="check",
        )
    op.drop_constraint(
        op.f(
            "fk_opportunity_analysis_links_matched_analysis_cache_id_opportunity_analysis_cache"
        ),
        "opportunity_analysis_links",
        type_="foreignkey",
    )
    for column in (
        "matched_analysis_cache_id",
        "dedup_window_seconds",
        "dedup_similarity",
        "normalized_text_sha256",
        "dedup_algorithm_version",
        "dedup_relation",
    ):
        op.drop_column("opportunity_analysis_links", column)


def _normalize(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value).casefold()
    return " ".join(TOKEN_PATTERN.findall(normalized))


def _hash(value: str) -> str:
    return sha256(value.encode("utf-8")).hexdigest()
