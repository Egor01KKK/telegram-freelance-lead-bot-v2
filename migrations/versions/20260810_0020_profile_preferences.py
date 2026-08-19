"""Add versioned structured search-profile preferences.

Revision ID: 20260810_0020
Revises: 20260809_0019
Create Date: 2026-08-10
"""
from typing import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "20260810_0020"
down_revision: str | None = "20260809_0019"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


_EMPTY_PREFERENCES = (
    "jsonb_build_object("
    "'schema_version', 'search_profile_preferences.v1', "
    "'work_types', NULL, 'minimum_budget', NULL, 'currency', NULL, "
    "'budget_policy', NULL, 'languages', NULL, 'geographies', NULL, "
    "'work_modes', NULL, 'excluded_categories', NULL)"
)


def upgrade() -> None:
    op.add_column(
        "search_profiles",
        sa.Column(
            "preferences",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text(_EMPTY_PREFERENCES),
            nullable=False,
        ),
    )
    op.create_check_constraint(
        op.f("ck_search_profiles_preferences_contract_valid"),
        "search_profiles",
        "jsonb_typeof(preferences) = 'object' "
        "AND preferences ->> 'schema_version' = "
        "'search_profile_preferences.v1' "
        "AND preferences ?& ARRAY['work_types', 'minimum_budget', 'currency', "
        "'budget_policy', 'languages', 'geographies', 'work_modes', "
        "'excluded_categories'] "
        "AND (preferences -> 'work_types' = 'null'::jsonb OR "
        "jsonb_typeof(preferences -> 'work_types') = 'array') "
        "AND (preferences -> 'minimum_budget' = 'null'::jsonb OR "
        "jsonb_typeof(preferences -> 'minimum_budget') = 'string') "
        "AND (preferences -> 'currency' = 'null'::jsonb OR "
        "jsonb_typeof(preferences -> 'currency') = 'string') "
        "AND (preferences -> 'budget_policy' = 'null'::jsonb OR "
        "preferences ->> 'budget_policy' IN "
        "('allow_unknown', 'require_explicit')) "
        "AND (preferences -> 'languages' = 'null'::jsonb OR "
        "jsonb_typeof(preferences -> 'languages') = 'array') "
        "AND (preferences -> 'geographies' = 'null'::jsonb OR "
        "jsonb_typeof(preferences -> 'geographies') = 'array') "
        "AND (preferences -> 'work_modes' = 'null'::jsonb OR "
        "jsonb_typeof(preferences -> 'work_modes') = 'array') "
        "AND (preferences -> 'excluded_categories' = 'null'::jsonb OR "
        "jsonb_typeof(preferences -> 'excluded_categories') = 'array')",
    )


def downgrade() -> None:
    op.drop_constraint(
        op.f("ck_search_profiles_preferences_contract_valid"),
        "search_profiles",
        type_="check",
    )
    op.drop_column("search_profiles", "preferences")
