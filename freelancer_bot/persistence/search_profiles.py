from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from hashlib import sha256
import json
from typing import Any
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from ..billing import TRIAL_POLICY_VERSION
from ..profile_onboarding import (
    OnboardingProfileAnalysis,
    OnboardingProfileAnalysisCall,
)
from ..search_profiles import (
    BudgetPolicy,
    OpportunityType,
    ParsedSearchProfile,
    SearchProfilePreferences,
    SearchProfileTerm,
    SearchProfileTermOrigin,
    WorkMode,
    empty_search_profile_preferences,
    normalize_semantic_text,
)
from .schema import search_profile_analysis_cache, search_profiles, users
from .jobs import DurableJobRepository


class UserNotFound(LookupError):
    pass


class SearchProfileNotFound(LookupError):
    pass


class SearchProfileConflict(RuntimeError):
    pass


class SearchProfileEditConflict(RuntimeError):
    pass


class SearchProfileOwnershipError(PermissionError):
    pass


class SearchProfileActivationError(RuntimeError):
    pass


class SearchProfileActivationConflict(SearchProfileActivationError):
    pass


class SearchProfileConfirmationStatus(str, Enum):
    DRAFT = "draft"
    CONFIRMED = "confirmed"


class SearchProfileAnalysisCacheNotFound(LookupError):
    pass


@dataclass(frozen=True)
class UserRecord:
    id: UUID
    platform: str
    external_user_id: str
    trial_started_at: datetime | None
    trial_expires_at: datetime | None
    trial_policy_version: str | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class UserWriteOutcome:
    user: UserRecord
    created: bool


@dataclass(frozen=True)
class SearchProfileRecord:
    id: UUID
    user_id: UUID
    schema_version: str
    parser_version: str
    analysis_cache_id: UUID | None
    roles: tuple[SearchProfileTerm, ...]
    skills: tuple[SearchProfileTerm, ...]
    categories: tuple[SearchProfileTerm, ...]
    semantic_text_original: str
    semantic_text_normalized: str
    preferences: SearchProfilePreferences
    confirmation_status: SearchProfileConfirmationStatus
    revision: int
    confirmed_at: datetime | None
    is_active: bool
    is_primary: bool
    activated_at: datetime | None
    deactivated_at: datetime | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class SearchProfileWriteOutcome:
    profile: SearchProfileRecord
    created: bool


@dataclass(frozen=True)
class SearchProfileActivationOutcome:
    profile: SearchProfileRecord
    trial_started: bool


@dataclass(frozen=True)
class SearchProfileAnalysisCacheRecord:
    id: UUID
    input_sha256: str
    normalized_input_text: str
    cache_version: str
    schema_version: str
    provider: str
    requested_model: str
    response_model: str
    analyzer_version: str
    prompt_version: str
    attempt_count: int
    provider_attempts: int
    completed_calls: int
    timeout_count: int
    transient_failure_count: int
    non_retryable_failure_count: int
    invalid_output_retry_count: int
    input_tokens: int
    output_tokens: int
    total_tokens: int
    analysis: OnboardingProfileAnalysis
    created_at: datetime


@dataclass(frozen=True)
class SearchProfileAnalysisCacheWriteOutcome:
    record: SearchProfileAnalysisCacheRecord
    created: bool


class UserRepository:
    async def ensure(
        self,
        connection: AsyncConnection,
        *,
        platform: str,
        external_user_id: str,
    ) -> UserWriteOutcome:
        normalized_platform = _platform(platform)
        normalized_external_id = _required_text(
            external_user_id,
            "external_user_id",
            limit=255,
        )
        user_id = uuid4()
        inserted_id = await connection.scalar(
            pg_insert(users)
            .values(
                id=user_id,
                platform=normalized_platform,
                external_user_id=normalized_external_id,
            )
            .on_conflict_do_nothing(
                constraint="uq_users_platform_external_user_id"
            )
            .returning(users.c.id)
        )
        row = (
            await connection.execute(
                sa.select(users).where(
                    users.c.platform == normalized_platform,
                    users.c.external_user_id == normalized_external_id,
                )
            )
        ).mappings().one()
        return UserWriteOutcome(
            user=_user_record(row),
            created=inserted_id is not None,
        )

    async def get(
        self,
        connection: AsyncConnection,
        user_id: UUID,
    ) -> UserRecord:
        row = (
            await connection.execute(
                sa.select(users).where(users.c.id == user_id)
            )
        ).mappings().one_or_none()
        if row is None:
            raise UserNotFound(f"User {user_id} does not exist")
        return _user_record(row)

    async def get_by_identity(
        self,
        connection: AsyncConnection,
        *,
        platform: str,
        external_user_id: str,
    ) -> UserRecord:
        normalized_platform = _platform(platform)
        normalized_external_id = _required_text(
            external_user_id,
            "external_user_id",
            limit=255,
        )
        row = (
            await connection.execute(
                sa.select(users).where(
                    users.c.platform == normalized_platform,
                    users.c.external_user_id == normalized_external_id,
                )
            )
        ).mappings().one_or_none()
        if row is None:
            raise UserNotFound("User identity does not exist")
        return _user_record(row)


class SearchProfileRepository:
    async def create(
        self,
        connection: AsyncConnection,
        *,
        user_id: UUID,
        parsed_profile: ParsedSearchProfile,
        profile_id: UUID | None = None,
        analysis_cache_id: UUID | None = None,
    ) -> SearchProfileWriteOutcome:
        if await connection.scalar(
            sa.select(users.c.id).where(users.c.id == user_id)
        ) is None:
            raise UserNotFound(f"User {user_id} does not exist")

        identifier = profile_id or uuid4()
        values = _profile_values(
            user_id,
            parsed_profile,
            analysis_cache_id=analysis_cache_id,
            preferences=empty_search_profile_preferences(),
        )
        inserted_id = await connection.scalar(
            pg_insert(search_profiles)
            .values(id=identifier, **values)
            .on_conflict_do_nothing(index_elements=[search_profiles.c.id])
            .returning(search_profiles.c.id)
        )
        row = (
            await connection.execute(
                sa.select(search_profiles).where(search_profiles.c.id == identifier)
            )
        ).mappings().one()
        if inserted_id is None and not _profile_matches(row, values):
            raise SearchProfileConflict(
                "Search profile identifier already exists with different content"
            )
        return SearchProfileWriteOutcome(
            profile=_profile_record(row),
            created=inserted_id is not None,
        )

    async def get(
        self,
        connection: AsyncConnection,
        profile_id: UUID,
    ) -> SearchProfileRecord:
        row = (
            await connection.execute(
                sa.select(search_profiles).where(search_profiles.c.id == profile_id)
            )
        ).mappings().one_or_none()
        if row is None:
            raise SearchProfileNotFound(
                f"Search profile {profile_id} does not exist"
            )
        return _profile_record(row)

    async def list_for_user(
        self,
        connection: AsyncConnection,
        *,
        user_id: UUID,
    ) -> tuple[SearchProfileRecord, ...]:
        rows = (
            await connection.execute(
                sa.select(search_profiles)
                .where(search_profiles.c.user_id == user_id)
                .order_by(
                    search_profiles.c.is_primary.desc(),
                    search_profiles.c.is_active.desc(),
                    search_profiles.c.created_at.desc(),
                    search_profiles.c.id,
                )
            )
        ).mappings().all()
        return tuple(_profile_record(row) for row in rows)

    async def list_active(
        self,
        connection: AsyncConnection,
    ) -> tuple[SearchProfileRecord, ...]:
        rows = (
            await connection.execute(
                sa.select(search_profiles)
                .where(
                    search_profiles.c.is_active.is_(True),
                    search_profiles.c.confirmation_status
                    == SearchProfileConfirmationStatus.CONFIRMED.value,
                )
                .order_by(search_profiles.c.id)
            )
        ).mappings().all()
        return tuple(_profile_record(row) for row in rows)

    async def edit(
        self,
        connection: AsyncConnection,
        *,
        profile_id: UUID,
        user_id: UUID,
        parsed_profile: ParsedSearchProfile,
        expected_revision: int,
    ) -> SearchProfileRecord:
        if expected_revision < 1:
            raise ValueError("expected_revision must be positive")
        current = await self.get(connection, profile_id)
        if current.user_id != user_id:
            raise SearchProfileOwnershipError("search profile belongs to another user")
        if current.confirmation_status is not SearchProfileConfirmationStatus.DRAFT:
            raise SearchProfileEditConflict("confirmed search profile cannot be edited")
        if parsed_profile.semantic_text_original != current.semantic_text_original:
            raise ValueError("profile edits must preserve the original semantic text")

        values = _profile_values(
            user_id,
            parsed_profile,
            analysis_cache_id=current.analysis_cache_id,
            preferences=current.preferences,
        )
        row = (
            await connection.execute(
                sa.update(search_profiles)
                .where(
                    search_profiles.c.id == profile_id,
                    search_profiles.c.user_id == user_id,
                    search_profiles.c.confirmation_status
                    == SearchProfileConfirmationStatus.DRAFT.value,
                    search_profiles.c.revision == expected_revision,
                )
                .values(
                    **values,
                    revision=search_profiles.c.revision + 1,
                    updated_at=sa.func.now(),
                )
                .returning(search_profiles)
            )
        ).mappings().one_or_none()
        if row is None:
            raise SearchProfileEditConflict(
                "search profile changed before this edit was applied"
            )
        return _profile_record(row)

    async def update_preferences(
        self,
        connection: AsyncConnection,
        *,
        profile_id: UUID,
        user_id: UUID,
        preferences: SearchProfilePreferences,
        expected_revision: int,
    ) -> SearchProfileRecord:
        # Preference changes create a new profile revision. Match traces keep
        # their original profile revision, so historical matching evidence is
        # never rewritten when a user tunes an existing search.
        if expected_revision < 1:
            raise ValueError("expected_revision must be positive")
        current = await self.get(connection, profile_id)
        if current.user_id != user_id:
            raise SearchProfileOwnershipError("search profile belongs to another user")
        row = (
            await connection.execute(
                sa.update(search_profiles)
                .where(
                    search_profiles.c.id == profile_id,
                    search_profiles.c.user_id == user_id,
                    search_profiles.c.revision == expected_revision,
                )
                .values(
                    preferences=_preferences_value(preferences),
                    revision=search_profiles.c.revision + 1,
                    updated_at=sa.func.now(),
                )
                .returning(search_profiles)
            )
        ).mappings().one_or_none()
        if row is None:
            raise SearchProfileEditConflict(
                "search profile changed before settings were applied"
            )
        return _profile_record(row)

    async def confirm(
        self,
        connection: AsyncConnection,
        *,
        profile_id: UUID,
        user_id: UUID,
        expected_revision: int,
    ) -> SearchProfileRecord:
        if expected_revision < 1:
            raise ValueError("expected_revision must be positive")
        current = await self.get(connection, profile_id)
        if current.user_id != user_id:
            raise SearchProfileOwnershipError("search profile belongs to another user")
        if current.confirmation_status is SearchProfileConfirmationStatus.CONFIRMED:
            return current
        if current.revision != expected_revision:
            raise SearchProfileEditConflict(
                "search profile changed before confirmation"
            )
        row = (
            await connection.execute(
                sa.update(search_profiles)
                .where(
                    search_profiles.c.id == profile_id,
                    search_profiles.c.user_id == user_id,
                    search_profiles.c.confirmation_status
                    == SearchProfileConfirmationStatus.DRAFT.value,
                    search_profiles.c.revision == expected_revision,
                )
                .values(
                    confirmation_status=SearchProfileConfirmationStatus.CONFIRMED.value,
                    confirmed_at=sa.func.now(),
                    revision=search_profiles.c.revision + 1,
                    updated_at=sa.func.now(),
                )
                .returning(search_profiles)
            )
        ).mappings().one_or_none()
        if row is None:
            concurrent = await self.get(connection, profile_id)
            if (
                concurrent.user_id == user_id
                and concurrent.confirmation_status
                is SearchProfileConfirmationStatus.CONFIRMED
            ):
                return concurrent
            raise SearchProfileEditConflict(
                "search profile changed before confirmation"
            )
        return _profile_record(row)

    async def activate_primary(
        self,
        connection: AsyncConnection,
        *,
        profile_id: UUID,
        user_id: UUID,
        expected_revision: int,
    ) -> SearchProfileActivationOutcome:
        if expected_revision < 1:
            raise ValueError("expected_revision must be positive")
        user_row = (
            await connection.execute(
                sa.select(users)
                .where(users.c.id == user_id)
                .with_for_update()
            )
        ).mappings().one_or_none()
        if user_row is None:
            raise UserNotFound(f"User {user_id} does not exist")
        row = (
            await connection.execute(
                sa.select(search_profiles)
                .where(search_profiles.c.id == profile_id)
                .with_for_update()
            )
        ).mappings().one_or_none()
        if row is None:
            raise SearchProfileNotFound(
                f"Search profile {profile_id} does not exist"
            )
        current = _profile_record(row)
        if current.user_id != user_id:
            raise SearchProfileOwnershipError(
                "search profile belongs to another user"
            )
        if current.is_active and current.is_primary:
            return SearchProfileActivationOutcome(
                profile=current,
                trial_started=False,
            )
        if (
            current.confirmation_status
            is not SearchProfileConfirmationStatus.CONFIRMED
        ):
            raise SearchProfileActivationError(
                "search profile must be confirmed before activation"
            )
        if not (current.roles or current.skills or current.categories):
            raise SearchProfileActivationError(
                "search profile must contain a role, skill or category"
            )
        if current.revision != expected_revision:
            raise SearchProfileActivationConflict(
                "search profile changed before activation"
            )

        await connection.execute(
            sa.update(search_profiles)
            .where(
                search_profiles.c.user_id == user_id,
                search_profiles.c.id != profile_id,
                search_profiles.c.is_active.is_(True),
            )
            .values(
                is_active=False,
                is_primary=False,
                deactivated_at=sa.func.now(),
                revision=search_profiles.c.revision + 1,
                updated_at=sa.func.now(),
            )
        )
        activated = (
            await connection.execute(
                sa.update(search_profiles)
                .where(
                    search_profiles.c.id == profile_id,
                    search_profiles.c.user_id == user_id,
                    search_profiles.c.confirmation_status
                    == SearchProfileConfirmationStatus.CONFIRMED.value,
                    search_profiles.c.revision == expected_revision,
                )
                .values(
                    is_active=True,
                    is_primary=True,
                    activated_at=sa.func.coalesce(
                        search_profiles.c.activated_at,
                        sa.func.now(),
                    ),
                    deactivated_at=None,
                    revision=search_profiles.c.revision + 1,
                    updated_at=sa.func.now(),
                )
                .returning(search_profiles)
            )
        ).mappings().one_or_none()
        if activated is None:
            raise SearchProfileActivationConflict(
                "search profile changed before activation"
            )

        trial_started = user_row["trial_started_at"] is None
        if trial_started:
            await connection.execute(
                sa.update(users)
                .where(
                    users.c.id == user_id,
                    users.c.trial_started_at.is_(None),
                )
                .values(
                    trial_started_at=sa.func.now(),
                    trial_expires_at=sa.func.now() + sa.text("interval '3 days'"),
                    trial_policy_version=TRIAL_POLICY_VERSION,
                    updated_at=sa.func.now(),
                )
                )
        await DurableJobRepository().enqueue(
            connection,
            job_type="profile.coverage.recheck",
            idempotency_key=f"profile:{profile_id}:revision:{int(activated['revision'])}",
        )
        return SearchProfileActivationOutcome(
            profile=_profile_record(activated),
            trial_started=trial_started,
        )

    async def deactivate(
        self,
        connection: AsyncConnection,
        *,
        profile_id: UUID,
        user_id: UUID,
        expected_revision: int,
    ) -> SearchProfileRecord:
        if expected_revision < 1:
            raise ValueError("expected_revision must be positive")
        row = (
            await connection.execute(
                sa.select(search_profiles)
                .where(search_profiles.c.id == profile_id)
                .with_for_update()
            )
        ).mappings().one_or_none()
        if row is None:
            raise SearchProfileNotFound(
                f"Search profile {profile_id} does not exist"
            )
        current = _profile_record(row)
        if current.user_id != user_id:
            raise SearchProfileOwnershipError(
                "search profile belongs to another user"
            )
        if not current.is_active:
            return current
        if current.revision != expected_revision:
            raise SearchProfileActivationConflict(
                "search profile changed before deactivation"
            )
        deactivated = (
            await connection.execute(
                sa.update(search_profiles)
                .where(
                    search_profiles.c.id == profile_id,
                    search_profiles.c.user_id == user_id,
                    search_profiles.c.is_active.is_(True),
                    search_profiles.c.revision == expected_revision,
                )
                .values(
                    is_active=False,
                    is_primary=False,
                    deactivated_at=sa.func.now(),
                    revision=search_profiles.c.revision + 1,
                    updated_at=sa.func.now(),
                )
                .returning(search_profiles)
            )
        ).mappings().one_or_none()
        if deactivated is None:
            raise SearchProfileActivationConflict(
                "search profile changed before deactivation"
            )
        return _profile_record(deactivated)


class SearchProfileAnalysisCacheRepository:
    async def get(
        self,
        connection: AsyncConnection,
        cache_id: UUID,
    ) -> SearchProfileAnalysisCacheRecord:
        row = (
            await connection.execute(
                sa.select(search_profile_analysis_cache).where(
                    search_profile_analysis_cache.c.id == cache_id
                )
            )
        ).mappings().one_or_none()
        if row is None:
            raise SearchProfileAnalysisCacheNotFound(
                f"Search profile analysis cache {cache_id} does not exist"
            )
        return _cache_record(row)

    async def get_by_identity(
        self,
        connection: AsyncConnection,
        *,
        input_sha256: str,
        cache_version: str,
    ) -> SearchProfileAnalysisCacheRecord | None:
        row = (
            await connection.execute(
                sa.select(search_profile_analysis_cache).where(
                    search_profile_analysis_cache.c.input_sha256 == input_sha256,
                    search_profile_analysis_cache.c.cache_version == cache_version,
                )
            )
        ).mappings().one_or_none()
        return None if row is None else _cache_record(row)

    async def record(
        self,
        connection: AsyncConnection,
        *,
        input_sha256: str,
        original_input_text: str,
        normalized_input_text: str,
        cache_version: str,
        call: OnboardingProfileAnalysisCall,
    ) -> SearchProfileAnalysisCacheWriteOutcome:
        normalized = normalize_semantic_text(original_input_text)
        if normalized != normalized_input_text:
            raise ValueError("profile analysis cache input must be normalized")
        expected_hash = sha256(original_input_text.encode("utf-8")).hexdigest()
        if input_sha256 != expected_hash:
            raise ValueError("profile analysis cache input hash is inconsistent")
        if call.schema_version != call.analysis.schema_version:
            raise ValueError("profile analysis cache schema metadata is inconsistent")
        record_id = uuid4()
        inserted_id = await connection.scalar(
            pg_insert(search_profile_analysis_cache)
            .values(
                id=record_id,
                input_sha256=input_sha256,
                normalized_input_text=normalized_input_text,
                cache_version=cache_version,
                schema_version=call.schema_version,
                provider=call.provider,
                requested_model=call.requested_model,
                response_model=call.response_model,
                analyzer_version=call.analyzer_version,
                prompt_version=call.prompt_version,
                attempt_count=call.attempt_count,
                provider_attempts=max(1, call.provider_metrics.provider_attempts),
                completed_calls=max(1, call.provider_metrics.completed_calls),
                timeout_count=call.provider_metrics.timeouts,
                transient_failure_count=call.provider_metrics.transient_failures,
                non_retryable_failure_count=call.provider_metrics.non_retryable_failures,
                invalid_output_retry_count=call.provider_metrics.invalid_output_retries,
                input_tokens=call.usage.input_tokens,
                output_tokens=call.usage.output_tokens,
                total_tokens=call.usage.total_tokens,
                result=call.analysis.model_dump(mode="json"),
            )
            .on_conflict_do_nothing(
                constraint="uq_search_profile_analysis_cache_input_version"
            )
            .returning(search_profile_analysis_cache.c.id)
        )
        record = await self.get_by_identity(
            connection,
            input_sha256=input_sha256,
            cache_version=cache_version,
        )
        if record is None:
            raise SearchProfileAnalysisCacheNotFound(
                "Profile analysis cache record disappeared after persistence"
            )
        return SearchProfileAnalysisCacheWriteOutcome(
            record=record,
            created=inserted_id is not None,
        )


def _profile_values(
    user_id: UUID,
    profile: ParsedSearchProfile,
    *,
    analysis_cache_id: UUID | None,
    preferences: SearchProfilePreferences,
) -> dict[str, Any]:
    return {
        "user_id": user_id,
        "schema_version": profile.schema_version,
        "parser_version": profile.parser_version,
        "analysis_cache_id": analysis_cache_id,
        "roles": _term_values(profile.roles),
        "skills": _term_values(profile.skills),
        "categories": _term_values(profile.categories),
        "semantic_text_original": profile.semantic_text_original,
        "semantic_text_normalized": profile.semantic_text_normalized,
        "preferences": _preferences_value(preferences),
    }


def _term_values(terms: tuple[SearchProfileTerm, ...]) -> list[dict[str, Any]]:
    return [
        {
            "value": term.value,
            "normalized_value": term.normalized_value,
            "origin": term.origin.value,
            "evidence": term.evidence,
        }
        for term in terms
    ]


def _profile_matches(row: Mapping[str, Any], values: Mapping[str, Any]) -> bool:
    return all(row[field] == value for field, value in values.items())


def _user_record(row: Mapping[str, Any]) -> UserRecord:
    return UserRecord(
        id=row["id"],
        platform=str(row["platform"]),
        external_user_id=str(row["external_user_id"]),
        trial_started_at=row["trial_started_at"],
        trial_expires_at=row["trial_expires_at"],
        trial_policy_version=row["trial_policy_version"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _profile_record(row: Mapping[str, Any]) -> SearchProfileRecord:
    return SearchProfileRecord(
        id=row["id"],
        user_id=row["user_id"],
        schema_version=str(row["schema_version"]),
        parser_version=str(row["parser_version"]),
        analysis_cache_id=row["analysis_cache_id"],
        roles=_terms_from_json(row["roles"]),
        skills=_terms_from_json(row["skills"]),
        categories=_terms_from_json(row["categories"]),
        semantic_text_original=str(row["semantic_text_original"]),
        semantic_text_normalized=str(row["semantic_text_normalized"]),
        preferences=_preferences_from_json(row["preferences"]),
        confirmation_status=SearchProfileConfirmationStatus(
            row["confirmation_status"]
        ),
        revision=int(row["revision"]),
        confirmed_at=row["confirmed_at"],
        is_active=bool(row["is_active"]),
        is_primary=bool(row["is_primary"]),
        activated_at=row["activated_at"],
        deactivated_at=row["deactivated_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _terms_from_json(value: list[Mapping[str, str]]) -> tuple[SearchProfileTerm, ...]:
    return tuple(
        SearchProfileTerm(
            value=str(term["value"]),
            normalized_value=str(term["normalized_value"]),
            origin=SearchProfileTermOrigin(
                term.get("origin", SearchProfileTermOrigin.EXPLICIT.value)
            ),
            evidence=term.get("evidence"),
        )
        for term in value
    )


def _cache_record(row: Mapping[str, Any]) -> SearchProfileAnalysisCacheRecord:
    analysis = OnboardingProfileAnalysis.model_validate_json(
        json.dumps(row["result"]),
        strict=True,
    )
    return SearchProfileAnalysisCacheRecord(
        id=row["id"],
        input_sha256=str(row["input_sha256"]),
        normalized_input_text=str(row["normalized_input_text"]),
        cache_version=str(row["cache_version"]),
        schema_version=str(row["schema_version"]),
        provider=str(row["provider"]),
        requested_model=str(row["requested_model"]),
        response_model=str(row["response_model"]),
        analyzer_version=str(row["analyzer_version"]),
        prompt_version=str(row["prompt_version"]),
        attempt_count=int(row["attempt_count"]),
        provider_attempts=int(row["provider_attempts"]),
        completed_calls=int(row["completed_calls"]),
        timeout_count=int(row["timeout_count"]),
        transient_failure_count=int(row["transient_failure_count"]),
        non_retryable_failure_count=int(row["non_retryable_failure_count"]),
        invalid_output_retry_count=int(row["invalid_output_retry_count"]),
        input_tokens=int(row["input_tokens"]),
        output_tokens=int(row["output_tokens"]),
        total_tokens=int(row["total_tokens"]),
        analysis=analysis,
        created_at=row["created_at"],
    )


def _preferences_value(preferences: SearchProfilePreferences) -> dict[str, Any]:
    return {
        "schema_version": preferences.schema_version,
        "work_types": _enum_values(preferences.work_types),
        "minimum_budget": (
            None
            if preferences.minimum_budget is None
            else str(preferences.minimum_budget)
        ),
        "currency": preferences.currency,
        "budget_policy": (
            None
            if preferences.budget_policy is None
            else preferences.budget_policy.value
        ),
        "languages": _optional_term_values(preferences.languages),
        "geographies": _optional_term_values(preferences.geographies),
        "work_modes": _enum_values(preferences.work_modes),
        "excluded_categories": _optional_term_values(
            preferences.excluded_categories
        ),
    }


def _preferences_from_json(value: Mapping[str, Any]) -> SearchProfilePreferences:
    return SearchProfilePreferences(
        schema_version=str(value["schema_version"]),
        work_types=_optional_enum_tuple(value["work_types"], OpportunityType),
        minimum_budget=(
            None
            if value["minimum_budget"] is None
            else Decimal(str(value["minimum_budget"]))
        ),
        currency=(None if value["currency"] is None else str(value["currency"])),
        budget_policy=(
            None
            if value["budget_policy"] is None
            else BudgetPolicy(value["budget_policy"])
        ),
        languages=_optional_terms_from_json(value["languages"]),
        geographies=_optional_terms_from_json(value["geographies"]),
        work_modes=_optional_enum_tuple(value["work_modes"], WorkMode),
        excluded_categories=_optional_terms_from_json(
            value["excluded_categories"]
        ),
    )


def _enum_values(values: tuple[Enum, ...] | None) -> list[str] | None:
    return None if values is None else [value.value for value in values]


def _optional_enum_tuple(value, enum_type):
    return None if value is None else tuple(enum_type(item) for item in value)


def _optional_term_values(
    terms: tuple[SearchProfileTerm, ...] | None,
) -> list[dict[str, Any]] | None:
    return None if terms is None else _term_values(terms)


def _optional_terms_from_json(value) -> tuple[SearchProfileTerm, ...] | None:
    return None if value is None else _terms_from_json(value)


def _required_text(value: str, field: str, *, limit: int) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field} must not be blank")
    if len(normalized) > limit:
        raise ValueError(f"{field} must be at most {limit} characters")
    return normalized


def _platform(value: str) -> str:
    normalized = _required_text(value, "platform", limit=32).lower()
    if not normalized[0].isalpha() or not all(
        character.isascii() and (character.isalnum() or character in "_-")
        for character in normalized
    ):
        raise ValueError("platform must be a safe lowercase identifier")
    return normalized
