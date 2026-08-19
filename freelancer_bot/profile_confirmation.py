from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal
from html import escape
from uuid import UUID

from .persistence.database import Database
from .persistence.jobs import DurableJobRepository
from .persistence.search_profiles import (
    SearchProfileAnalysisCacheRepository,
    SearchProfileConfirmationStatus,
    SearchProfileOwnershipError,
    SearchProfileRecord,
    SearchProfileRepository,
    UserRepository,
    UserRecord,
)
from .profile_discovery import (
    ProfileDiscoveryIntentRepository,
    build_profile_discovery_intent,
)
from .telegram_profile_discovery import (
    TELEGRAM_PROFILE_DISCOVERY_JOB_TYPE,
    profile_discovery_job_key,
)
from .search_profiles import (
    BudgetPolicy,
    OpportunityType,
    ParsedSearchProfile,
    SearchProfilePreferences,
    SearchProfileTerm,
    SearchProfileTermInput,
    WorkMode,
    parse_search_profile,
    parse_search_profile_preferences,
)


EDITABLE_PROFILE_FIELDS = frozenset({"roles", "skills", "categories"})


@dataclass(frozen=True)
class ProfileConfirmationView:
    profile: SearchProfileRecord
    missing_fields: tuple[str, ...]
    uncertain_terms: tuple[str, ...]


@dataclass(frozen=True)
class ProfileActivationView:
    profile: ProfileConfirmationView
    trial_started: bool


class ProfileConfirmationService:
    def __init__(
        self,
        database: Database,
        *,
        users: UserRepository | None = None,
        profiles: SearchProfileRepository | None = None,
        analysis_cache: SearchProfileAnalysisCacheRepository | None = None,
        discovery_intents: ProfileDiscoveryIntentRepository | None = None,
        jobs: DurableJobRepository | None = None,
    ) -> None:
        self._database = database
        self._users = users or UserRepository()
        self._profiles = profiles or SearchProfileRepository()
        self._analysis_cache = analysis_cache or SearchProfileAnalysisCacheRepository()
        self._discovery_intents = discovery_intents or ProfileDiscoveryIntentRepository()
        self._jobs = jobs or DurableJobRepository()

    async def create_manual_draft(
        self,
        *,
        platform: str,
        external_user_id: str,
        semantic_text: str,
        roles: tuple[str, ...],
        skills: tuple[str, ...],
        categories: tuple[str, ...],
    ) -> ProfileConfirmationView:
        if not roles and not skills and not categories:
            raise ValueError("at least one role, skill or category is required")
        parsed = parse_search_profile(
            roles=roles,
            skills=skills,
            categories=categories,
            semantic_text=semantic_text,
        )
        async with self._database.transaction() as connection:
            user = await self._users.ensure(
                connection,
                platform=platform,
                external_user_id=external_user_id,
            )
            outcome = await self._profiles.create(
                connection,
                user_id=user.user.id,
                parsed_profile=parsed,
            )
        return self._view(outcome.profile)

    async def show(
        self,
        *,
        platform: str,
        external_user_id: str,
        profile_id: UUID,
    ) -> ProfileConfirmationView:
        async with self._database.connect() as connection:
            user = await self._users.get_by_identity(
                connection,
                platform=platform,
                external_user_id=external_user_id,
            )
            profile = await self._profiles.get(connection, profile_id)
            if profile.user_id != user.id:
                raise SearchProfileOwnershipError(
                    "search profile belongs to another user"
                )
            uncertain_terms: tuple[str, ...] = ()
            if profile.analysis_cache_id is not None:
                cached = await self._analysis_cache.get(
                    connection,
                    profile.analysis_cache_id,
                )
                explicit_values = {
                    term.normalized_value
                    for terms in (profile.roles, profile.skills, profile.categories)
                    for term in terms
                    if term.origin.value == "explicit"
                }
                uncertain_terms = tuple(
                    term
                    for term in cached.analysis.uncertain_terms
                    if term.casefold() not in explicit_values
                )
        return self._view(profile, uncertain_terms=uncertain_terms)

    async def get_user(
        self,
        *,
        platform: str,
        external_user_id: str,
    ) -> UserRecord:
        async with self._database.connect() as connection:
            return await self._users.get_by_identity(
                connection,
                platform=platform,
                external_user_id=external_user_id,
            )

    async def edit_terms(
        self,
        *,
        platform: str,
        external_user_id: str,
        profile_id: UUID,
        field: str,
        values: tuple[str, ...],
        expected_revision: int,
    ) -> ProfileConfirmationView:
        if field not in EDITABLE_PROFILE_FIELDS:
            raise ValueError("unsupported search profile field")
        async with self._database.transaction() as connection:
            user = await self._users.get_by_identity(
                connection,
                platform=platform,
                external_user_id=external_user_id,
            )
            current = await self._profiles.get(connection, profile_id)
            if current.user_id != user.id:
                raise SearchProfileOwnershipError(
                    "search profile belongs to another user"
                )
            replacements = {
                "roles": _preserved_terms(current.roles),
                "skills": _preserved_terms(current.skills),
                "categories": _preserved_terms(current.categories),
            }
            replacements[field] = values
            parsed = parse_search_profile(
                roles=replacements["roles"],
                skills=replacements["skills"],
                categories=replacements["categories"],
                semantic_text=current.semantic_text_original,
            )
            if not parsed.roles and not parsed.skills and not parsed.categories:
                raise ValueError("a confirmed profile cannot have all fields empty")
            updated = await self._profiles.edit(
                connection,
                profile_id=profile_id,
                user_id=user.id,
                parsed_profile=parsed,
                expected_revision=expected_revision,
            )
        return await self.show(
            platform=platform,
            external_user_id=external_user_id,
            profile_id=updated.id,
        )

    async def confirm(
        self,
        *,
        platform: str,
        external_user_id: str,
        profile_id: UUID,
        expected_revision: int,
    ) -> ProfileConfirmationView:
        async with self._database.transaction() as connection:
            user = await self._users.get_by_identity(
                connection,
                platform=platform,
                external_user_id=external_user_id,
            )
            profile = await self._profiles.confirm(
                connection,
                profile_id=profile_id,
                user_id=user.id,
                expected_revision=expected_revision,
            )
        return await self.show(
            platform=platform,
            external_user_id=external_user_id,
            profile_id=profile.id,
        )

    async def activate(
        self,
        *,
        platform: str,
        external_user_id: str,
        profile_id: UUID,
        expected_revision: int,
    ) -> ProfileActivationView:
        async with self._database.transaction() as connection:
            user = await self._users.get_by_identity(
                connection,
                platform=platform,
                external_user_id=external_user_id,
            )
            outcome = await self._profiles.activate_primary(
                connection,
                profile_id=profile_id,
                user_id=user.id,
                expected_revision=expected_revision,
            )
            intent = build_profile_discovery_intent(outcome.profile)
            await self._discovery_intents.ensure(
                connection,
                intent,
            )
            from .telegram_chat_discovery import ensure_profile_derived_topics

            await ensure_profile_derived_topics(connection, intent)
            await self._jobs.enqueue(
                connection,
                job_type=TELEGRAM_PROFILE_DISCOVERY_JOB_TYPE,
                idempotency_key=profile_discovery_job_key(
                    outcome.profile.id,
                    outcome.profile.revision,
                ),
                max_attempts=3,
                correlation_id=outcome.profile.id,
            )
        view = await self.show(
            platform=platform,
            external_user_id=external_user_id,
            profile_id=outcome.profile.id,
        )
        return ProfileActivationView(
            profile=view,
            trial_started=outcome.trial_started,
        )

    async def deactivate(
        self,
        *,
        platform: str,
        external_user_id: str,
        profile_id: UUID,
        expected_revision: int,
    ) -> ProfileConfirmationView:
        async with self._database.transaction() as connection:
            user = await self._users.get_by_identity(
                connection,
                platform=platform,
                external_user_id=external_user_id,
            )
            profile = await self._profiles.deactivate(
                connection,
                profile_id=profile_id,
                user_id=user.id,
                expected_revision=expected_revision,
            )
        return await self.show(
            platform=platform,
            external_user_id=external_user_id,
            profile_id=profile.id,
        )

    async def list_profiles(
        self,
        *,
        platform: str,
        external_user_id: str,
    ) -> tuple[ProfileConfirmationView, ...]:
        async with self._database.connect() as connection:
            user = await self._users.get_by_identity(
                connection,
                platform=platform,
                external_user_id=external_user_id,
            )
            profiles = await self._profiles.list_for_user(
                connection,
                user_id=user.id,
            )
        return tuple(self._view(profile) for profile in profiles)

    async def set_work_types(
        self,
        *,
        platform: str,
        external_user_id: str,
        profile_id: UUID,
        work_types: tuple[OpportunityType, ...],
        expected_revision: int,
    ) -> ProfileConfirmationView:
        parsed = parse_search_profile_preferences(work_types=work_types)
        return await self._update_preferences(
            platform=platform,
            external_user_id=external_user_id,
            profile_id=profile_id,
            expected_revision=expected_revision,
            changes={"work_types": parsed.work_types},
        )

    async def toggle_work_type(
        self,
        *,
        platform: str,
        external_user_id: str,
        profile_id: UUID,
        work_type: OpportunityType,
        expected_revision: int,
    ) -> ProfileConfirmationView:
        async with self._database.transaction() as connection:
            user = await self._users.get_by_identity(
                connection,
                platform=platform,
                external_user_id=external_user_id,
            )
            current = await self._profiles.get(connection, profile_id)
            if current.user_id != user.id:
                raise SearchProfileOwnershipError(
                    "search profile belongs to another user"
                )
            selected = list(current.preferences.work_types or ())
            if work_type in selected:
                selected.remove(work_type)
            else:
                selected.append(work_type)
            preferences = replace(
                current.preferences,
                work_types=tuple(selected),
            )
            updated = await self._profiles.update_preferences(
                connection,
                profile_id=profile_id,
                user_id=user.id,
                preferences=preferences,
                expected_revision=expected_revision,
            )
        return await self.show(
            platform=platform,
            external_user_id=external_user_id,
            profile_id=updated.id,
        )

    async def set_budget(
        self,
        *,
        platform: str,
        external_user_id: str,
        profile_id: UUID,
        minimum_budget: Decimal | str | int | None,
        currency: str | None,
        budget_policy: BudgetPolicy,
        expected_revision: int,
    ) -> ProfileConfirmationView:
        parsed = parse_search_profile_preferences(
            minimum_budget=minimum_budget,
            currency=currency,
            budget_policy=budget_policy,
        )
        return await self._update_preferences(
            platform=platform,
            external_user_id=external_user_id,
            profile_id=profile_id,
            expected_revision=expected_revision,
            changes={
                "minimum_budget": parsed.minimum_budget,
                "currency": parsed.currency,
                "budget_policy": parsed.budget_policy,
            },
        )

    async def set_term_preferences(
        self,
        *,
        platform: str,
        external_user_id: str,
        profile_id: UUID,
        field: str,
        values: tuple[str, ...],
        expected_revision: int,
    ) -> ProfileConfirmationView:
        if field not in {"languages", "geographies", "excluded_categories"}:
            raise ValueError("unsupported term preference field")
        parsed = parse_search_profile_preferences(**{field: values})
        return await self._update_preferences(
            platform=platform,
            external_user_id=external_user_id,
            profile_id=profile_id,
            expected_revision=expected_revision,
            changes={field: getattr(parsed, field)},
        )

    async def set_work_modes(
        self,
        *,
        platform: str,
        external_user_id: str,
        profile_id: UUID,
        work_modes: tuple[WorkMode, ...],
        expected_revision: int,
    ) -> ProfileConfirmationView:
        parsed = parse_search_profile_preferences(work_modes=work_modes)
        return await self._update_preferences(
            platform=platform,
            external_user_id=external_user_id,
            profile_id=profile_id,
            expected_revision=expected_revision,
            changes={"work_modes": parsed.work_modes},
        )

    async def _update_preferences(
        self,
        *,
        platform: str,
        external_user_id: str,
        profile_id: UUID,
        expected_revision: int,
        changes: dict[str, object],
    ) -> ProfileConfirmationView:
        async with self._database.transaction() as connection:
            user = await self._users.get_by_identity(
                connection,
                platform=platform,
                external_user_id=external_user_id,
            )
            current = await self._profiles.get(connection, profile_id)
            if current.user_id != user.id:
                raise SearchProfileOwnershipError(
                    "search profile belongs to another user"
                )
            preferences = replace(current.preferences, **changes)
            updated = await self._profiles.update_preferences(
                connection,
                profile_id=profile_id,
                user_id=user.id,
                preferences=preferences,
                expected_revision=expected_revision,
            )
        return await self.show(
            platform=platform,
            external_user_id=external_user_id,
            profile_id=updated.id,
        )

    @staticmethod
    def _view(
        profile: SearchProfileRecord,
        *,
        uncertain_terms: tuple[str, ...] = (),
    ) -> ProfileConfirmationView:
        missing = tuple(
            field
            for field in ("roles", "skills", "categories")
            if not getattr(profile, field)
        )
        return ProfileConfirmationView(
            profile=profile,
            missing_fields=missing,
            uncertain_terms=uncertain_terms,
        )


def format_profile_summary(view: ProfileConfirmationView) -> str:
    profile = view.profile
    lines = [
        "<b>Профиль поиска</b>",
        f"Роли: {_format_terms(profile.roles)}",
        f"Навыки: {_format_terms(profile.skills)}",
        f"Категории: {_format_terms(profile.categories)}",
        f"Типы работы: {_format_work_types(profile.preferences)}",
        f"Бюджет: {_format_budget(profile.preferences)}",
        f"Языки: {_format_optional_terms(profile.preferences.languages)}",
        f"География: {_format_optional_terms(profile.preferences.geographies)}",
        f"Формат: {_format_work_modes(profile.preferences)}",
        "Исключения: "
        f"{_format_optional_terms(profile.preferences.excluded_categories)}",
    ]
    if view.missing_fields:
        labels = ", ".join(_field_label(field) for field in view.missing_fields)
        lines.append(f"Не указано: {labels}")
    if view.uncertain_terms:
        uncertain = ", ".join(escape(term) for term in view.uncertain_terms)
        lines.append(f"Нужно проверить: {uncertain}")
    if profile.confirmation_status is SearchProfileConfirmationStatus.DRAFT:
        lines.append(
            "Статус: проверьте данные перед подтверждением"
        )
    elif profile.is_active and profile.is_primary:
        lines.append("Статус: поиск активен, основной профиль")
    elif profile.is_active:
        lines.append("Статус: поиск активен")
    elif profile.deactivated_at is not None:
        lines.append("Статус: поиск остановлен")
    else:
        lines.append(
            "Статус: подтверждено, поиск ещё не активирован"
        )
    return "\n".join(lines)


def _preserved_terms(
    terms: tuple[SearchProfileTerm, ...],
) -> tuple[SearchProfileTermInput, ...]:
    return tuple(
        SearchProfileTermInput(
            value=term.value,
            evidence=term.evidence or term.value,
            origin=term.origin,
        )
        for term in terms
    )


def _format_terms(terms: tuple[SearchProfileTerm, ...]) -> str:
    if not terms:
        return "не указано"
    return ", ".join(escape(term.value) for term in terms)


def _field_label(field: str) -> str:
    return {
        "roles": "роли",
        "skills": "навыки",
        "categories": "категории",
    }[field]


def _format_work_types(preferences: SearchProfilePreferences) -> str:
    if preferences.work_types is None:
        return "не указано"
    if not preferences.work_types:
        return "нет"
    labels = {
        OpportunityType.ONE_OFF_ORDER: "разовые заказы",
        OpportunityType.PROJECT: "проекты",
        OpportunityType.VACANCY: "вакансии",
        OpportunityType.PART_TIME_CONTRACTOR: (
            "частичная занятость/контракт"
        ),
    }
    return ", ".join(labels[value] for value in preferences.work_types)


def _format_budget(preferences: SearchProfilePreferences) -> str:
    parts: list[str] = []
    if preferences.minimum_budget is not None:
        parts.append(f"от {preferences.minimum_budget} {preferences.currency}")
    if preferences.budget_policy is BudgetPolicy.REQUIRE_EXPLICIT:
        parts.append("только с указанным бюджетом")
    elif preferences.budget_policy is BudgetPolicy.ALLOW_UNKNOWN:
        parts.append("можно без указанного бюджета")
    return ", ".join(parts) if parts else "не указано"


def _format_optional_terms(
    terms: tuple[SearchProfileTerm, ...] | None,
) -> str:
    if terms is None:
        return "не указано"
    return _format_terms(terms) if terms else "нет"


def _format_work_modes(preferences: SearchProfilePreferences) -> str:
    if preferences.work_modes is None:
        return "не указано"
    if not preferences.work_modes:
        return "нет"
    labels = {
        WorkMode.REMOTE: "удалённо",
        WorkMode.HYBRID: "гибрид",
        WorkMode.ON_SITE: "на месте",
    }
    return ", ".join(labels[value] for value in preferences.work_modes)
