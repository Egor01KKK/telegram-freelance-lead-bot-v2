from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from .profile_confirmation import (
    EDITABLE_PROFILE_FIELDS,
    ProfileConfirmationService,
    ProfileConfirmationView,
    format_profile_summary,
)
from .profile_onboarding import OnboardingProfileError
from .profile_onboarding_service import ProfileOnboardingOutcome
from .search_profiles import BudgetPolicy, OpportunityType, WorkMode


@dataclass(frozen=True)
class TelegramButtonSpec:
    label: str
    data: bytes


@dataclass(frozen=True)
class TelegramOnboardingResponse:
    text: str
    buttons: tuple[tuple[TelegramButtonSpec, ...], ...] = ()
    retryable: bool = False


class AIProfileOnboarding(Protocol):
    async def create_from_description(
        self,
        *,
        platform: str,
        external_user_id: str,
        description: str,
    ) -> ProfileOnboardingOutcome: ...


class TelegramProfileOnboarding:
    def __init__(
        self,
        confirmation: ProfileConfirmationService,
        ai_onboarding: AIProfileOnboarding | None,
    ) -> None:
        self._confirmation = confirmation
        self._ai_onboarding = ai_onboarding

    async def begin(
        self,
        *,
        external_user_id: str,
        description: str | None,
    ) -> TelegramOnboardingResponse:
        if description is None or not description.strip():
            return TelegramOnboardingResponse(
                "Опишите, кем вы работаете "
                "и какие задачи ищете:\n"
                "/profile ваш текст\n\n"
                + _manual_help()
            )
        if self._ai_onboarding is None:
            return _ai_unavailable_response()
        try:
            outcome = await self._ai_onboarding.create_from_description(
                platform="telegram",
                external_user_id=external_user_id,
                description=description,
            )
        except OnboardingProfileError:
            return _ai_unavailable_response()
        view = await self._confirmation.show(
            platform="telegram",
            external_user_id=external_user_id,
            profile_id=outcome.profile.id,
        )
        return _profile_response(view)

    async def create_manual(
        self,
        *,
        external_user_id: str,
        payload: str,
    ) -> TelegramOnboardingResponse:
        roles, skills, categories = parse_manual_profile_payload(payload)
        view = await self._confirmation.create_manual_draft(
            platform="telegram",
            external_user_id=external_user_id,
            semantic_text=payload,
            roles=roles,
            skills=skills,
            categories=categories,
        )
        return _profile_response(view)

    async def show(
        self,
        *,
        external_user_id: str,
        profile_id: UUID,
    ) -> TelegramOnboardingResponse:
        view = await self._confirmation.show(
            platform="telegram",
            external_user_id=external_user_id,
            profile_id=profile_id,
        )
        return _profile_response(view)

    async def edit(
        self,
        *,
        external_user_id: str,
        profile_id: UUID,
        field: str,
        values: str,
        expected_revision: int,
    ) -> TelegramOnboardingResponse:
        if field not in EDITABLE_PROFILE_FIELDS:
            raise ValueError("unknown editable profile field")
        view = await self._confirmation.edit_terms(
            platform="telegram",
            external_user_id=external_user_id,
            profile_id=profile_id,
            field=field,
            values=parse_term_list(values),
            expected_revision=expected_revision,
        )
        return _profile_response(view)

    async def confirm(
        self,
        *,
        external_user_id: str,
        profile_id: UUID,
        expected_revision: int,
    ) -> TelegramOnboardingResponse:
        view = await self._confirmation.confirm(
            platform="telegram",
            external_user_id=external_user_id,
            profile_id=profile_id,
            expected_revision=expected_revision,
        )
        return _profile_response(view)

    async def activate(
        self,
        *,
        external_user_id: str,
        profile_id: UUID,
        expected_revision: int,
    ) -> TelegramOnboardingResponse:
        outcome = await self._confirmation.activate(
            platform="telegram",
            external_user_id=external_user_id,
            profile_id=profile_id,
            expected_revision=expected_revision,
        )
        response = _profile_response(outcome.profile)
        if not outcome.trial_started:
            return response
        return TelegramOnboardingResponse(
            response.text + "\n\nПробный период начался.",
            response.buttons,
        )

    async def deactivate(
        self,
        *,
        external_user_id: str,
        profile_id: UUID,
        expected_revision: int,
    ) -> TelegramOnboardingResponse:
        view = await self._confirmation.deactivate(
            platform="telegram",
            external_user_id=external_user_id,
            profile_id=profile_id,
            expected_revision=expected_revision,
        )
        return _profile_response(view)

    async def toggle_work_type(
        self,
        *,
        external_user_id: str,
        profile_id: UUID,
        work_type: OpportunityType,
        expected_revision: int,
    ) -> TelegramOnboardingResponse:
        view = await self._confirmation.toggle_work_type(
            platform="telegram",
            external_user_id=external_user_id,
            profile_id=profile_id,
            work_type=work_type,
            expected_revision=expected_revision,
        )
        return _profile_response(view)

    async def edit_setting(
        self,
        *,
        external_user_id: str,
        profile_id: UUID,
        field: str,
        value: str,
        expected_revision: int,
    ) -> TelegramOnboardingResponse:
        common = {
            "platform": "telegram",
            "external_user_id": external_user_id,
            "profile_id": profile_id,
            "expected_revision": expected_revision,
        }
        if field == "work_types":
            view = await self._confirmation.set_work_types(
                work_types=tuple(
                    OpportunityType(item) for item in parse_term_list(value)
                ),
                **common,
            )
        elif field == "budget":
            amount, currency, policy = parse_budget_setting(value)
            view = await self._confirmation.set_budget(
                minimum_budget=amount,
                currency=currency,
                budget_policy=policy,
                **common,
            )
        elif field in {"languages", "geography", "exclusions"}:
            preference_field = {
                "languages": "languages",
                "geography": "geographies",
                "exclusions": "excluded_categories",
            }[field]
            view = await self._confirmation.set_term_preferences(
                field=preference_field,
                values=parse_term_list(value),
                **common,
            )
        elif field == "work_modes":
            view = await self._confirmation.set_work_modes(
                work_modes=tuple(
                    WorkMode(item) for item in parse_term_list(value)
                ),
                **common,
            )
        else:
            raise ValueError("unknown search setting")
        return _profile_response(view)


def parse_manual_profile_payload(
    payload: str,
) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    if not isinstance(payload, str):
        raise TypeError("manual profile payload must be a string")
    parts = tuple(part.strip() for part in payload.split("|"))
    if len(parts) != 3:
        raise ValueError("manual profile requires roles | skills | categories")
    roles, skills, categories = tuple(parse_term_list(part) for part in parts)
    if not roles and not skills and not categories:
        raise ValueError("manual profile must contain at least one value")
    return roles, skills, categories


def parse_term_list(value: str) -> tuple[str, ...]:
    normalized = value.strip()
    if normalized in {"", "-"}:
        return ()
    terms = tuple(term.strip() for term in normalized.split(","))
    if any(not term for term in terms):
        raise ValueError("profile values must not contain empty entries")
    return terms


def parse_budget_setting(
    value: str,
) -> tuple[str | None, str | None, BudgetPolicy]:
    parts = tuple(part.strip() for part in value.split(","))
    if len(parts) != 3:
        raise ValueError("budget requires amount,currency,policy")
    amount, currency, policy = parts
    if (amount == "-") != (currency == "-"):
        raise ValueError("budget amount and currency must both be set or '-'")
    return (
        None if amount == "-" else amount,
        None if currency == "-" else currency,
        BudgetPolicy(policy),
    )


def _profile_response(view: ProfileConfirmationView) -> TelegramOnboardingResponse:
    profile = view.profile
    if profile.confirmation_status.value == "confirmed":
        action = "deactivate" if profile.is_active else "activate"
        label = "Остановить поиск" if profile.is_active else "Активировать поиск"
        return TelegramOnboardingResponse(
            format_profile_summary(view),
            (
                (
                    TelegramButtonSpec(
                        label,
                        f"profile:{action}:{profile.id}:{profile.revision}".encode(
                            "ascii"
                        ),
                    ),
                ),
            ),
        )
    revision = profile.revision
    profile_id = profile.id
    buttons = (
        (
            TelegramButtonSpec(
                "Подтвердить",
                f"profile:confirm:{profile_id}:{revision}".encode("ascii"),
            ),
        ),
        tuple(
            TelegramButtonSpec(
                label,
                f"profile:edit:{profile_id}:{field}:{revision}".encode("ascii"),
            )
            for field, label in (
                ("roles", "Изменить роли"),
                ("skills", "Изменить навыки"),
                ("categories", "Изменить категории"),
            )
        ),
        tuple(
            TelegramButtonSpec(
                _work_type_button_label(profile, work_type, label),
                f"pwt:{profile_id}:{code}:{revision}".encode("ascii"),
            )
            for work_type, code, label in (
                (OpportunityType.ONE_OFF_ORDER, "o", "Заказы"),
                (OpportunityType.PROJECT, "p", "Проекты"),
            )
        ),
        tuple(
            TelegramButtonSpec(
                _work_type_button_label(profile, work_type, label),
                f"pwt:{profile_id}:{code}:{revision}".encode("ascii"),
            )
            for work_type, code, label in (
                (OpportunityType.VACANCY, "v", "Вакансии"),
                (
                    OpportunityType.PART_TIME_CONTRACTOR,
                    "c",
                    "Частичная/контракт",
                ),
            )
        ),
        (
            TelegramButtonSpec(
                "Другие настройки",
                f"pset:{profile_id}:{revision}".encode("ascii"),
            ),
        ),
    )
    return TelegramOnboardingResponse(format_profile_summary(view), buttons)


def _ai_unavailable_response() -> TelegramOnboardingResponse:
    return TelegramOnboardingResponse(
        "Не удалось обработать описание через AI. Попробуйте отправить "
        "описание ещё раз.\n\n"
        "Для явного ручного заполнения используйте команду:\n"
        + _manual_help(),
        retryable=True,
    )


def _manual_help() -> str:
    return (
        "Формат: /profile_manual роли | навыки | категории\n"
        "Несколько значений разделяйте запятыми, "
        "пустое поле "
        "обозначьте знаком -."
    )


def _work_type_button_label(profile, work_type, label: str) -> str:
    selected = work_type in (profile.preferences.work_types or ())
    return f"{'[x]' if selected else '[ ]'} {label}"


def settings_help(profile_id: UUID, revision: int) -> str:
    prefix = f"/profile_setting {profile_id} {revision}"
    return (
        "Изменяйте настройки одной командой:\n"
        f"{prefix} work_types one_off_order,project,vacancy,"
        "part_time_contractor\n"
        f"{prefix} budget 80000,RUB,allow_unknown\n"
        f"{prefix} budget -,-,require_explicit\n"
        f"{prefix} languages ru,en\n"
        f"{prefix} geography Россия,Москва\n"
        f"{prefix} work_modes remote,hybrid,on_site\n"
        f"{prefix} exclusions gambling,adult\n"
        "Знак '-' задаёт явно пустой список."
    )


WORK_TYPE_CALLBACK_CODES = {
    "o": OpportunityType.ONE_OFF_ORDER,
    "p": OpportunityType.PROJECT,
    "v": OpportunityType.VACANCY,
    "c": OpportunityType.PART_TIME_CONTRACTOR,
}
