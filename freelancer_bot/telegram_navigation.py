from __future__ import annotations

from datetime import datetime, timezone
from html import escape
from uuid import UUID

from .billing import (
    DEFAULT_TRIAL_POLICY,
    BillingPlan,
    EntitlementState,
    evaluate_trial_entitlement,
)
from .persistence.search_profiles import (
    SearchProfileConfirmationStatus,
    UserNotFound,
)
from .profile_confirmation import (
    ProfileConfirmationService,
    ProfileConfirmationView,
    format_profile_summary,
)
from .search_profiles import OpportunityType
from .telegram_onboarding import TelegramButtonSpec, TelegramOnboardingResponse


MAIN_MENU_LABELS = ("Мой поиск", "Настройки", "Подписка")
HOME_LABEL = "Главное меню"
CANCEL_LABEL = "Отмена"

SETTING_FIELD_CODES = {
    "roles": "roles",
    "skills": "skills",
    "categories": "categories",
    "budget": "budget",
    "languages": "lang",
    "geographies": "geo",
    "work_modes": "mode",
    "excluded_categories": "exclude",
}
SETTING_FIELDS_BY_CODE = {
    value: key for key, value in SETTING_FIELD_CODES.items()
}
SETTING_FIELD_LABELS = {
    "roles": "Роли",
    "skills": "Навыки",
    "categories": "Категории",
    "budget": "Бюджет",
    "languages": "Языки",
    "geographies": "География",
    "work_modes": "Формат работы",
    "excluded_categories": "Исключения",
}

_WORK_TYPE_OPTIONS = (
    (OpportunityType.ONE_OFF_ORDER, "o", "Заказы"),
    (OpportunityType.PROJECT, "p", "Проекты"),
    (OpportunityType.VACANCY, "v", "Вакансии"),
    (OpportunityType.PART_TIME_CONTRACTOR, "c", "Частичная/контракт"),
)


class TelegramNavigationService:
    """Builds Telegram navigation states from PostgreSQL-backed V2 state."""

    def __init__(
        self,
        confirmation: ProfileConfirmationService,
        *,
        billing_plan: BillingPlan | None = None,
    ) -> None:
        self._confirmation = confirmation
        self._billing_plan = billing_plan or BillingPlan()

    def home(self) -> TelegramOnboardingResponse:
        return TelegramOnboardingResponse(
            "<b>Главное меню</b>\n\nВыберите раздел:",
            (
                (
                    _button("Мой поиск", b"nav:search"),
                    _button("Настройки", b"nav:settings"),
                ),
                (_button("Подписка", b"nav:subscription"),),
            ),
        )

    async def my_search(
        self,
        *,
        external_user_id: str,
    ) -> TelegramOnboardingResponse:
        profiles = await self._profiles(external_user_id)
        if not profiles:
            return TelegramOnboardingResponse(
                "<b>Мой поиск</b>\n\n"
                "Пока нет сохранённых поисков. Создайте первый поиск "
                "одним сообщением.",
                (
                    (_button("Создать поиск", b"nav:profile:new"),),
                    (_button(HOME_LABEL, b"nav:home"),),
                ),
            )

        rows = tuple(
            (
                _button(
                    f"Поиск {index}: {_profile_state(view)}",
                    f"nav:profile:{view.profile.id}".encode("ascii"),
                ),
            )
            for index, view in enumerate(profiles, 1)
        )
        return TelegramOnboardingResponse(
            "<b>Мой поиск</b>\n\nВыберите профиль, чтобы посмотреть его состояние.",
            rows
            + (
                (_button("Создать поиск", b"nav:profile:new"),),
                (_button(HOME_LABEL, b"nav:home"),),
            ),
        )

    async def settings(
        self,
        *,
        external_user_id: str,
    ) -> TelegramOnboardingResponse:
        profiles = await self._profiles(external_user_id)
        if not profiles:
            return TelegramOnboardingResponse(
                "<b>Настройки</b>\n\n"
                "Сначала создайте профиль поиска, чтобы настроить его.",
                (
                    (_button("Создать поиск", b"nav:profile:new"),),
                    (_button(HOME_LABEL, b"nav:home"),),
                ),
            )

        rows = tuple(
            (
                _button(
                    f"Поиск {index}: {_profile_state(view)}",
                    (
                        f"nav:settings:{view.profile.id}:"
                        f"{view.profile.revision}"
                    ).encode("ascii"),
                ),
            )
            for index, view in enumerate(profiles, 1)
        )
        return TelegramOnboardingResponse(
            "<b>Настройки</b>\n\nВыберите поиск, который хотите изменить.",
            rows + ((_button(HOME_LABEL, b"nav:home"),),),
        )

    async def profile(
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
        profile = view.profile
        rows: list[tuple[TelegramButtonSpec, ...]] = [
            (
                _button(
                    "Настройки поиска",
                    (
                        f"nav:settings:{profile.id}:"
                        f"{profile.revision}"
                    ).encode("ascii"),
                ),
            )
        ]
        if profile.confirmation_status is SearchProfileConfirmationStatus.DRAFT:
            rows.append(
                (
                    _button(
                        "Подтвердить",
                        (
                            f"nav:confirm:{profile.id}:"
                            f"{profile.revision}"
                        ).encode("ascii"),
                    ),
                )
            )
        else:
            action = "deactivate" if profile.is_active else "activate"
            label = "Остановить поиск" if profile.is_active else "Активировать поиск"
            rows.append(
                (
                    _button(
                        label,
                        (
                            f"nav:{action}:{profile.id}:"
                            f"{profile.revision}"
                        ).encode("ascii"),
                    ),
                )
            )
        rows.append((_button("Мой поиск", b"nav:search"),))
        return TelegramOnboardingResponse(
            f"<b>Поиск</b>\n\n{format_profile_summary(view)}",
            tuple(rows),
        )

    async def settings_for_profile(
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
        profile = view.profile
        rows: list[tuple[TelegramButtonSpec, ...]] = []
        if profile.confirmation_status is SearchProfileConfirmationStatus.DRAFT:
            rows.extend(
                (
                    _button(
                        SETTING_FIELD_LABELS[field],
                        (
                            f"nav:edit:{profile.id}:{profile.revision}:"
                            f"{SETTING_FIELD_CODES[field]}"
                        ).encode("ascii"),
                    ),
                )
                for field in ("roles", "skills", "categories")
            )

        rows.extend(
            (
                _work_type_button(profile, first),
                _work_type_button(profile, second),
            )
            for first, second in (
                _WORK_TYPE_OPTIONS[:2],
                _WORK_TYPE_OPTIONS[2:],
            )
        )
        rows.extend(
            (
                _setting_button(profile, first),
                _setting_button(profile, second),
            )
            for first, second in (
                ("budget", "languages"),
                ("geographies", "work_modes"),
            )
        )
        rows.append((_setting_button(profile, "excluded_categories"),))

        if profile.confirmation_status is SearchProfileConfirmationStatus.DRAFT:
            rows.append(
                (
                    _button(
                        "Подтвердить",
                        (
                            f"nav:confirm:{profile.id}:"
                            f"{profile.revision}"
                        ).encode("ascii"),
                    ),
                )
            )
        else:
            action = "deactivate" if profile.is_active else "activate"
            label = "Остановить поиск" if profile.is_active else "Активировать поиск"
            rows.append(
                (
                    _button(
                        label,
                        (
                            f"nav:{action}:{profile.id}:"
                            f"{profile.revision}"
                        ).encode("ascii"),
                    ),
                )
            )
        rows.append(
            (
                _button("Назад к поиску", f"nav:profile:{profile.id}".encode("ascii")),
            )
        )
        return TelegramOnboardingResponse(
            "<b>Настройки поиска</b>\n\n"
            f"{format_profile_summary(view)}\n\n"
            "Выберите параметр. После нажатия отправьте новое значение "
            "обычным сообщением.",
            tuple(rows),
        )

    async def subscription(
        self,
        *,
        external_user_id: str,
    ) -> TelegramOnboardingResponse:
        try:
            user = await self._confirmation.get_user(
                platform="telegram",
                external_user_id=external_user_id,
            )
        except UserNotFound:
            trial_text = "Пробный период ещё не начался."
        else:
            trial_text = _trial_status(
                user.trial_started_at,
                user.trial_expires_at,
                trial_policy_version=user.trial_policy_version,
            )
        return TelegramOnboardingResponse(
            "<b>Подписка</b>\n\n"
            f"{trial_text}\n\n"
            f"Тариф: {self._billing_plan.price_label}.\n"
            "Платная подписка пока не подключена в V2.",
            (
                (_button("Мой поиск", b"nav:search"),),
                (_button(HOME_LABEL, b"nav:home"),),
            ),
        )

    async def _profiles(
        self,
        external_user_id: str,
    ) -> tuple[ProfileConfirmationView, ...]:
        try:
            return await self._confirmation.list_profiles(
                platform="telegram",
                external_user_id=external_user_id,
            )
        except UserNotFound:
            return ()


def setting_field_from_code(code: str) -> str:
    try:
        return SETTING_FIELDS_BY_CODE[code]
    except KeyError as exc:
        raise ValueError("unknown navigation setting") from exc


def setting_prompt(code: str) -> str:
    field = setting_field_from_code(code)
    label = SETTING_FIELD_LABELS[field]
    examples = {
        "roles": "Python-разработчик, продуктовый дизайнер",
        "skills": "Python, Telethon",
        "categories": "Telegram, SaaS",
        "budget": "80000,RUB,allow_unknown или -,-,require_explicit",
        "languages": "ru,en или -",
        "geographies": "Россия, Москва или -",
        "work_modes": "remote,hybrid или -",
        "excluded_categories": "gambling,adult или -",
    }
    return (
        f"<b>{escape(label)}</b>\n\n"
        "Отправьте новое значение обычным сообщением.\n"
        f"Пример: <code>{escape(examples[field])}</code>"
    )


def new_profile_prompt() -> str:
    return (
        "<b>Новый поиск</b>\n\n"
        "Опишите обычным сообщением, кем вы работаете и какие задачи "
        "ищете. AI подготовит черновик профиля для проверки.\n\n"
        "Для явного ручного заполнения используйте команду:\n"
        "/profile_manual роли | навыки | категории"
    )


def _button(label: str, data: bytes) -> TelegramButtonSpec:
    if len(data) > 64:
        raise ValueError("Telegram navigation callback exceeds 64 bytes")
    return TelegramButtonSpec(label, data)


def _profile_state(view: ProfileConfirmationView) -> str:
    profile = view.profile
    if profile.confirmation_status is SearchProfileConfirmationStatus.DRAFT:
        return "черновик"
    if profile.is_active and profile.is_primary:
        return "активен, основной"
    if profile.is_active:
        return "активен"
    if profile.deactivated_at is not None:
        return "остановлен"
    return "подтверждён"


def _work_type_button(profile, option) -> TelegramButtonSpec:
    work_type, code, label = option
    selected = work_type in (profile.preferences.work_types or ())
    return _button(
        f"{'[x]' if selected else '[ ]'} {label}",
        (
            f"nav:toggle:{profile.id}:{code}:"
            f"{profile.revision}"
        ).encode("ascii"),
    )


def _setting_button(profile, field: str) -> TelegramButtonSpec:
    return _button(
        SETTING_FIELD_LABELS[field],
        (
            f"nav:edit:{profile.id}:{profile.revision}:"
            f"{SETTING_FIELD_CODES[field]}"
        ).encode("ascii"),
    )


def _trial_status(
    trial_started_at: datetime | None,
    trial_expires_at: datetime | None = None,
    *,
    trial_policy_version: str | None = None,
) -> str:
    if trial_started_at is None:
        return "Пробный период ещё не начался."
    started = trial_started_at
    if started.tzinfo is None:
        started = started.replace(tzinfo=timezone.utc)
    started = started.astimezone(timezone.utc)
    decision = evaluate_trial_entitlement(
        started,
        trial_expires_at=trial_expires_at,
        trial_policy_version=trial_policy_version,
        evaluated_at=datetime.now(timezone.utc),
        policy=DEFAULT_TRIAL_POLICY,
    )
    if decision.state is EntitlementState.TRIAL_EXPIRED:
        return (
            "Пробный период завершён: "
            f"{decision.trial_expires_at:%Y-%m-%d %H:%M UTC}. "
            "Новые лиды доступны после оформления подписки."
        )
    return (
        "Пробный период активирован: "
        f"{started:%Y-%m-%d %H:%M UTC}; действует до "
        f"{decision.trial_expires_at:%Y-%m-%d %H:%M UTC}."
    )
