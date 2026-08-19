from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from uuid import UUID

from .persistence.database import Database
from .persistence.search_profiles import (
    SearchProfileAnalysisCacheRecord,
    SearchProfileAnalysisCacheRepository,
    SearchProfileRecord,
    SearchProfileRepository,
    UserRecord,
    UserRepository,
)
from .persistence.profile_onboarding import ProfileOnboardingAttemptRepository
from .profile_onboarding import (
    OnboardingProfileAnalysis,
    OnboardingProfileAnalysisCall,
    OnboardingProfileError,
    OnboardingProfileProviderMetrics,
    OnboardingProfileAnalyzer,
    onboarding_profile_cache_version,
    parsed_profile_from_analysis,
)
from .search_profiles import normalize_semantic_text


@dataclass(frozen=True)
class ProfileOnboardingOutcome:
    user: UserRecord
    profile: SearchProfileRecord
    analysis: OnboardingProfileAnalysis
    analysis_cache: SearchProfileAnalysisCacheRecord
    user_created: bool
    profile_created: bool
    model_invoked: bool
    cache_created: bool


class ProfileOnboardingService:
    def __init__(
        self,
        database: Database,
        analyzer: OnboardingProfileAnalyzer,
        *,
        users: UserRepository | None = None,
        profiles: SearchProfileRepository | None = None,
        cache: SearchProfileAnalysisCacheRepository | None = None,
        attempts: ProfileOnboardingAttemptRepository | None = None,
    ) -> None:
        self._database = database
        self._analyzer = analyzer
        self._users = users or UserRepository()
        self._profiles = profiles or SearchProfileRepository()
        self._cache = cache or SearchProfileAnalysisCacheRepository()
        self._attempts = attempts or ProfileOnboardingAttemptRepository()

    async def create_from_description(
        self,
        *,
        platform: str,
        external_user_id: str,
        description: str,
        profile_id: UUID | None = None,
    ) -> ProfileOnboardingOutcome:
        normalized_input = normalize_semantic_text(description)
        input_sha256 = sha256(description.encode("utf-8")).hexdigest()
        cache_version = onboarding_profile_cache_version(self._analyzer)

        async with self._database.connect() as connection:
            cached = await self._cache.get_by_identity(
                connection,
                input_sha256=input_sha256,
                cache_version=cache_version,
            )

        model_invoked = cached is None
        cache_created = False
        if cached is None:
            try:
                call = await self._analyzer.analyze(description)
            except OnboardingProfileError as exc:
                async with self._database.transaction() as connection:
                    await self._attempts.record(
                        connection,
                        platform=platform,
                        external_user_id=external_user_id,
                        input_sha256=input_sha256,
                        cache_version=cache_version,
                        provider=self._analyzer.provider,
                        requested_model=self._analyzer.model,
                        status="failed",
                        retryable=exc.retryable,
                        metrics=exc.provider_metrics,
                        error_code=exc.failure_class,
                    )
                raise
            _validate_call(self._analyzer, call)
            async with self._database.transaction() as connection:
                cache_outcome = await self._cache.record(
                    connection,
                    input_sha256=input_sha256,
                    original_input_text=description,
                    normalized_input_text=normalized_input,
                    cache_version=cache_version,
                    call=call,
                )
            cached = cache_outcome.record
            cache_created = cache_outcome.created

        parsed = parsed_profile_from_analysis(description, cached.analysis)
        async with self._database.transaction() as connection:
            user_outcome = await self._users.ensure(
                connection,
                platform=platform,
                external_user_id=external_user_id,
            )
            profile_outcome = await self._profiles.create(
                connection,
                user_id=user_outcome.user.id,
                parsed_profile=parsed,
                profile_id=profile_id,
                analysis_cache_id=cached.id,
            )
            await self._attempts.record(
                connection,
                platform=platform,
                external_user_id=external_user_id,
                input_sha256=input_sha256,
                cache_version=cache_version,
                provider=call.provider if model_invoked else cached.provider,
                requested_model=(
                    call.requested_model if model_invoked else cached.requested_model
                ),
                status="succeeded",
                retryable=False,
                metrics=(
                    _effective_metrics(call)
                    if model_invoked
                    else OnboardingProfileProviderMetrics(
                        provider_attempts=0,
                        completed_calls=1,
                    )
                ),
            )
        return ProfileOnboardingOutcome(
            user=user_outcome.user,
            profile=profile_outcome.profile,
            analysis=cached.analysis,
            analysis_cache=cached,
            user_created=user_outcome.created,
            profile_created=profile_outcome.created,
            model_invoked=model_invoked,
            cache_created=cache_created,
        )


def _validate_call(
    analyzer: OnboardingProfileAnalyzer,
    call: OnboardingProfileAnalysisCall,
) -> None:
    expected = (
        analyzer.provider,
        analyzer.model,
        analyzer.analyzer_version,
        analyzer.prompt_version,
        analyzer.schema_version,
    )
    actual = (
        call.provider,
        call.requested_model,
        call.analyzer_version,
        call.prompt_version,
        call.schema_version,
    )
    if actual != expected:
        raise ValueError("onboarding analyzer returned incompatible call metadata")


def _effective_metrics(
    call: OnboardingProfileAnalysisCall,
) -> OnboardingProfileProviderMetrics:
    metrics = call.provider_metrics
    return OnboardingProfileProviderMetrics(
        provider_attempts=max(1, metrics.provider_attempts),
        completed_calls=max(1, metrics.completed_calls),
        timeouts=metrics.timeouts,
        transient_failures=metrics.transient_failures,
        non_retryable_failures=metrics.non_retryable_failures,
        invalid_output_retries=max(metrics.invalid_output_retries, call.attempt_count - 1),
    )
