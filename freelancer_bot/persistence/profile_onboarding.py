from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncConnection

from ..profile_onboarding import OnboardingProfileProviderMetrics
from .schema import search_profile_onboarding_attempts


@dataclass(frozen=True)
class ProfileOnboardingAttemptRecord:
    id: UUID
    platform: str
    external_user_id: str
    input_sha256: str
    cache_version: str
    provider: str
    requested_model: str
    status: str
    retryable: bool
    metrics: OnboardingProfileProviderMetrics
    error_code: str | None
    created_at: datetime
    finished_at: datetime


class ProfileOnboardingAttemptRepository:
    async def record(
        self,
        connection: AsyncConnection,
        *,
        platform: str,
        external_user_id: str,
        input_sha256: str,
        cache_version: str,
        provider: str,
        requested_model: str,
        status: str,
        retryable: bool,
        metrics: OnboardingProfileProviderMetrics,
        error_code: str | None = None,
    ) -> UUID:
        attempt_id = uuid4()
        await connection.execute(
            search_profile_onboarding_attempts.insert().values(
                id=attempt_id,
                platform=platform,
                external_user_id=external_user_id,
                input_sha256=input_sha256,
                cache_version=cache_version,
                provider=provider,
                requested_model=requested_model,
                status=status,
                retryable=retryable,
                provider_attempts=metrics.provider_attempts,
                completed_calls=metrics.completed_calls,
                timeout_count=metrics.timeouts,
                transient_failure_count=metrics.transient_failures,
                non_retryable_failure_count=metrics.non_retryable_failures,
                invalid_output_retry_count=metrics.invalid_output_retries,
                error_code=error_code,
            )
        )
        return attempt_id
