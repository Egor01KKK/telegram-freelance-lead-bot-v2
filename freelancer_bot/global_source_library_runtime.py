"""Durable, restart-safe handlers for library planning and profile coverage."""

from __future__ import annotations

import re
from uuid import UUID

from .config import RuntimeConfig
from .persistence.database import Database
from .persistence.discovery_campaigns import DiscoveryCampaignRepository
from .persistence.jobs import JobClaim
from .persistence.search_profiles import SearchProfileRepository
from .profile_discovery import ProfileDiscoveryService


class ProfileCoverageRecheckProcessor:
    def __init__(self, database: Database) -> None:
        self._database = database

    async def __call__(self, claim: JobClaim) -> None:
        match = re.fullmatch(r"profile:([0-9a-f-]{36}):revision:(\d+)", claim.idempotency_key)
        if match is None:
            raise ValueError("invalid profile coverage job identity")
        profile_id = UUID(match.group(1))
        async with self._database.connect() as connection:
            profile = await SearchProfileRepository().get(connection, profile_id)
        if profile is None or not profile.is_active:
            return
        await ProfileDiscoveryService(self._database).coverage_for_profile(profile)


class DiscoveryCampaignPlanProcessor:
    """Execute one bounded, restart-safe Web campaign plan.

    The durable job references only the campaign key.  The campaign/query rows
    remain the source of truth for the work batch, while DiscoveryRunner's
    deterministic run key makes a reclaimed lease safe to repeat.
    """

    def __init__(self, database: Database, config: RuntimeConfig | None = None) -> None:
        self._database = database
        self._config = config

    async def __call__(self, claim: JobClaim) -> None:
        prefix = "campaign:"
        if not claim.idempotency_key.startswith(prefix):
            raise ValueError("invalid discovery campaign plan identity")
        campaign_ref = claim.idempotency_key[len(prefix) :]
        campaign_key = campaign_ref.split(":batch:", 1)[0]
        async with self._database.connect() as connection:
            repository = DiscoveryCampaignRepository()
            campaign = await repository.get_by_key(
                connection,
                campaign_key,
            )
        if campaign is None:
            raise ValueError("discovery campaign plan references an unknown campaign")
        if campaign.status == "paused":
            return
        async with self._database.connect() as connection:
            pending = await repository.pending_query_count(
                connection,
                campaign_id=campaign.id,
                include_running=False,
            )
        if campaign.status == "completed" and pending == 0:
            return
        if self._config is None:
            raise RuntimeError("Global Source Library worker requires RuntimeConfig")
        from .source_bootstrap import GlobalSourceLibraryService

        await GlobalSourceLibraryService(self._database, self._config).run_campaign(
            campaign_key,
            max_queries=20,
            results_per_query=10,
            max_candidates=100,
            max_page_fetches=100,
        )
