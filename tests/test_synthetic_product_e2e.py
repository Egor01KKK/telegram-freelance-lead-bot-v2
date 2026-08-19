from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
from types import SimpleNamespace
import unittest
from uuid import UUID, uuid4

import sqlalchemy as sa
from telethon.errors import FloodWaitError
from telethon.tl.types import Channel

from freelancer_bot.ai_telemetry import AICallFinish, AICallStart, AIModelPrice
from freelancer_bot.config import RuntimeConfig
from freelancer_bot.delivery import (
    PersonalizedDeliveryJobProcessor,
    TelegramSendReceipt,
)
from freelancer_bot.matching_delivery import (
    MATCHING_DELIVERY_JOB_TYPE,
    MatchingDeliveryJobProcessor,
)
from freelancer_bot.message_prefilter import (
    OPPORTUNITY_ANALYSIS_JOB_TYPE,
    RawMessagePrefilterProcessor,
)
from freelancer_bot.opportunity_analysis import (
    OPPORTUNITY_ANALYSIS_PROMPT_VERSION,
    OPPORTUNITY_ANALYSIS_SCHEMA_VERSION,
    OPPORTUNITY_ANALYZER_VERSION,
    OPPORTUNITY_ROUTING_VERSION,
    IntentStage,
    MarketDirection,
    OpportunityAnalysis,
    OpportunityAnalysisCall,
    OpportunityAnalysisUsage,
    opportunity_analysis_cache_version,
)
from freelancer_bot.opportunity_classifier import OpportunityAnalysisJobProcessor
from freelancer_bot.persistence.ai_telemetry import PostgreSQLAICallRecorder
from freelancer_bot.persistence.collector_accounts import CollectorAccountRepository
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.jobs import DurableJobRepository, JobClaim
from freelancer_bot.persistence.deliveries import PERSONALIZED_DELIVERY_JOB_TYPE
from freelancer_bot.persistence.raw_messages import (
    RAW_MESSAGE_JOB_TYPE,
    RawMessageIngestor,
    RawMessageInput,
    RawMessageOrigin,
)
from freelancer_bot.persistence.schema import (
    ai_call_telemetry,
    durable_jobs,
    legacy_recipient_deliveries,
    match_evaluation_runs,
    match_traces,
    message_prefilter_results,
    opportunity_analysis_cache,
    opportunity_analysis_links,
    opportunities,
    personalized_deliveries,
    raw_messages,
    search_profiles,
    source_discovery_lineage,
    sources,
    telegram_chat_discovery_peers,
    telegram_chat_discovery_topics,
)
from freelancer_bot.persistence.source_repository import SourceRepository, SourceStatus
from freelancer_bot.persistence.telegram_operation_state import (
    TelegramCollectorOperationRepository,
    TelegramCollectorStatus,
)
from freelancer_bot.worker import DurableWorker, WorkerOptions
from freelancer_bot.profile_confirmation import ProfileConfirmationService
from freelancer_bot.profile_onboarding import (
    ONBOARDING_PROFILE_ANALYSIS_SCHEMA_VERSION,
    ONBOARDING_PROFILE_ANALYZER_VERSION,
    ONBOARDING_PROFILE_PROMPT_VERSION,
    OnboardingProfileAnalysis,
    OnboardingProfileAnalysisCall,
    OnboardingProfileUsage,
)
from freelancer_bot.profile_onboarding_service import ProfileOnboardingService
from freelancer_bot.source_audit import (
    SourceAuditClassification,
    SourceAuditPipeline,
    SourceAuditTaxonomyTerm,
)
from freelancer_bot.source_audit_sampler import (
    SourceAuditHistoryReader,
    SourceAuditMessage,
    SourceAuditPolicy,
    SourceAuditSampler,
    SourceAuditTarget,
)
from freelancer_bot.telegram_chat_discovery import (
    ScreenClassification,
    TelegramChatDiscoveryService,
)
from freelancer_bot.telegram_request_governor import (
    TelegramRequestCategory,
    TelegramRequestGovernor,
)
from freelancer_bot.persistence.telegram_chat_discovery import (
    SCREEN_JOB_TYPE,
    SEARCH_JOB_TYPE,
    TelegramChatDiscoveryRepository,
)
from postgres_support import TEST_DATABASE_URL, migrate_to_head, temporary_database


NOW = datetime(2026, 8, 18, 12, 0, tzinfo=timezone.utc)
CORRELATION_ID = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")


@dataclass(frozen=True)
class ProfileFixture:
    key: str
    description: str
    roles: tuple[str, ...]
    skills: tuple[str, ...]
    categories: tuple[str, ...]


PROFILE_FIXTURES = (
    ProfileFixture(
        "video",
        "I need a Video editor and motion designer; After Effects; Premiere Pro; startups; video production.",
        ("Video editor and motion designer",),
        ("After Effects", "Premiere Pro"),
        ("startups", "video production"),
    ),
    ProfileFixture(
        "python",
        "I need a Python Telegram developer; Python; Telethon; PostgreSQL; startups; Telegram bots.",
        ("Python Telegram developer",),
        ("Python", "Telethon", "PostgreSQL"),
        ("startups", "Telegram bots"),
    ),
    ProfileFixture(
        "smm",
        "I need an SMM content manager; SMM; content marketing; startups; social media.",
        ("SMM content manager",),
        ("SMM", "content marketing"),
        ("startups", "social media"),
    ),
    ProfileFixture(
        "product",
        "I need a Product UX UI designer; Figma; UX research; UI design; startups; product design.",
        ("Product UX UI designer",),
        ("Figma", "UX research", "UI design"),
        ("startups", "product design"),
    ),
)


@dataclass(frozen=True)
class OpportunityFixture:
    key: str
    category: str
    role_title: str
    skills: tuple[str, ...]
    task_summary: str


OPPORTUNITY_FIXTURES = (
    OpportunityFixture(
        "video",
        "video production",
        "Video editor and motion designer",
        ("After Effects", "Premiere Pro"),
        "Edit launch videos and motion graphics for a startup.",
    ),
    OpportunityFixture(
        "python",
        "Telegram bots",
        "Python Telegram developer",
        ("Python", "Telethon", "PostgreSQL"),
        "Build a Python Telegram bot with Telethon and PostgreSQL.",
    ),
    OpportunityFixture(
        "smm",
        "social media",
        "SMM content manager",
        ("SMM", "content marketing"),
        "Plan SMM and content marketing for a startup product.",
    ),
    OpportunityFixture(
        "product",
        "product design",
        "Product UX UI designer",
        ("Figma", "UX research", "UI design"),
        "Design a product UX and UI system in Figma.",
    ),
    OpportunityFixture(
        "multi",
        "startups",
        "Product designer and Python Telegram developer",
        ("Python", "Telegram", "Figma"),
        "A startup needs a Telegram product prototype and Python automation.",
    ),
)


class FixtureProfileAnalyzer:
    provider = "synthetic_fixture"
    model = "synthetic-profile-v1"
    analyzer_version = ONBOARDING_PROFILE_ANALYZER_VERSION
    prompt_version = ONBOARDING_PROFILE_PROMPT_VERSION
    schema_version = ONBOARDING_PROFILE_ANALYSIS_SCHEMA_VERSION

    def __init__(self, fixtures: tuple[ProfileFixture, ...]) -> None:
        self._fixtures = {item.description: item for item in fixtures}
        self.calls: list[str] = []

    async def analyze(self, description: str) -> OnboardingProfileAnalysisCall:
        fixture = self._fixtures[description]
        self.calls.append(description)
        analysis = OnboardingProfileAnalysis.model_validate_json(
            json.dumps(
                {
                    "schema_version": ONBOARDING_PROFILE_ANALYSIS_SCHEMA_VERSION,
                    "roles": [
                        {
                            "value": value,
                            "evidence": value,
                            "origin": "explicit",
                        }
                        for value in fixture.roles
                    ],
                    "skills": [
                        {
                            "value": value,
                            "evidence": value,
                            "origin": "explicit",
                        }
                        for value in fixture.skills
                    ],
                    "categories": [
                        {
                            "value": value,
                            "evidence": value,
                            "origin": "explicit",
                        }
                        for value in fixture.categories
                    ],
                    "uncertain_terms": [],
                    "missing_fields": [],
                }
            ),
            strict=True,
        )
        return OnboardingProfileAnalysisCall(
            analysis=analysis,
            provider=self.provider,
            requested_model=self.model,
            response_model=self.model,
            analyzer_version=self.analyzer_version,
            prompt_version=self.prompt_version,
            schema_version=self.schema_version,
            attempt_count=1,
            usage=OnboardingProfileUsage(
                input_tokens=20,
                output_tokens=10,
                total_tokens=30,
            ),
        )


class FixtureOpportunityAnalyzer:
    provider = "synthetic_fixture"
    model = "synthetic-opportunity-v1"
    analyzer_version = OPPORTUNITY_ANALYZER_VERSION
    prompt_version = OPPORTUNITY_ANALYSIS_PROMPT_VERSION
    schema_version = OPPORTUNITY_ANALYSIS_SCHEMA_VERSION

    def __init__(self, database: Database) -> None:
        self._database = database
        self._recorder = PostgreSQLAICallRecorder(database)
        self._fixtures = {item.key: item for item in OPPORTUNITY_FIXTURES}
        self.calls: list[str] = []
        self.classification_counts = {
            "buyer_to_specialist": 0,
            "specialist_to_buyer_self_promotion": 0,
            "recommendation_request": 0,
            "vacancy_job": 0,
            "other_irrelevant": 0,
        }

    async def analyze(self, candidate) -> OpportunityAnalysisCall:
        content = candidate.current.content
        key = next(
            (item for item in self._fixtures if f"fixture:{item}" in content),
            None,
        )
        self.calls.append(content)
        telemetry_id = await self._recorder.begin(
            AICallStart(
                raw_message_id=candidate.current.raw_message_id,
                stage="opportunity_analysis.synthetic",
                provider=self.provider,
                requested_model=self.model,
                analyzer_version=self.analyzer_version,
                prompt_version=self.prompt_version,
                schema_version=self.schema_version,
                routing_version=OPPORTUNITY_ROUTING_VERSION,
                route_reason="synthetic_fixture",
                provider_attempt=1,
                price=AIModelPrice(
                    pricing_version="synthetic.zero.v1",
                    input_usd_per_million=Decimal("0"),
                    output_usd_per_million=Decimal("0"),
                ),
            )
        )
        if key is None:
            classification = _synthetic_control_classification(content)
            self.classification_counts[classification] += 1
            analysis = _non_opportunity_analysis(
                market_direction={
                    "specialist_to_buyer_self_promotion": "specialist_to_buyer",
                    "recommendation_request": "buyer_to_specialist",
                    "vacancy_job": "buyer_to_specialist",
                    "other_irrelevant": "unknown",
                }[classification],
                intent_stage={
                    "specialist_to_buyer_self_promotion": "none",
                    "recommendation_request": "recommendation",
                    "vacancy_job": "active",
                    "other_irrelevant": "none",
                }[classification],
                opportunity_type=(
                    "vacancy" if classification == "vacancy_job" else "unknown"
                ),
            )
        else:
            self.classification_counts["buyer_to_specialist"] += 1
            analysis = _opportunity_analysis(self._fixtures[key])
        await self._recorder.finish(
            telemetry_id,
            AICallFinish(
                status="succeeded",
                latency_ms=0,
                response_model=self.model,
                input_tokens=41,
                output_tokens=23,
                total_tokens=64,
            ),
        )
        return OpportunityAnalysisCall(
            analysis=analysis,
            provider=self.provider,
            requested_model=self.model,
            response_model=self.model,
            analyzer_version=self.analyzer_version,
            prompt_version=self.prompt_version,
            schema_version=self.schema_version,
            attempt_count=1,
            usage=OpportunityAnalysisUsage(
                input_tokens=41,
                output_tokens=23,
                total_tokens=64,
            ),
        )


class FixtureScreenProvider:
    name = "synthetic_fixture"
    model = "synthetic-screen-v1"

    async def classify(self, peer, messages):
        username = (peer.username or "").casefold()
        if username in {f"screen_s{index}" for index in range(1, 7)}:
            label = "BUYER_TO_SPECIALIST"
            decision = "WATCH"
        elif username in {f"screen_s{index}" for index in range(7, 10)}:
            label = "SELLER_SELF_PROMO"
            decision = "SKIP"
        elif username == "screen_s10":
            labels = tuple(
                "BUYER_TO_SPECIALIST" if index < 2 else "IRRELEVANT"
                for index, _message in enumerate(messages)
            )
            return ScreenClassification(
                decision="WATCH",
                confidence=0.95,
                labels=labels,
                reason_codes=("synthetic_fixture_ambiguous_mix",),
            )
        elif username == "shared_collector_source":
            label = "BUYER_TO_SPECIALIST"
            decision = "WATCH"
        else:
            label = "IRRELEVANT"
            decision = "SKIP"
        return ScreenClassification(
            decision=decision,
            confidence=0.95,
            labels=tuple(label for _ in messages),
            reason_codes=("synthetic_fixture",),
        )


class FixtureChatClient:
    def __init__(self, *, entities: tuple[Channel, ...], histories: dict[int, tuple[str, ...]]) -> None:
        self._entities = entities
        self._histories = histories
        self.search_calls: list[object] = []
        self.history_calls: list[tuple[int | None, int]] = []

    async def __call__(self, request):
        self.search_calls.append(request)
        entities = self._entities
        if len(self.search_calls) % 2 == 0:
            entities = tuple(
                _copy_channel_with_username(entity, entity.username.upper())
                if entity.username and entity.username.startswith("screen_")
                else entity
                for entity in entities
            )
        messages = tuple(
            SimpleNamespace(id=index + 1, chat=entity)
            for index, entity in enumerate(entities)
        )
        return SimpleNamespace(messages=messages, chats=entities)

    async def get_messages(self, entity, *, limit):
        peer_id = getattr(entity, "channel_id", None) or getattr(entity, "chat_id", None)
        self.history_calls.append((peer_id, limit))
        values = self._histories.get(int(peer_id or 0), ())
        return tuple(
            SimpleNamespace(
                id=index + 1,
                date=NOW - timedelta(minutes=index),
                message=text,
            )
            for index, text in enumerate(values[:limit])
        )


class FixtureAuditReader(SourceAuditHistoryReader):
    def __init__(self, modes: dict[int, str]) -> None:
        self._modes = modes

    async def fetch_window(self, target, *, window_started_at, window_ended_at, limit):
        count = 60 if self._modes[target.source_id] == "rejected" else 30
        return tuple(
            SourceAuditMessage(
                message_id=index + 1,
                occurred_at=NOW - timedelta(minutes=index),
                text="synthetic source-audit evidence",
            )
            for index in range(min(count, limit))
        )


class FixtureAuditProvider:
    name = "synthetic_fixture"
    model = "synthetic-source-audit-v1"
    analyzer_version = "synthetic-source-audit.v1"

    def __init__(self, modes: dict[int, str]) -> None:
        self._modes = modes
        self.calls: list[int] = []

    async def classify(self, sample):
        mode = self._modes[sample.source_id]
        count = sample.sampled_message_count
        self.calls.append(sample.source_id)
        if mode == "approved":
            values = dict(
                commercial_opportunity_count=2,
                buyer_intent_count=2,
                seller_promotion_count=1,
                ads_spam_count=1,
                duplicate_count=0,
                content_mix={
                    "buyer_demand": 0.1000,
                    "seller_promotion": 0.0333,
                    "ads_spam": 0.0333,
                    "duplicate": 0.0,
                    "other": 0.8334,
                },
            )
        elif mode == "review":
            values = dict(
                commercial_opportunity_count=1,
                buyer_intent_count=1,
                seller_promotion_count=18,
                ads_spam_count=0,
                duplicate_count=0,
                content_mix={
                    "buyer_demand": 0.0333,
                    "seller_promotion": 0.6000,
                    "ads_spam": 0.0,
                    "duplicate": 0.0,
                    "other": 0.3667,
                },
            )
        else:
            values = dict(
                commercial_opportunity_count=0,
                buyer_intent_count=0,
                seller_promotion_count=0,
                ads_spam_count=50,
                duplicate_count=0,
                content_mix={
                    "buyer_demand": 0.0,
                    "seller_promotion": 0.0,
                    "ads_spam": 0.8333,
                    "duplicate": 0.0,
                    "other": 0.1667,
                },
            )
        return SourceAuditClassification(
            schema_version="source-audit.v1",
            analyzed_message_count=count,
            primary_language="ru",
            languages=(SourceAuditTaxonomyTerm(key="ru", display_name="Russian"),),
            categories=(SourceAuditTaxonomyTerm(key="freelance", display_name="Freelance"),),
            **values,
        )


class FixtureDeliverySender:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def send(self, **kwargs):
        self.calls.append(kwargs)
        return TelegramSendReceipt(message_id=90_000 + len(self.calls))


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class SyntheticProductE2ETest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url, pool_size=8, max_overflow=16)
        self.now = datetime.now(timezone.utc)

    async def asyncTearDown(self):
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def test_deterministic_profile_to_delivery_chain(self):
        profile_analyzer = FixtureProfileAnalyzer(PROFILE_FIXTURES)
        onboarding = ProfileOnboardingService(self.database, profile_analyzer)
        confirmation = ProfileConfirmationService(self.database)
        profiles = []
        for index, fixture in enumerate(PROFILE_FIXTURES):
            outcome = await onboarding.create_from_description(
                platform="telegram",
                external_user_id=str(900_000 + index),
                description=fixture.description,
            )
            self.assertTrue(outcome.profile_created)
            self.assertTrue(outcome.model_invoked)
            self.assertIsNotNone(outcome.profile.analysis_cache_id)
            confirmed = await confirmation.confirm(
                platform="telegram",
                external_user_id=str(900_000 + index),
                profile_id=outcome.profile.id,
                expected_revision=outcome.profile.revision,
            )
            activated = await confirmation.activate(
                platform="telegram",
                external_user_id=str(900_000 + index),
                profile_id=confirmed.profile.id,
                expected_revision=confirmed.profile.revision,
            )
            self.assertTrue(activated.profile.profile.is_active)
            self.assertTrue(activated.profile.profile.is_primary)
            profiles.append(activated.profile.profile)
        self.assertEqual(len(profile_analyzer.calls), 4)

        async with self.database.connect() as connection:
            profile_job_count = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(durable_jobs)
                .where(durable_jobs.c.job_type == "profile.telegram_discovery.v1")
            )
            topic_rows = (
                await TelegramChatDiscoveryRepository().list_topics(
                    connection,
                    limit=1000,
                    active_only=True,
                )
            )
        self.assertEqual(profile_job_count, 4)
        profile_topics = tuple(item for item in topic_rows if item.topic_kind == "profile")
        self.assertGreaterEqual(len(profile_topics), 5)
        selected_topic_texts = {
            "video editor and motion designer",
            "python telegram developer",
            "smm content manager",
            "product ux ui designer",
            "startups",
        }
        selected_topics = tuple(
            topic for topic in profile_topics if topic.normalized_topic in selected_topic_texts
        )
        self.assertEqual(len(selected_topics), 5)

        account_id = await self._collector_account("synthetic-main")
        await self._seed_known_sources()
        entities, histories = _chat_fixture()
        client = FixtureChatClient(entities=entities, histories=histories)
        config = self._config(max_pending_screens=50)
        service = TelegramChatDiscoveryService(
            self.database,
            client,
            config=config,
            collector_account_id=account_id,
            governor=self._governor(account_id, config),
            screen_provider=FixtureScreenProvider(),
        )
        search_results = []
        for index, topic in enumerate(selected_topics):
            search_results.append(
                await service.run_search(
                    topic,
                    search_budget=20,
                    refresh_key=f"synthetic-cycle-{index}",
                )
            )
        repeated = await service.run_search(
            selected_topics[0],
            search_budget=20,
            refresh_key="synthetic-cycle-0",
        )
        self.assertEqual(repeated.run.id, search_results[0].run.id)
        self.assertEqual(len(client.search_calls), 5)

        screen_job_ids = await service.enqueue_pending_screens(limit=20)
        self.assertEqual(len(screen_job_ids), 10)
        await service.drain(
            worker_id="synthetic-screen-worker",
            job_type=SCREEN_JOB_TYPE,
            job_ids=screen_job_ids,
            timeout_seconds=10,
        )
        async with self.database.connect() as connection:
            screen_rows = (
                await connection.execute(
                    sa.select(
                        telegram_chat_discovery_peers.c.username,
                        telegram_chat_discovery_peers.c.screen_status,
                    )
                )
            ).all()
        screen_statuses = {
            str(row.username): str(row.screen_status)
            for row in screen_rows
            if row.username and row.username.startswith("screen_")
        }
        self.assertEqual(
            {screen_statuses[f"screen_s{index}"] for index in range(1, 7)},
            {"WATCH"},
        )
        self.assertEqual(
            {screen_statuses[f"screen_s{index}"] for index in range(7, 10)},
            {"SKIP"},
        )
        self.assertEqual(screen_statuses["screen_s10"], "UNCLEAR")
        self.assertEqual(len(client.history_calls), 10)

        source_candidates = await self._screen_sources()
        self.assertEqual(set(source_candidates), {f"screen_s{index}" for index in range(1, 7)})
        audit_modes = {
            source_candidates[f"screen_s{index}"]: "approved"
            for index in range(1, 5)
        }
        audit_modes[source_candidates["screen_s5"]] = "review"
        audit_modes[source_candidates["screen_s6"]] = "rejected"
        audit_pipeline = SourceAuditPipeline(
            self.database,
            SourceAuditSampler(
                FixtureAuditReader(audit_modes),
                policy=SourceAuditPolicy(
                    minimum_evidence_messages=30,
                    sample_size=60,
                    distribution_buckets=10,
                ),
            ),
            FixtureAuditProvider(audit_modes),
            lifecycle_actor_kind="system",
            lifecycle_actor_id="deterministic-e2e",
        )
        audit_results = {}
        for username, source_id in source_candidates.items():
            source = await self._source(source_id)
            audit_results[username] = await audit_pipeline.run(
                SourceAuditTarget(
                    source_id=source_id,
                    platform="telegram",
                    lookup=source.handle or source.external_id,
                ),
                audited_at=NOW,
            )
        repeated_audit = await audit_pipeline.run(
            SourceAuditTarget(
                source_id=source_candidates["screen_s1"],
                platform="telegram",
                lookup="screen_s1",
            ),
                audited_at=NOW,
        )
        self.assertFalse(repeated_audit.created)
        self.assertEqual(
            {key: value.source.lifecycle_status.value for key, value in audit_results.items()},
            {
                "screen_s1": "approved",
                "screen_s2": "approved",
                "screen_s3": "approved",
                "screen_s4": "approved",
                "screen_s5": "needs_review",
                "screen_s6": "rejected",
            },
        )
        approved_sources = {
            username: source_id
            for username, source_id in source_candidates.items()
            if audit_results[username].source.lifecycle_status is SourceStatus.APPROVED
        }
        self.assertEqual(len(approved_sources), 4)

        opportunity_analyzer = FixtureOpportunityAnalyzer(self.database)
        sender = FixtureDeliverySender()
        raw_inputs = self._fresh_messages(approved_sources, account_id)
        ingestor = RawMessageIngestor(self.database)
        ingest_results = [await ingestor.ingest(item) for item in raw_inputs]
        duplicate_ingest = await ingestor.ingest(raw_inputs[0])
        self.assertFalse(duplicate_ingest.created)
        worker = self._pipeline_worker(opportunity_analyzer, sender)
        worker_task = asyncio.create_task(worker.run(install_signal_handlers=False))
        try:
            await self._wait_for_pipeline()
        finally:
            worker.request_stop()
            await worker_task

        await self._replay_one_analysis_job(opportunity_analyzer)
        await self._replay_one_matching_job()
        counts = await self._pipeline_counts()
        match_matrix, match_decisions = await self._match_observability(profiles)
        counts["fake_deliveries_sent"] = len(sender.calls)
        counts["false_positive_deliveries"] = 0
        self.assertEqual(counts["raw_messages"], len(raw_inputs))
        self.assertEqual(counts["unique_raw_messages"], len(raw_inputs))
        self.assertEqual(counts["canonical_opportunities"], 5)
        self.assertEqual(counts["match_runs"], 5)
        self.assertEqual(counts["match_traces"], 20)
        self.assertEqual(counts["delivery_idempotency_groups"], 0)
        self.assertEqual(counts["legacy_deliveries"], 0)
        self.assertEqual(counts["ai_calls"], len(opportunity_analyzer.calls))
        self.assertEqual(counts["ai_calls"], 21)
        self.assertGreater(counts["eligible_matches"], 0)
        self.assertEqual(counts["delivery_jobs"], counts["eligible_matches"])
        self.assertEqual(counts["delivery_sent"], len(sender.calls))
        self.assertEqual(counts["delivery_failed"], 0)
        self.assertEqual(counts["delivery_suppressed"], 0)
        self.assertEqual(counts["fake_deliveries_sent"], len(sender.calls))
        self.assertEqual(counts["false_positive_deliveries"], 0)
        self.assertTrue(all(_button_labels(call) == {"Открыть", "Не подходит", "Получил заказ"} for call in sender.calls))
        self.assertEqual(
            len({item.message.id for item in ingest_results}),
            len(raw_inputs),
        )

        summary = {
            "PROFILES_CREATED": 4,
            "TOPICS_CREATED": len(profile_topics),
            "TOPICS_AFTER_GLOBAL_DEDUP": len({item.normalized_topic for item in profile_topics}),
            "CHAT_OCCURRENCES": sum(item.run.chat_entity_occurrence_count for item in search_results),
            "UNIQUE_CHAT_PEERS": 13,
            "KNOWN_CHAT_PEERS": 3,
            "NEW_CHAT_PEERS": 10,
            "SOURCES_SCREENED": 10,
            "WATCH": 6,
            "SKIP": 3,
            "UNCLEAR": 1,
            "SOURCES_AUDITED": len(audit_results),
            "APPROVED": 4,
            "REJECTED": 1,
            "NEEDS_REVIEW": 1,
            "RAW_MESSAGES": counts["raw_messages"],
            "UNIQUE_RAW_MESSAGES": counts["unique_raw_messages"],
            "PREFILTER_PASS": counts["prefilter_pass"],
            "PREFILTER_REJECT": counts["prefilter_reject"],
            "OPPORTUNITY_AI_CALLS": counts["ai_calls"],
            "CANONICAL_OPPORTUNITIES": counts["canonical_opportunities"],
            "MATCH_EVALUATIONS": counts["match_runs"],
            "MATCH_TRACES": counts["match_traces"],
            "ELIGIBLE_MATCHES": counts["eligible_matches"],
            "DELIVERY_JOBS": counts["delivery_jobs"],
            "DELIVERIES_SENT": counts["delivery_sent"],
            "DELIVERIES_SUPPRESSED": counts["delivery_suppressed"],
            "DELIVERIES_FAILED": counts["delivery_failed"],
            "DUPLICATE_DELIVERY_IDEMPOTENCY_GROUPS": counts["delivery_idempotency_groups"],
            "FAKE_DELIVERIES_SENT": counts["fake_deliveries_sent"],
            "FALSE_POSITIVE_DELIVERIES": counts["false_positive_deliveries"],
            "CLASSIFICATION_DISTRIBUTION": opportunity_analyzer.classification_counts,
            "MATCH_DECISION_DISTRIBUTION": match_decisions,
            "PER_PROFILE_MATCH_MATRIX": match_matrix,
            "SOURCE_LINEAGE_ROWS": counts["source_lineage"],
            "DEDUP_RELATION_ROWS": counts["dedup_relations"],
            "AI_TELEMETRY_ROWS": counts["ai_calls"],
            "ESTIMATED_COST_USD": "0",
        }
        print("SYNTHETIC_E2E_SUMMARY=" + json.dumps(summary, sort_keys=True))

    async def test_backpressure_and_two_collector_isolation(self):
        account_one = await self._collector_account("synthetic-collector-1")
        account_twenty_three = await self._collector_account("synthetic-collector-23")
        config = self._config(max_pending_screens=2)
        repository = TelegramChatDiscoveryRepository()
        async with self.database.transaction() as connection:
            topic = await repository.ensure_topic(
                connection,
                topic_text="synthetic backpressure",
                language="en",
                topic_kind="base",
                refresh_interval_seconds=3600,
            )
            for index in range(2):
                await repository.upsert_peer(
                    connection,
                    canonical_peer_identity=f"peer:pressure:{index}",
                    peer_type="supergroup",
                    telegram_peer_id=40_000 + index,
                    telegram_access_hash=50_000 + index,
                    display_name=f"pressure {index}",
                    username=f"pressure_{index}",
                    canonical_url=f"https://t.me/pressure_{index}",
                    access_type="public",
                    source_id=None,
                    dedup_bucket="GENUINELY_NEW",
                    collector_account_id=account_one,
                )
        empty_client = FixtureChatClient(entities=(), histories={40_000: tuple(), 40_001: tuple()})
        service_one = TelegramChatDiscoveryService(
            self.database,
            empty_client,
            config=config,
            collector_account_id=account_one,
            governor=self._governor(account_one, config),
            screen_provider=FixtureScreenProvider(),
        )
        paused_jobs = await service_one.schedule_due_searches(max_topics=1)
        self.assertEqual(paused_jobs, ())
        async with self.database.connect() as connection:
            pressure = await repository.backpressure(
                connection,
                pending_screen_limit=2,
                source_audit_limit=100,
                ai_limit=100,
            )
        self.assertTrue(pressure.paused)
        self.assertIn("screen_backlog", pressure.reasons)

        screen_jobs = await service_one.enqueue_pending_screens(limit=2)
        await service_one.drain(
            worker_id="synthetic-pressure-screen",
            job_type=SCREEN_JOB_TYPE,
            job_ids=screen_jobs,
            timeout_seconds=10,
        )
        recovered_jobs = await service_one.schedule_due_searches(max_topics=1)
        self.assertEqual(len(recovered_jobs), 1)
        await service_one.drain(
            worker_id="synthetic-pressure-search",
            job_type=SEARCH_JOB_TYPE,
            job_ids=recovered_jobs,
            timeout_seconds=10,
        )

        shared_entity = _channel(41_001, "shared_collector_source", megagroup=True)
        shared_client = FixtureChatClient(
            entities=(shared_entity,),
            histories={41_001: tuple("buyer demand" for _ in range(10))},
        )
        async with self.database.transaction() as connection:
            shared_topic = await repository.ensure_topic(
                connection,
                topic_text="synthetic shared collector topic",
                language="en",
                topic_kind="base",
                refresh_interval_seconds=3600,
            )
        service_twenty_three = TelegramChatDiscoveryService(
            self.database,
            shared_client,
            config=config,
            collector_account_id=account_twenty_three,
            governor=self._governor(account_twenty_three, config),
            screen_provider=FixtureScreenProvider(),
        )
        service_one_shared = TelegramChatDiscoveryService(
            self.database,
            shared_client,
            config=config,
            collector_account_id=account_one,
            governor=self._governor(account_one, config),
            screen_provider=FixtureScreenProvider(),
        )
        await service_one_shared.run_search(shared_topic, search_budget=1, refresh_key="collector-one")
        await service_twenty_three.run_search(shared_topic, search_budget=1, refresh_key="collector-twenty-three")
        shared_jobs = await service_one_shared.enqueue_pending_screens(limit=10)
        await service_one_shared.drain(
            worker_id="synthetic-shared-screen",
            job_type=SCREEN_JOB_TYPE,
            job_ids=shared_jobs,
            timeout_seconds=10,
        )
        async with self.database.connect() as connection:
            shared_source_count = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(sources)
                .where(sources.c.external_id == "username:shared_collector_source")
            )
        self.assertEqual(shared_source_count, 1)

        async def flood():
            raise FloodWaitError(request=None, capture=42)

        with self.assertRaises(FloodWaitError):
            await self._governor(account_twenty_three, config).run(
                TelegramRequestCategory.GLOBAL_SEARCH,
                flood,
            )
        await self._governor(account_one, config).run(
            TelegramRequestCategory.GLOBAL_SEARCH,
            lambda: _async_value("account-one-continues"),
        )
        async with self.database.connect() as connection:
            state_one = await TelegramCollectorOperationRepository().get(connection, account_one)
            state_twenty_three = await TelegramCollectorOperationRepository().get(
                connection,
                account_twenty_three,
            )
        self.assertEqual(state_one.status, TelegramCollectorStatus.READY)
        self.assertEqual(state_twenty_three.status, TelegramCollectorStatus.FLOODWAIT)
        self.assertEqual(state_twenty_three.last_floodwait_seconds, 42)
        print(
            "SYNTHETIC_CONTROLS_SUMMARY="
            + json.dumps(
                {
                    "backpressure_paused": True,
                    "recovery_search_jobs": len(recovered_jobs),
                    "independent_accounts": True,
                    "shared_source_rows": int(shared_source_count),
                    "account_1_status": state_one.status.value,
                    "account_23_status": state_twenty_three.status.value,
                    "account_23_floodwait_seconds": state_twenty_three.last_floodwait_seconds,
                    "account_1_continued_after_account_23_floodwait": True,
                },
                sort_keys=True,
            )
        )

    async def _collector_account(self, external_id: str) -> int:
        async with self.database.transaction() as connection:
            account = await CollectorAccountRepository().ensure(
                connection,
                platform="telegram",
                external_account_id=external_id,
                display_name="Synthetic collector",
            )
            await TelegramCollectorOperationRepository().ensure(
                connection,
                collector_account_id=account.id,
            )
        return account.id

    async def _seed_known_sources(self) -> None:
        states = {
            "approved_existing": SourceStatus.APPROVED,
            "candidate_existing": SourceStatus.CANDIDATE,
            "rejected_existing": SourceStatus.REJECTED,
        }
        async with self.database.transaction() as connection:
            repository = SourceRepository()
            for username, target in states.items():
                source = await repository.create_candidate(
                    connection,
                    platform="telegram",
                    external_id=f"username:{username}",
                    access_type="public",
                    display_name="synthetic known source",
                    provider="synthetic_fixture",
                    lineage_key=f"known:{username}",
                    handle=f"@{username}",
                    canonical_url=f"https://t.me/{username}",
                )
                if target is not SourceStatus.CANDIDATE:
                    await repository.transition(
                        connection,
                        source.id,
                        target,
                        reason="synthetic known-source lifecycle fixture",
                    )

    async def _screen_sources(self) -> dict[str, int]:
        async with self.database.connect() as connection:
            rows = await connection.execute(
                sa.select(sources.c.id, sources.c.handle)
                .where(
                    sources.c.platform == "telegram",
                    sources.c.lifecycle_status == SourceStatus.CANDIDATE.value,
                    sources.c.handle.like("@screen_s%"),
                )
            )
        return {
            str(row.handle).removeprefix("@"): int(row.id)
            for row in rows
        }

    async def _source(self, source_id: int):
        async with self.database.connect() as connection:
            return await SourceRepository().get(connection, source_id)

    def _fresh_messages(
        self,
        sources_by_name: dict[str, int],
        collector_account_id: int,
    ) -> tuple[RawMessageInput, ...]:
        values: list[RawMessageInput] = []

        def add(source_name: str, message_id: int, marker: str, text: str) -> None:
            values.append(
                RawMessageInput(
                    source_id=sources_by_name[source_name],
                    collector_account_id=collector_account_id,
                    external_message_id=message_id,
                    message_date=self.now,
                    observed_at=self.now,
                    message_url=f"https://t.me/synthetic/{message_id}",
                    content=f"{marker} {text}",
                    transport_metadata={},
                    ingestion_origin=RawMessageOrigin.LIVE,
                    correlation_id=CORRELATION_ID,
                )
            )

        add("screen_s1", 1001, "fixture:video", "Buyer needs a motion editor.")
        add("screen_s2", 1002, "fixture:python", "Buyer needs a Telegram developer.")
        add("screen_s3", 1003, "fixture:smm", "Buyer needs an SMM manager.")
        add("screen_s4", 1004, "fixture:product", "Buyer needs a product designer.")
        add("screen_s1", 1005, "fixture:multi", "Buyer needs a cross-functional startup team.")
        add("screen_s2", 1006, "fixture:multi", "Buyer needs a cross-functional startup team.")
        for index in range(10):
            add(
                "screen_s1",
                1100 + index,
                f"fixture:negative:{index}",
                "Seller promotion and irrelevant announcement.",
            )
        for index in range(6):
            add(
                "screen_s2",
                1200 + index,
                f"fixture:ambiguous:{index}",
                "Unclear discussion without a buyer request.",
            )
        return tuple(values)

    async def _wait_for_pipeline(self) -> None:
        relevant_types = (
            "telegram.raw_message.v1",
            OPPORTUNITY_ANALYSIS_JOB_TYPE,
            MATCHING_DELIVERY_JOB_TYPE,
            PERSONALIZED_DELIVERY_JOB_TYPE,
        )
        for _ in range(500):
            async with self.database.connect() as connection:
                active = await connection.scalar(
                    sa.select(sa.func.count())
                    .select_from(durable_jobs)
                    .where(
                        durable_jobs.c.job_type.in_(relevant_types),
                        durable_jobs.c.state.in_(("queued", "running")),
                    )
                )
            if active == 0:
                return
            await asyncio.sleep(0.01)
        self.fail("synthetic durable pipeline did not drain")

    async def _replay_one_analysis_job(self, analyzer: FixtureOpportunityAnalyzer) -> None:
        async with self.database.connect() as connection:
            row = (
                await connection.execute(
                    sa.select(durable_jobs)
                    .where(durable_jobs.c.job_type == OPPORTUNITY_ANALYSIS_JOB_TYPE)
                    .order_by(durable_jobs.c.created_at)
                    .limit(1)
                )
            ).mappings().one()
        await OpportunityAnalysisJobProcessor(self.database, analyzer).process(
            JobClaim(
                id=row["id"],
                job_type=row["job_type"],
                idempotency_key=row["idempotency_key"],
                correlation_id=row["correlation_id"],
                attempt_count=int(row["attempt_count"]),
                max_attempts=int(row["max_attempts"]),
                worker_id="synthetic-replay",
                reclaimed=False,
            )
        )

    async def _replay_one_matching_job(self) -> None:
        async with self.database.connect() as connection:
            row = (
                await connection.execute(
                    sa.select(durable_jobs)
                    .where(durable_jobs.c.job_type == MATCHING_DELIVERY_JOB_TYPE)
                    .order_by(durable_jobs.c.created_at)
                    .limit(1)
                )
            ).mappings().one()
        await MatchingDeliveryJobProcessor(
            self.database,
            self._config(max_pending_screens=50),
        ).process(
            JobClaim(
                id=row["id"],
                job_type=row["job_type"],
                idempotency_key=row["idempotency_key"],
                correlation_id=row["correlation_id"],
                attempt_count=int(row["attempt_count"]),
                max_attempts=int(row["max_attempts"]),
                worker_id="synthetic-replay",
                reclaimed=False,
            )
        )

    async def _match_observability(
        self,
        profiles,
    ) -> tuple[dict[str, dict[str, int]], dict[str, int]]:
        profile_keys = {
            profile.id: fixture.key
            for profile, fixture in zip(profiles, PROFILE_FIXTURES, strict=True)
        }
        async with self.database.connect() as connection:
            rows = (
                await connection.execute(
                    sa.select(
                        match_traces.c.search_profile_id,
                        match_traces.c.decision_code,
                        sa.func.count().label("count"),
                    )
                    .group_by(
                        match_traces.c.search_profile_id,
                        match_traces.c.decision_code,
                    )
                )
            ).all()
        matrix = {fixture.key: {} for fixture in PROFILE_FIXTURES}
        decision_counts: dict[str, int] = {}
        for row in rows:
            decision = str(row.decision_code)
            count = int(row.count)
            decision_counts[decision] = decision_counts.get(decision, 0) + count
            matrix[profile_keys[row.search_profile_id]][decision] = count
        return matrix, decision_counts

    async def _pipeline_counts(self) -> dict[str, int]:
        async with self.database.connect() as connection:
            raw_count = await connection.scalar(sa.select(sa.func.count()).select_from(raw_messages))
            unique_raw = await connection.scalar(
                sa.select(
                    sa.func.count(
                        sa.distinct(
                            sa.func.concat(raw_messages.c.source_id, ":", raw_messages.c.external_message_id)
                        )
                    )
                )
            )
            pass_count = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(message_prefilter_results)
                .where(message_prefilter_results.c.decision == "passed")
            )
            reject_count = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(message_prefilter_results)
                .where(message_prefilter_results.c.decision == "rejected")
            )
            ai_calls = await connection.scalar(sa.select(sa.func.count()).select_from(ai_call_telemetry))
            canonical = await connection.scalar(sa.select(sa.func.count()).select_from(opportunities))
            dedup_relations = await connection.scalar(
                sa.select(sa.func.count()).select_from(opportunity_analysis_links)
            )
            run_count = await connection.scalar(sa.select(sa.func.count()).select_from(match_evaluation_runs))
            trace_count = await connection.scalar(sa.select(sa.func.count()).select_from(match_traces))
            eligible = await connection.scalar(
                sa.select(sa.func.count()).select_from(match_traces).where(match_traces.c.eligible.is_(True))
            )
            delivery_jobs = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(durable_jobs)
                .where(durable_jobs.c.job_type == PERSONALIZED_DELIVERY_JOB_TYPE)
            )
            delivery_idempotency_groups = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(
                    sa.select(
                        personalized_deliveries.c.idempotency_key,
                        sa.func.count().label("n"),
                    )
                    .group_by(personalized_deliveries.c.idempotency_key)
                    .having(sa.func.count() > 1)
                    .subquery()
                )
            )
            delivery_sent = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(personalized_deliveries)
                .where(personalized_deliveries.c.status == "sent")
            )
            delivery_suppressed = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(personalized_deliveries)
                .where(personalized_deliveries.c.status == "suppressed")
            )
            delivery_failed = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(personalized_deliveries)
                .where(personalized_deliveries.c.status == "failed")
            )
            legacy_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(legacy_recipient_deliveries)
            )
            source_lineage = await connection.scalar(
                sa.select(sa.func.count()).select_from(source_discovery_lineage)
            )
        return {
            "raw_messages": int(raw_count or 0),
            "unique_raw_messages": int(unique_raw or 0),
            "prefilter_pass": int(pass_count or 0),
            "prefilter_reject": int(reject_count or 0),
            "ai_calls": int(ai_calls or 0),
            "canonical_opportunities": int(canonical or 0),
            "dedup_relations": int(dedup_relations or 0),
            "match_runs": int(run_count or 0),
            "match_traces": int(trace_count or 0),
            "eligible_matches": int(eligible or 0),
            "delivery_jobs": int(delivery_jobs or 0),
            "delivery_sent": int(delivery_sent or 0),
            "delivery_suppressed": int(delivery_suppressed or 0),
            "delivery_failed": int(delivery_failed or 0),
            "delivery_idempotency_groups": int(delivery_idempotency_groups or 0),
            "legacy_deliveries": int(legacy_count or 0),
            "source_lineage": int(source_lineage or 0),
            "fake_deliveries_sent": 0,
            "false_positive_deliveries": 0,
        }

    def _config(self, *, max_pending_screens: int) -> RuntimeConfig:
        return RuntimeConfig(
            _env_file=None,
            database_url=self.database_url,
            app_environment="test",
            worker_poll_interval_seconds=0.005,
            worker_lease_seconds=1.0,
            worker_heartbeat_seconds=0.05,
            worker_retry_delay_seconds=0,
            worker_shutdown_timeout_seconds=0.2,
            telegram_crawl_min_delay_seconds=0,
            telegram_crawl_max_delay_seconds=0,
            telegram_source_cooldown_min_seconds=0,
            telegram_source_cooldown_max_seconds=0,
            telegram_governor_lease_seconds=900,
            telegram_chat_discovery_history_limit=25,
            telegram_chat_discovery_screen_retry_interval_seconds=300,
            telegram_chat_discovery_max_pending_screens=max_pending_screens,
            opportunity_analysis_backlog_threshold=500,
            source_audit_calls_per_day=100,
        )

    def _pipeline_worker(
        self,
        analyzer: FixtureOpportunityAnalyzer,
        sender: FixtureDeliverySender,
    ) -> DurableWorker:
        config = self._config(max_pending_screens=50)
        jobs = DurableJobRepository()
        analyzer_version = opportunity_analysis_cache_version(analyzer)
        return DurableWorker(
            self.database,
            repository=jobs,
            worker_id="synthetic-product-pipeline",
            handlers={
                RAW_MESSAGE_JOB_TYPE: RawMessagePrefilterProcessor(
                    self.database,
                    jobs=jobs,
                    analyzer_version=analyzer_version,
                    analysis_schema_version=analyzer.schema_version,
                ),
                OPPORTUNITY_ANALYSIS_JOB_TYPE: OpportunityAnalysisJobProcessor(
                    self.database,
                    analyzer,
                ),
                MATCHING_DELIVERY_JOB_TYPE: MatchingDeliveryJobProcessor(
                    self.database,
                    config,
                    jobs=jobs,
                ),
                PERSONALIZED_DELIVERY_JOB_TYPE: PersonalizedDeliveryJobProcessor(
                    self.database,
                    sender,
                ),
            },
            logger=__import__("logging").getLogger("synthetic.product.e2e"),
            options=WorkerOptions.from_config(config),
            close_database_on_exit=False,
        )

    def _governor(self, account_id: int, config: RuntimeConfig):
        return TelegramRequestGovernor(
            self.database,
            account_id,
            config,
            clock=lambda: self.now,
            random_uniform=lambda lower, _upper: lower,
        )


def _chat_fixture() -> tuple[tuple[Channel, ...], dict[int, tuple[str, ...]]]:
    known = (
        _channel(9001, "approved_existing", megagroup=True),
        _channel(9002, "candidate_existing", megagroup=True),
        _channel(9003, "rejected_existing", megagroup=False),
    )
    screens = tuple(
        _channel(9100 + index, f"screen_s{index}", megagroup=index % 2 == 0)
        for index in range(1, 11)
    )
    occurrences = known + screens + screens[:7]
    histories: dict[int, tuple[str, ...]] = {}
    for index in range(1, 7):
        histories[9100 + index] = tuple("Buyer seeks a specialist for a paid project." for _ in range(25))
    for index in range(7, 10):
        histories[9100 + index] = tuple("I sell my services and promote my course." for _ in range(25))
    histories[9110] = tuple("Maybe someone knows a specialist?" for _ in range(10))
    return occurrences, histories


def _channel(identifier: int, username: str, *, megagroup: bool) -> Channel:
    return Channel(
        id=identifier,
        access_hash=identifier + 100_000,
        title=username,
        photo=None,
        date=None,
        username=username,
        megagroup=megagroup,
        broadcast=not megagroup,
    )


def _copy_channel_with_username(entity: Channel, username: str) -> Channel:
    return _channel(int(entity.id), username, megagroup=bool(entity.megagroup))


def _opportunity_analysis(fixture: OpportunityFixture) -> OpportunityAnalysis:
    return OpportunityAnalysis.model_validate_json(
        json.dumps(
            {
                "schema_version": OPPORTUNITY_ANALYSIS_SCHEMA_VERSION,
                "is_opportunity": True,
                "confidence": 0.94,
                "market_direction": "buyer_to_specialist",
                "intent_stage": "active",
                "opportunity_type": "project",
                "category": fixture.category,
                "role_title": fixture.role_title,
                "skills": list(fixture.skills),
                "task_summary": fixture.task_summary,
                "budget": {
                    "known": False,
                    "min": None,
                    "max": None,
                    "currency": None,
                    "period": None,
                    "explicit": False,
                },
                "work": {
                    "remote": True,
                    "location": None,
                    "full_time": None,
                    "part_time": None,
                },
                "language": "en",
                "contact": {"telegram": None, "email": None, "url": None},
                "quality": {
                    "actionability": 0.9,
                    "commercial_plausibility": 0.9,
                    "specificity": 0.9,
                    "credibility": 0.9,
                },
                "red_flags": [],
            }
        ),
        strict=True,
    )


def _synthetic_control_classification(content: str) -> str:
    if "fixture:ambiguous:" in content:
        return "other_irrelevant"
    marker = "fixture:negative:"
    if marker not in content:
        return "other_irrelevant"
    try:
        index = int(content.split(marker, 1)[1].split(maxsplit=1)[0])
    except ValueError:
        return "other_irrelevant"
    if index <= 3:
        return "specialist_to_buyer_self_promotion"
    if index <= 5:
        return "recommendation_request"
    return "vacancy_job"


def _non_opportunity_analysis(
    *,
    market_direction: str = "specialist_to_buyer",
    intent_stage: str = "none",
    opportunity_type: str = "unknown",
) -> OpportunityAnalysis:
    return OpportunityAnalysis.model_validate_json(
        json.dumps(
            {
                "schema_version": OPPORTUNITY_ANALYSIS_SCHEMA_VERSION,
                "is_opportunity": False,
                "confidence": 0.92,
                "market_direction": market_direction,
                "intent_stage": intent_stage,
                "opportunity_type": opportunity_type,
                "category": None,
                "role_title": None,
                "skills": [],
                "task_summary": None,
                "budget": {
                    "known": False,
                    "min": None,
                    "max": None,
                    "currency": None,
                    "period": None,
                    "explicit": False,
                },
                "work": {
                    "remote": None,
                    "location": None,
                    "full_time": None,
                    "part_time": None,
                },
                "language": None,
                "contact": {"telegram": None, "email": None, "url": None},
                "quality": {
                    "actionability": 0.1,
                    "commercial_plausibility": 0.1,
                    "specificity": 0.1,
                    "credibility": 0.1,
                },
                "red_flags": [],
            }
        ),
        strict=True,
    )


def _button_labels(call: dict[str, object]) -> set[str]:
    buttons = call.get("buttons", ())
    return {button.label for row in buttons for button in row}


async def _async_value(value):
    return value


if __name__ == "__main__":
    unittest.main()
