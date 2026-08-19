from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import unittest
from uuid import uuid4

from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.product_metrics import (
    ProductMetricsRepository,
    ProductMetricsWindow,
    SourcePerformanceMetric,
    _rate,
    _source_rank_key,
)
from freelancer_bot.persistence.schema import (
    collector_accounts,
    delivery_action_events,
    durable_jobs,
    feedback_events,
    match_evaluation_runs,
    match_traces,
    message_prefilter_results,
    opportunities,
    opportunity_analysis_cache,
    opportunity_analysis_links,
    opportunity_source_messages,
    personalized_deliveries,
    raw_messages,
    search_profiles,
    source_quality_snapshots,
    sources,
    users,
)
from postgres_support import TEST_DATABASE_URL, migrate_to_head, temporary_database


UTC = timezone.utc


class ProductMetricsUnitTest(unittest.TestCase):
    def test_window_is_half_open_and_timezone_aware(self):
        started = datetime(2026, 8, 15, tzinfo=UTC)
        ended = started + timedelta(days=1)
        window = ProductMetricsWindow(started, ended)

        self.assertEqual(window.started_at, started)
        self.assertEqual(window.ended_at, ended)
        with self.assertRaisesRegex(ValueError, "timezone"):
            ProductMetricsWindow(started.replace(tzinfo=None), ended)
        with self.assertRaisesRegex(ValueError, "end after"):
            ProductMetricsWindow(started, started)

    def test_won_lead_rate_uses_sent_delivery_denominator(self):
        self.assertEqual(_rate(1, 3), Decimal("0.3333"))
        self.assertEqual(_rate(1, 2), Decimal("0.5000"))
        self.assertIsNone(_rate(1, 0))

        profile_id = uuid4()
        metrics = ProductMetricsRepository._won_lead_rate_metrics(
            {(11, profile_id, "design", "project"): 2},
            {(11, profile_id, "design", "project"): [2, 1, 1]},
        )

        self.assertEqual(len(metrics), 1)
        self.assertEqual(metrics[0].delivered_count, 2)
        self.assertEqual(metrics[0].feedback_count, 2)
        self.assertEqual(metrics[0].not_suitable_count, 1)
        self.assertEqual(metrics[0].got_job_count, 1)
        self.assertEqual(metrics[0].won_lead_rate, Decimal("0.5000"))

    def test_source_rank_uses_audit_and_pipeline_yield_not_subscriber_count(self):
        common = dict(
            source_display_name="fixture",
            messages=10,
            candidates=5,
            analyses=5,
            opportunities=5,
            scheduled_deliveries=2,
            delivered_deliveries=2,
            feedback_events=1,
            not_suitable_count=0,
            got_job_count=1,
            pipeline_opportunity_yield=Decimal("0.5000"),
            buyer_intent_ratio=None,
            seller_ratio=None,
            spam_ratio=None,
            duplicate_ratio=None,
            source_quality_audited_at=None,
            source_quality_window_started_at=None,
            source_quality_window_ended_at=None,
            source_quality_snapshot_id=None,
            source_quality_audit_key=None,
            won_lead_rate=Decimal("0.5000"),
            rank=0,
        )
        higher_yield = SourcePerformanceMetric(
            source_id=1,
            quality_opportunity_yield=Decimal("0.8000"),
            **common,
        )
        lower_yield = SourcePerformanceMetric(
            source_id=2,
            quality_opportunity_yield=Decimal("0.2000"),
            **common,
        )

        self.assertLess(_source_rank_key(higher_yield), _source_rank_key(lower_yield))


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class ProductMetricsPostgresTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url, pool_size=4, max_overflow=8)
        self.metrics = ProductMetricsRepository()
        self.started_at = datetime(2026, 8, 15, 0, 0, tzinfo=UTC)
        self.ended_at = self.started_at + timedelta(days=1)

    async def asyncTearDown(self):
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def test_report_reads_funnel_feedback_and_source_audit_evidence(self):
        source_one = await self._insert_source("one")
        source_two = await self._insert_source("two")
        user_one, profile_one = await self._insert_profile("1001", "Designer")
        user_two, profile_two = await self._insert_profile("1002", "Developer")
        collector_id = await self._insert_collector()

        raw_one, raw_two = uuid4(), uuid4()
        opportunity_one, opportunity_two = uuid4(), uuid4()
        cache_one, cache_two = uuid4(), uuid4()
        run_one, run_two = uuid4(), uuid4()
        trace_one, trace_two = uuid4(), uuid4()
        delivery_one, delivery_two = uuid4(), uuid4()
        action_one, action_two = uuid4(), uuid4()
        now = self.started_at + timedelta(hours=2)

        async with self.database.transaction() as connection:
            raw_jobs = [
                self._job("raw-one", now),
                self._job("raw-two", now),
            ]
            prefilter_jobs = [
                self._job("analysis-one", now),
                self._job("analysis-two", now),
            ]
            delivery_jobs = [
                self._completed_job("delivery-one", now),
                self._completed_job("delivery-two", now),
            ]
            await connection.execute(durable_jobs.insert(), raw_jobs + prefilter_jobs + delivery_jobs)
            await connection.execute(
                raw_messages.insert(),
                [
                    self._raw_message(raw_one, source_one, collector_id, raw_jobs[0]["id"], now, 1),
                    self._raw_message(raw_two, source_two, collector_id, raw_jobs[1]["id"], now, 2),
                ],
            )
            await connection.execute(
                message_prefilter_results.insert(),
                [
                    self._prefilter(raw_one, prefilter_jobs[0]["id"], now, "1"),
                    self._prefilter(raw_two, prefilter_jobs[1]["id"], now, "2"),
                ],
            )
            await connection.execute(
                opportunity_analysis_cache.insert(),
                [
                    self._analysis_cache(cache_one, "1"),
                    self._analysis_cache(cache_two, "2"),
                ],
            )
            await connection.execute(
                opportunities.insert(),
                [
                    self._opportunity(opportunity_one, raw_one, now, "project"),
                    self._opportunity(opportunity_two, raw_two, now, "vacancy"),
                ],
            )
            await connection.execute(
                opportunity_analysis_links.insert(),
                [
                    self._analysis_link(cache_one, opportunity_one, now, "1"),
                    self._analysis_link(cache_two, opportunity_two, now, "2"),
                ],
            )
            await connection.execute(
                opportunity_source_messages.insert(),
                [
                    {"raw_message_id": raw_one, "opportunity_id": opportunity_one},
                    {"raw_message_id": raw_two, "opportunity_id": opportunity_two},
                ],
            )
            await connection.execute(
                match_evaluation_runs.insert(),
                [
                    self._match_run(run_one, now, "1"),
                    self._match_run(run_two, now, "2"),
                ],
            )
            await connection.execute(
                match_traces.insert(),
                [
                    self._match_trace(trace_one, run_one, opportunity_one, profile_one, now, "1"),
                    self._match_trace(trace_two, run_two, opportunity_two, profile_two, now, "2"),
                ],
            )
            await connection.execute(
                personalized_deliveries.insert(),
                [
                    self._delivery(
                        delivery_one,
                        "1",
                        trace_one,
                        run_one,
                        opportunity_one,
                        profile_one,
                        user_one,
                        delivery_jobs[0]["id"],
                        now,
                    ),
                    self._delivery(
                        delivery_two,
                        "2",
                        trace_two,
                        run_two,
                        opportunity_two,
                        profile_two,
                        user_two,
                        delivery_jobs[1]["id"],
                        now,
                    ),
                ],
            )
            await connection.execute(
                delivery_action_events.insert(),
                [
                    self._action(
                        action_one,
                        "1",
                        "got_job",
                        delivery_one,
                        trace_one,
                        run_one,
                        opportunity_one,
                        profile_one,
                        user_one,
                        source_one,
                        raw_one,
                        now,
                    ),
                    self._action(
                        action_two,
                        "2",
                        "not_suitable",
                        delivery_two,
                        trace_two,
                        run_two,
                        opportunity_two,
                        profile_two,
                        user_two,
                        source_two,
                        raw_two,
                        now,
                    ),
                ],
            )
            await connection.execute(
                feedback_events.insert(),
                [
                    self._feedback(
                        uuid4(),
                        action_one,
                        "got_job",
                        "conversion",
                        delivery_one,
                        trace_one,
                        run_one,
                        opportunity_one,
                        "project",
                        profile_one,
                        user_one,
                        source_one,
                        raw_one,
                        now,
                    ),
                    self._feedback(
                        uuid4(),
                        action_two,
                        "not_suitable",
                        "personal_match",
                        delivery_two,
                        trace_two,
                        run_two,
                        opportunity_two,
                        "vacancy",
                        profile_two,
                        user_two,
                        source_two,
                        raw_two,
                        now,
                    ),
                ],
            )
            await connection.execute(
                source_quality_snapshots.insert(),
                [
                    self._snapshot(source_one, now, "0.8000000", "one"),
                    self._snapshot(source_two, now, "0.2000000", "two"),
                ],
            )

            report = await self.metrics.build_report(
                connection,
                window_started_at=self.started_at,
                window_ended_at=self.ended_at,
                profile_segment_labels={profile_one: "design", profile_two: "engineering"},
            )
            repeated_report = await self.metrics.build_report(
                connection,
                window_started_at=self.started_at,
                window_ended_at=self.ended_at,
                profile_segment_labels={profile_one: "design", profile_two: "engineering"},
            )

        self.assertEqual(report, repeated_report)
        self.assertEqual(
            report.funnel,
            report.funnel.__class__(
                messages=2,
                candidates=2,
                analyses=2,
                opportunities=2,
                matches=2,
                deliveries=2,
                sent_deliveries=2,
                feedback=2,
                not_suitable=1,
                got_job=1,
            ),
        )
        self.assertEqual(
            [(metric.source_id, metric.profile_segment, metric.opportunity_type, metric.won_lead_rate)
             for metric in report.won_lead_rate],
            [
                (source_one, "design", "project", Decimal("1.0000")),
                (source_two, "engineering", "vacancy", Decimal("0.0000")),
            ],
        )
        self.assertEqual([row.source_id for row in report.source_performance], [source_one, source_two])
        self.assertEqual(report.source_performance[0].quality_opportunity_yield, Decimal("0.8000000"))
        self.assertEqual(report.source_performance[0].rank, 1)
        self.assertEqual(report.source_performance[1].rank, 2)
        self.assertEqual(report.source_performance[1].opportunities, 1)
        self.assertEqual(report.source_performance[1].not_suitable_count, 1)
        self.assertEqual(report.unattributed_sent_deliveries, 0)
        self.assertEqual(report.ranking_dimensions[:2], ("quality_opportunity_yield", "pipeline_opportunity_yield"))

    async def _insert_source(self, suffix: str) -> int:
        async with self.database.transaction() as connection:
            return int(
                await connection.scalar(
                    sources.insert()
                    .values(
                        platform="telegram",
                        external_id=f"product-metrics:{suffix}",
                        access_type="public",
                        lifecycle_status="approved",
                        display_name=f"Product metrics source {suffix}",
                        handle=f"@product_metrics_{suffix}",
                    )
                    .returning(sources.c.id)
                )
            )

    async def _insert_collector(self) -> int:
        async with self.database.transaction() as connection:
            return int(
                await connection.scalar(
                    collector_accounts.insert()
                    .values(
                        platform="telegram",
                        external_account_id="product-metrics-collector",
                        display_name="Product metrics collector",
                    )
                    .returning(collector_accounts.c.id)
                )
            )

    async def _insert_profile(self, external_id: str, role: str):
        user_id, profile_id = uuid4(), uuid4()
        async with self.database.transaction() as connection:
            await connection.execute(
                users.insert().values(
                    id=user_id,
                    platform="telegram",
                    external_user_id=external_id,
                    created_at=self.started_at - timedelta(days=1),
                )
            )
            await connection.execute(
                search_profiles.insert().values(
                    id=profile_id,
                    user_id=user_id,
                    schema_version="search_profile.v1",
                    parser_version="search-profile-parser.v1",
                    roles=[self._term(role)],
                    skills=[self._term("Python")],
                    categories=[self._term("Software")],
                    semantic_text_original=role,
                    semantic_text_normalized=role,
                    preferences={
                        "schema_version": "search_profile_preferences.v1",
                        "work_types": None,
                        "minimum_budget": None,
                        "currency": None,
                        "budget_policy": None,
                        "languages": None,
                        "geographies": None,
                        "work_modes": None,
                        "excluded_categories": None,
                    },
                )
            )
        return user_id, profile_id

    def _job(self, key: str, now: datetime):
        return {
            "id": uuid4(),
            "job_type": "fixture.job",
            "idempotency_key": f"product-metrics:{key}",
            "correlation_id": uuid4(),
        }

    def _completed_job(self, key: str, now: datetime):
        return {
            "id": uuid4(),
            "job_type": "fixture.delivery",
            "idempotency_key": f"product-metrics:{key}",
            "state": "completed",
            "attempt_count": 1,
            "correlation_id": uuid4(),
            "claimed_at": None,
            "completed_at": now,
        }

    def _raw_message(self, message_id, source_id, collector_id, job_id, now, number):
        return {
            "id": message_id,
            "source_id": source_id,
            "collector_account_id": collector_id,
            "processing_job_id": job_id,
            "schema_version": "telegram.raw_message.v1",
            "platform": "telegram",
            "external_source_id": f"product-metrics:{number}",
            "external_message_id": number,
            "message_date": now,
            "observed_at": now,
            "message_url": f"https://t.me/product_metrics/{number}",
            "content": f"fixture opportunity {number}",
            "transport_metadata": {},
            "ingestion_origin": "live",
            "correlation_id": uuid4(),
        }

    def _prefilter(self, raw_id, job_id, now, suffix):
        return {
            "id": uuid4(),
            "raw_message_id": raw_id,
            "analysis_job_id": job_id,
            "schema_version": "message-prefilter.v1",
            "decision": "passed",
            "reason_codes": [],
            "normalized_content": f"fixture opportunity {suffix}",
            "normalized_content_sha256": suffix * 64,
            "analysis_input_sha256": ("a" if suffix == "1" else "b") * 64,
            "analyzer_version": "fixture-analyzer.v1",
            "analysis_schema_version": "opportunity_analysis.v1",
            "dedup_relation": "canonical",
            "dedup_window_seconds": 604800,
            "created_at": now,
        }

    def _analysis_cache(self, cache_id, suffix):
        return {
            "id": cache_id,
            "normalized_content": f"fixture opportunity {suffix}",
            "normalized_content_sha256": suffix * 64,
            "analysis_input_sha256": ("a" if suffix == "1" else "b") * 64,
            "analyzer_version": "fixture-analyzer.v1",
            "analysis_schema_version": "opportunity_analysis.v1",
            "result": {"is_opportunity": True},
            "created_at": self.started_at + timedelta(hours=2),
        }

    def _analysis_link(self, cache_id, opportunity_id, now, suffix):
        return {
            "analysis_cache_id": cache_id,
            "opportunity_id": opportunity_id,
            "dedup_relation": "canonical",
            "dedup_algorithm_version": "fixture-dedup.v1",
            "normalized_text_sha256": suffix * 64,
            "dedup_window_seconds": 604800,
            "linked_at": now,
        }

    def _opportunity(self, opportunity_id, raw_id, now, opportunity_type):
        return {
            "id": opportunity_id,
            "schema_version": "canonical_opportunity.v1",
            "canonical_title": "Fixture opportunity",
            "task_summary": "Fixture opportunity for metrics",
            "market_direction": "buyer_to_specialist",
            "intent_stage": "active",
            "opportunity_type": opportunity_type,
            "category": "Software",
            "role_title": "Specialist",
            "skills": ["Python"],
            "budget_known": False,
            "budget_explicit": False,
            "work_remote": True,
            "analysis_confidence": Decimal("0.9000"),
            "quality_actionability": Decimal("0.8000"),
            "quality_commercial_plausibility": Decimal("0.8000"),
            "quality_specificity": Decimal("0.8000"),
            "quality_credibility": Decimal("0.8000"),
            "red_flags": [],
            "first_seen_at": now,
            "last_seen_at": now,
            "preferred_raw_message_id": raw_id,
            "preferred_source_policy_version": "preferred-source.v1",
            "created_at": now,
            "updated_at": now,
            "lifecycle_changed_at": now,
        }

    def _match_run(self, run_id, now, suffix):
        return {
            "id": run_id,
            "idempotency_key": suffix * 64,
            "schema_version": "match-decision.v1",
            "algorithm_version": "match-algorithm.v1",
            "policy_version": "match-policy.v1",
            "policy_config": {},
            "evaluated_at": now,
            "trace_count": 1,
        }

    def _match_trace(self, trace_id, run_id, opportunity_id, profile_id, now, suffix):
        return {
            "id": trace_id,
            "run_id": run_id,
            "opportunity_id": opportunity_id,
            "search_profile_id": profile_id,
            "profile_revision": 1,
            "profile_schema_version": "search_profile.v1",
            "preferences_schema_version": "search_profile_preferences.v1",
            "input_sha256": ("c" if suffix == "1" else "d") * 64,
            "opportunity_lifecycle_status": "active",
            "opportunity_last_seen_at": now,
            "filter_version": "filter.v1",
            "hard_filter_eligible": True,
            "hard_filter_reasons": [],
            "nonblocking_unknowns": [],
            "structured_scoring_version": "structured.v1",
            "structured_policy_version": "structured-policy.v1",
            "structured_components": [],
            "user_relevance_score": Decimal("0.80000"),
            "structured_score": Decimal("0.80000"),
            "semantic_matching_version": "semantic.v1",
            "semantic_policy_version": "semantic-policy.v1",
            "semantic_status": "unavailable_input",
            "combined_relevance_score": Decimal("0.80000"),
            "opportunity_quality_score": Decimal("0.80000"),
            "source_quality_score": Decimal("0.80000"),
            "red_flag_penalty": Decimal("0.00000"),
            "base_combined_score": Decimal("0.80000"),
            "freshness_age_seconds": 60,
            "freshness_score": Decimal("1.00000"),
            "final_rank_score": Decimal("0.80000"),
            "minimum_relevance_threshold": Decimal("0.50000"),
            "minimum_rank_score_threshold": Decimal("0.50000"),
            "decision_code": "eligible",
            "eligible": True,
            "rank": 1,
            "decision_schema_version": "match-decision.v1",
            "decision_algorithm_version": "match-algorithm.v1",
            "decision_policy_version": "match-policy.v1",
            "evaluated_at": now,
        }

    def _delivery(self, delivery_id, suffix, trace_id, run_id, opportunity_id, profile_id, user_id, job_id, now):
        return {
            "id": delivery_id,
            "idempotency_key": suffix * 64,
            "schema_version": "personalized-delivery.v1",
            "renderer_schema_version": "telegram-lead-card.v1",
            "match_trace_id": trace_id,
            "match_run_id": run_id,
            "opportunity_id": opportunity_id,
            "search_profile_id": profile_id,
            "profile_revision": 1,
            "user_id": user_id,
            "recipient_platform": "telegram",
            "recipient_external_user_id": "100" + suffix,
            "job_id": job_id,
            "status": "sent",
            "card_body_html": "<b>Fixture opportunity</b>",
            "source_url": f"https://t.me/product_metrics/{suffix}",
            "parse_mode": "html",
            "link_preview": False,
            "rendered_at": now,
            "attempt_count": 1,
            "last_attempt_at": now,
            "telegram_message_id": 100 + int(suffix),
            "sent_at": now,
            "created_at": now,
            "updated_at": now,
        }

    def _action(self, action_id, suffix, action_type, delivery_id, trace_id, run_id, opportunity_id, profile_id, user_id, source_id, raw_id, now):
        return {
            "id": action_id,
            "idempotency_key": suffix * 64,
            "schema_version": "delivery-action.v1",
            "action_type": action_type,
            "delivery_id": delivery_id,
            "match_trace_id": trace_id,
            "match_run_id": run_id,
            "opportunity_id": opportunity_id,
            "search_profile_id": profile_id,
            "profile_revision": 1,
            "user_id": user_id,
            "source_id": source_id,
            "source_raw_message_id": raw_id,
            "source_url": f"https://t.me/product_metrics/{suffix}",
            "actor_platform": "telegram",
            "actor_external_user_id": "100" + suffix,
            "created_at": now,
        }

    def _feedback(self, feedback_id, action_id, feedback_type, signal_scope, delivery_id, trace_id, run_id, opportunity_id, opportunity_type, profile_id, user_id, source_id, raw_id, now):
        return {
            "id": feedback_id,
            "schema_version": "feedback.v1",
            "delivery_action_event_id": action_id,
            "feedback_type": feedback_type,
            "signal_scope": signal_scope,
            "delivery_id": delivery_id,
            "match_trace_id": trace_id,
            "match_run_id": run_id,
            "opportunity_id": opportunity_id,
            "opportunity_type": opportunity_type,
            "search_profile_id": profile_id,
            "profile_revision": 1,
            "user_id": user_id,
            "source_id": source_id,
            "source_raw_message_id": raw_id,
            "source_url": f"https://t.me/product_metrics/{opportunity_id}",
            "match_score": Decimal("0.80000"),
            "match_score_version": "match-algorithm.v1",
            "match_policy_version": "match-policy.v1",
            "feedback_at": now,
        }

    def _snapshot(self, source_id, now, yield_value, suffix):
        return {
            "source_id": source_id,
            "audit_key": f"product-metrics-audit:{suffix}",
            "audited_at": now,
            "window_started_at": self.started_at,
            "window_ended_at": now - timedelta(minutes=1),
            "sampled_message_count": 10,
            "opportunity_yield": Decimal(yield_value),
            "buyer_intent_ratio": Decimal("0.7000000"),
            "seller_ratio": Decimal("0.1000000"),
            "spam_ratio": Decimal("0.0500000"),
            "duplicate_ratio": Decimal("0.0500000"),
        }

    @staticmethod
    def _term(value: str):
        return {
            "value": value,
            "normalized_value": value.casefold(),
            "origin": "explicit",
            "evidence": value,
        }


if __name__ == "__main__":
    unittest.main()
