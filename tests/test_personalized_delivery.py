from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from hashlib import sha256
import io
import json
from types import SimpleNamespace
import unittest
from unittest import mock
from uuid import UUID, uuid4

import sqlalchemy as sa

from freelancer_bot.app import LeadBot, TelethonPersonalizedDeliverySender
from freelancer_bot.billing import TRIAL_POLICY_VERSION
from freelancer_bot.delivery import (
    PersonalizedDeliveryJobProcessor,
    PersonalizedDeliveryService,
    TelegramSendReceipt,
)
from freelancer_bot.delivery_actions import (
    DeliveryActionService,
    delivery_action_buttons,
    encode_delivery_action_callback,
)
from freelancer_bot.match_decisions import (
    MATCH_DECISION_ALGORITHM_VERSION,
    MatchDecisionPolicy,
)
from freelancer_bot.matching_service import CandidateMatchingService
from freelancer_bot.metrics import InMemoryMetrics, MetricNames
from freelancer_bot.observability import Redactor, configure_structured_logger
from freelancer_bot.opportunity_analysis import (
    OpenAIOpportunityAnalyzer,
    RoutedOpportunityAnalyzer,
)
from freelancer_bot.payment_provider import PaymentStatus, VerifiedPaymentEvent
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.delivery_actions import (
    DeliveryActionOwnershipError,
    DeliveryActionRepository,
    DeliveryActionType,
    DeliveryActionUnavailable,
)
from freelancer_bot.persistence.deliveries import (
    PERSONALIZED_DELIVERY_JOB_TYPE,
    DeliveryStatus,
    PersonalizedDeliveryRepository,
)
from freelancer_bot.persistence.feedback import (
    FeedbackRepository,
    FeedbackType,
    SourceFeedbackSignalRepository,
)
from freelancer_bot.persistence.entitlements import TrialEntitlementChecker
from freelancer_bot.persistence.jobs import DurableJobRepository
from freelancer_bot.persistence.matches import MatchTraceRepository
from freelancer_bot.persistence.payments import PaymentRepository
from freelancer_bot.persistence.search_profiles import SearchProfileRepository
from freelancer_bot.persistence.schema import (
    ai_call_telemetry,
    collector_accounts,
    delivery_action_events,
    durable_jobs,
    match_traces,
    opportunities,
    opportunity_source_messages,
    personalized_deliveries,
    raw_messages,
    search_profiles,
    sources,
    users,
)
from freelancer_bot.opportunity_dedup import PREFERRED_SOURCE_POLICY_VERSION
from freelancer_bot.profile_confirmation import ProfileConfirmationService
from freelancer_bot.search_profiles import (
    SEARCH_PROFILE_PARSER_VERSION,
    SEARCH_PROFILE_PREFERENCES_SCHEMA_VERSION,
    SEARCH_PROFILE_SCHEMA_VERSION,
)
from freelancer_bot.worker import DurableWorker, WorkerOptions
from postgres_support import TEST_DATABASE_URL, migrate_to_head, temporary_database


NOW = datetime(2026, 8, 14, 20, 0, tzinfo=timezone.utc)


class TelethonPersonalizedDeliverySenderTest(unittest.IsolatedAsyncioTestCase):
    async def test_adapter_sends_minimal_card_with_versioned_actions(self):
        client = _FakeBotClient()
        sender = TelethonPersonalizedDeliverySender(client)
        delivery_id = uuid4()

        receipt = await sender.send(
            recipient_chat_id=7000001,
            body_html="<b>Нужен Python-разработчик</b>",
            parse_mode="html",
            link_preview=False,
            idempotency_key="a" * 64,
            buttons=delivery_action_buttons(
                delivery_id,
                source_available=True,
            ),
        )

        self.assertEqual(receipt, TelegramSendReceipt(message_id=91))
        self.assertEqual(len(client.calls), 1)
        chat_id, body, kwargs = client.calls[0]
        self.assertEqual(chat_id, 7000001)
        self.assertEqual(body, "<b>Нужен Python-разработчик</b>")
        self.assertEqual(kwargs["parse_mode"], "html")
        self.assertFalse(kwargs["link_preview"])
        self.assertEqual(
            [[button.text for button in row] for row in kwargs["buttons"]],
            [["Открыть"], ["Не подходит", "Получил заказ"]],
        )
        callback_data = [
            button.data for row in kwargs["buttons"] for button in row
        ]
        self.assertTrue(all(len(data) <= 64 for data in callback_data))
        self.assertEqual(
            {data.decode("ascii").split(":")[1] for data in callback_data},
            {"o", "n", "g"},
        )
        self.assertTrue(
            all(data.decode("ascii").endswith(delivery_id.hex) for data in callback_data)
        )

    async def test_callback_records_action_then_installs_persisted_source_url(self):
        delivery_id = uuid4()
        bot = LeadBot.__new__(LeadBot)
        bot.delivery_actions = SimpleNamespace(
            record=mock.AsyncMock(
                return_value=SimpleNamespace(
                    event=SimpleNamespace(source_url="https://t.me/source/42")
                )
            )
        )
        event = SimpleNamespace(
            data=encode_delivery_action_callback(
                delivery_id,
                DeliveryActionType.OPEN,
            ),
            sender_id=7000010,
            chat_id=7000010,
            answer=mock.AsyncMock(),
            edit=mock.AsyncMock(),
        )

        await bot._handle_delivery_action_callback(event)

        bot.delivery_actions.record.assert_awaited_once_with(
            delivery_id=delivery_id,
            action_type=DeliveryActionType.OPEN,
            actor_external_user_id="7000010",
        )
        event.answer.assert_awaited_once_with(
            "Ссылка готова. Нажмите «Открыть» ещё раз"
        )
        event.edit.assert_awaited_once()
        edited_buttons = event.edit.await_args.kwargs["buttons"]
        self.assertEqual(edited_buttons[0][0].text, "Открыть")
        self.assertEqual(edited_buttons[0][0].url, "https://t.me/source/42")
        self.assertEqual(
            [button.text for button in edited_buttons[1]],
            ["Не подходит", "Получил заказ"],
        )

    async def test_callback_hides_cross_user_delivery(self):
        bot = LeadBot.__new__(LeadBot)
        bot.delivery_actions = SimpleNamespace(
            record=mock.AsyncMock(
                side_effect=DeliveryActionOwnershipError("private detail")
            )
        )
        event = SimpleNamespace(
            data=encode_delivery_action_callback(
                uuid4(),
                DeliveryActionType.GOT_JOB,
            ),
            sender_id=7000011,
            chat_id=7000011,
            answer=mock.AsyncMock(),
        )

        await bot._handle_delivery_action_callback(event)

        event.answer.assert_awaited_once_with(
            "Это действие вам недоступно",
            alert=True,
        )


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class PersonalizedDeliveryPostgresTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url, pool_size=8, max_overflow=12)
        self.metrics = InMemoryMetrics()
        self.log_output = io.StringIO()
        self.logger = configure_structured_logger(
            f"test.delivery.{id(self)}",
            redactor=Redactor(),
            stream=self.log_output,
        )

    async def asyncTearDown(self):
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def test_concurrent_schedule_delivers_each_profile_once_and_reuses_success(self):
        run_id, profile_ids, user_ids = await self._matched_run(
            external_user_ids=("7000001", "7000002"),
        )
        service = self._service()

        first, repeated = await asyncio.gather(
            service.schedule_run(run_id, rendered_at=NOW),
            service.schedule_run(run_id, rendered_at=NOW),
        )

        self.assertEqual(first.failures, ())
        self.assertEqual(repeated.failures, ())
        self.assertEqual(first.created_count + repeated.created_count, 2)
        self.assertEqual(first.reused_count + repeated.reused_count, 2)
        sender = _RecordingSender()
        await self._run_delivery_worker(
            sender,
            expected_delivery_status=DeliveryStatus.SENT,
            expected_count=2,
            worker_count=4,
        )

        records = await self._deliveries(run_id)
        self.assertEqual(len(records), 2)
        self.assertEqual({item.search_profile_id for item in records}, set(profile_ids))
        self.assertEqual({item.user_id for item in records}, set(user_ids))
        self.assertEqual(len({item.match_trace_id for item in records}), 2)
        self.assertEqual(len({item.idempotency_key for item in records}), 2)
        self.assertEqual({item.telegram_message_id for item in records}, {1001, 1002})
        self.assertEqual(sender.success_count, 2)
        self.assertEqual(
            {call["recipient_chat_id"] for call in sender.calls},
            {7000001, 7000002},
        )
        self.assertTrue(all("%" not in call["body_html"] for call in sender.calls))

        original_bodies = {item.card_body_html for item in records}
        after_success = await service.schedule_run(
            run_id,
            rendered_at=NOW + timedelta(hours=1),
        )
        self.assertEqual(after_success.created_count, 0)
        self.assertEqual(after_success.reused_count, 2)
        self.assertEqual(
            {item.delivery.card_body_html for item in after_success.deliveries},
            original_bodies,
        )
        await self._run_idle_worker(sender)
        self.assertEqual(sender.success_count, 2)

        async with self.database.connect() as connection:
            states = set(
                (
                    await connection.execute(
                        sa.select(durable_jobs.c.state).where(
                            durable_jobs.c.job_type
                            == PERSONALIZED_DELIVERY_JOB_TYPE
                        )
                    )
                ).scalars()
            )
            ai_calls = await connection.scalar(
                sa.select(sa.func.count()).select_from(ai_call_telemetry)
            )
        self.assertEqual(states, {"completed"})
        self.assertEqual(ai_calls, 0)

    async def test_logical_delivery_deduplicates_across_matching_runs(self):
        run_id, profile_ids, _ = await self._matched_run(
            external_user_ids=("7000015",)
        )
        service = self._service()
        first = await service.schedule_run(run_id, rendered_at=NOW)
        self.assertEqual(first.created_count, 1)
        opportunity_id = first.deliveries[0].delivery.opportunity_id

        second_matching = await CandidateMatchingService(self.database).generate_matches(
            (opportunity_id,),
            evaluated_at=NOW + timedelta(hours=1),
            decision_policy=MatchDecisionPolicy(
                minimum_relevance_score=Decimal("0.0000"),
                minimum_rank_score=Decimal("0.0000"),
            ),
        )
        second = await service.schedule_run(
            second_matching.persistence.run.id,
            rendered_at=NOW + timedelta(hours=1),
        )

        self.assertEqual(second.created_count, 0)
        self.assertEqual(second.reused_count, 1)
        async with self.database.connect() as connection:
            delivery_count = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(personalized_deliveries)
                .where(
                    personalized_deliveries.c.opportunity_id == opportunity_id,
                    personalized_deliveries.c.search_profile_id == profile_ids[0],
                    personalized_deliveries.c.profile_revision == 1,
                )
            )
            delivery_job_count = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(durable_jobs)
                .where(durable_jobs.c.job_type == PERSONALIZED_DELIVERY_JOB_TYPE)
            )
            trace_count = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(match_traces)
                .where(
                    match_traces.c.opportunity_id == opportunity_id,
                    match_traces.c.search_profile_id == profile_ids[0],
                )
            )
        self.assertEqual(delivery_count, 1)
        self.assertEqual(delivery_job_count, 1)
        self.assertEqual(trace_count, 2)

    async def test_concurrent_logical_delivery_collision_converges(self):
        run_id, profile_ids, _ = await self._matched_run(
            external_user_ids=("7000016",)
        )
        async with self.database.connect() as connection:
            first_match = (
                await MatchTraceRepository().list_eligible_for_run(
                    connection,
                    run_id,
                )
            )[0]
        second_matching = await CandidateMatchingService(self.database).generate_matches(
            (first_match.trace.opportunity_id,),
            evaluated_at=NOW + timedelta(hours=1),
            decision_policy=MatchDecisionPolicy(
                minimum_relevance_score=Decimal("0.0000"),
                minimum_rank_score=Decimal("0.0000"),
            ),
        )
        async with self.database.connect() as connection:
            second_match = (
                await MatchTraceRepository().list_eligible_for_run(
                    connection,
                    second_matching.persistence.run.id,
                )
            )[0]

        first, second = await asyncio.gather(
            self._service().schedule_trace(first_match.id, rendered_at=NOW),
            self._service().schedule_trace(
                second_match.id,
                rendered_at=NOW + timedelta(hours=1),
            ),
        )

        self.assertEqual(int(first.created) + int(second.created), 1)
        self.assertEqual(int(first.created is False) + int(second.created is False), 1)
        async with self.database.connect() as connection:
            delivery_count = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(personalized_deliveries)
                .where(
                    personalized_deliveries.c.opportunity_id
                    == first_match.trace.opportunity_id,
                    personalized_deliveries.c.search_profile_id == profile_ids[0],
                    personalized_deliveries.c.profile_revision == 1,
                )
            )
            delivery_job_count = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(durable_jobs)
                .where(durable_jobs.c.job_type == PERSONALIZED_DELIVERY_JOB_TYPE)
            )
        self.assertEqual(delivery_count, 1)
        self.assertEqual(delivery_job_count, 1)

    async def test_legacy_trace_key_reuses_logical_delivery(self):
        run_id, _, _ = await self._matched_run(external_user_ids=("7000017",))
        service = self._service()
        first = await service.schedule_run(run_id, rendered_at=NOW)
        delivery = first.deliveries[0].delivery
        async with self.database.connect() as connection:
            input_sha256 = await connection.scalar(
                sa.select(match_traces.c.input_sha256).where(
                    match_traces.c.id == delivery.match_trace_id
                )
            )
        legacy_payload = {
            "delivery_schema_version": "personalized-delivery.v1",
            "renderer_schema_version": delivery.renderer_schema_version,
            "match_trace_id": str(delivery.match_trace_id),
            "match_run_id": str(delivery.match_run_id),
            "match_input_sha256": input_sha256,
            "opportunity_id": str(delivery.opportunity_id),
            "search_profile_id": str(delivery.search_profile_id),
            "profile_revision": delivery.profile_revision,
        }
        legacy_key = sha256(
            json.dumps(
                legacy_payload,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        async with self.database.transaction() as connection:
            await connection.execute(
                durable_jobs.update()
                .where(durable_jobs.c.id == delivery.job_id)
                .values(idempotency_key=legacy_key)
            )
            await connection.execute(
                personalized_deliveries.update()
                .where(personalized_deliveries.c.id == delivery.id)
                .values(idempotency_key=legacy_key)
            )

        repeated = await service.schedule_run(run_id, rendered_at=NOW)

        self.assertEqual(repeated.created_count, 0)
        self.assertEqual(repeated.reused_count, 1)
        async with self.database.connect() as connection:
            delivery_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(personalized_deliveries)
            )
            delivery_job_count = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(durable_jobs)
                .where(durable_jobs.c.job_type == PERSONALIZED_DELIVERY_JOB_TYPE)
            )
        self.assertEqual(delivery_count, 1)
        self.assertEqual(delivery_job_count, 1)

    async def test_non_primary_profile_cannot_schedule_existing_match(self):
        run_id, profile_ids, _ = await self._matched_run(
            external_user_ids=("7000018",)
        )
        async with self.database.transaction() as connection:
            await connection.execute(
                search_profiles.update()
                .where(search_profiles.c.id == profile_ids[0])
                .values(is_primary=False, is_active=True)
            )

        report = await self._service().schedule_run(run_id, rendered_at=NOW)

        self.assertEqual(report.deliveries, ())
        self.assertEqual(len(report.failures), 1)
        self.assertEqual(report.failures[0].failure_code, "DeliverySchedulingError")
        async with self.database.connect() as connection:
            self.assertEqual(
                await connection.scalar(
                    sa.select(sa.func.count()).select_from(personalized_deliveries)
                ),
                0,
            )

    async def test_non_primary_profile_is_suppressed_before_send(self):
        run_id, profile_ids, _ = await self._matched_run(
            external_user_ids=("7000019",)
        )
        report = await self._service().schedule_run(run_id, rendered_at=NOW)
        self.assertEqual(report.created_count, 1)
        async with self.database.transaction() as connection:
            await connection.execute(
                search_profiles.update()
                .where(search_profiles.c.id == profile_ids[0])
                .values(is_primary=False, is_active=True)
            )

        sender = _RecordingSender()
        await self._run_delivery_worker(
            sender,
            expected_delivery_status=DeliveryStatus.SUPPRESSED,
            expected_count=1,
        )

        delivery = (await self._deliveries(run_id))[0]
        self.assertEqual(delivery.failure_code, "ProfileIneligible")
        self.assertEqual(sender.attempt_count, 0)

    async def test_historical_active_profile_is_excluded_and_reconciled(self):
        run_id, profile_ids, _ = await self._matched_run(
            external_user_ids=("7000020",)
        )
        report = await self._service().schedule_run(run_id, rendered_at=NOW)
        self.assertEqual(report.created_count, 1)
        profile_service = ProfileConfirmationService(self.database)
        draft = await profile_service.create_manual_draft(
            platform="telegram",
            external_user_id="7000020",
            semantic_text="Python developer | Python | Telegram",
            roles=("Python developer",),
            skills=("Python",),
            categories=("Telegram",),
        )
        confirmed = await profile_service.confirm(
            platform="telegram",
            external_user_id="7000020",
            profile_id=draft.profile.id,
            expected_revision=draft.profile.revision,
        )
        async with self.database.transaction() as connection:
            await connection.execute(
                search_profiles.update()
                .where(search_profiles.c.id == profile_ids[0])
                .values(is_active=True, is_primary=False)
            )
            await connection.execute(
                search_profiles.update()
                .where(search_profiles.c.id == confirmed.profile.id)
                .values(
                    is_active=True,
                    is_primary=True,
                    activated_at=NOW,
                    deactivated_at=None,
                )
            )
            active_profiles = await SearchProfileRepository().list_active(connection)

        self.assertEqual(
            tuple(profile.id for profile in active_profiles),
            (confirmed.profile.id,),
        )
        await self._run_delivery_worker(
            _RecordingSender(),
            expected_delivery_status=DeliveryStatus.SUPPRESSED,
            expected_count=1,
        )

        repeated = await profile_service.activate(
            platform="telegram",
            external_user_id="7000020",
            profile_id=confirmed.profile.id,
            expected_revision=confirmed.profile.revision,
        )
        self.assertFalse(repeated.trial_started)
        profiles = await profile_service.list_profiles(
            platform="telegram",
            external_user_id="7000020",
        )
        by_id = {view.profile.id: view.profile for view in profiles}
        self.assertFalse(by_id[profile_ids[0]].is_active)
        self.assertFalse(by_id[profile_ids[0]].is_primary)
        self.assertIsNotNone(by_id[profile_ids[0]].deactivated_at)
        self.assertTrue(by_id[confirmed.profile.id].is_active)
        self.assertTrue(by_id[confirmed.profile.id].is_primary)

    async def test_expired_trial_rejects_new_delivery_scheduling(self):
        run_id, _, user_ids = await self._matched_run(external_user_ids=("7000012",))
        await self._expire_trial(user_ids[0])

        report = await self._service().schedule_run(run_id, rendered_at=NOW)

        self.assertEqual(report.deliveries, ())
        self.assertEqual(len(report.failures), 1)
        self.assertEqual(report.failures[0].failure_code, "DeliverySchedulingError")
        async with self.database.connect() as connection:
            delivery_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(personalized_deliveries)
            )
        self.assertEqual(delivery_count, 0)

    async def test_verified_paid_period_restores_scheduling_after_expired_trial(self):
        run_id, _, user_ids = await self._matched_run(external_user_ids=("7000014",))
        await self._expire_trial(user_ids[0])
        event = VerifiedPaymentEvent(
            provider="robokassa",
            provider_event_id="delivery-paid-period-event",
            event_type="payment.succeeded",
            provider_payment_id="delivery-paid-period-payment",
            user_id=user_ids[0],
            status=PaymentStatus.SUCCEEDED,
            amount=Decimal("1250"),
            currency="RUB",
            period_start_at=NOW - timedelta(hours=1),
            period_end_at=NOW + timedelta(days=30),
            occurred_at=NOW,
            received_at=NOW,
            payload={"fixture": "authoritative"},
            verification_version="fixture-authoritative.v1",
        )
        async with self.database.transaction() as connection:
            outcome = await PaymentRepository().record_verified_event(
                connection,
                event,
            )

        report = await self._service().schedule_run(run_id, rendered_at=NOW)

        self.assertTrue(outcome.period_created)
        self.assertEqual(report.created_count, 1)
        self.assertEqual(report.failures, ())

    async def test_trial_expiry_suppresses_queued_delivery_without_sending(self):
        run_id, _, user_ids = await self._matched_run(external_user_ids=("7000013",))
        report = await self._service().schedule_run(run_id, rendered_at=NOW)
        self.assertEqual(report.created_count, 1)

        await self._expire_trial(user_ids[0])
        sender = _RecordingSender()
        await self._run_delivery_worker(
            sender,
            expected_delivery_status=DeliveryStatus.SUPPRESSED,
            expected_count=1,
        )

        delivery = (await self._deliveries(run_id))[0]
        self.assertEqual(delivery.status, DeliveryStatus.SUPPRESSED)
        self.assertEqual(delivery.failure_code, "TrialExpired")
        self.assertEqual(sender.success_count, 0)

    async def test_transient_failure_retries_then_records_one_confirmed_message(self):
        run_id, _, _ = await self._matched_run(external_user_ids=("7000002",))
        report = await self._service().schedule_run(run_id, rendered_at=NOW)
        self.assertEqual(report.created_count, 1)
        sender = _RecordingSender(failures_before_success=1)

        await self._run_delivery_worker(
            sender,
            expected_delivery_status=DeliveryStatus.SENT,
            expected_count=1,
        )

        delivery = (await self._deliveries(run_id))[0]
        self.assertEqual(sender.attempt_count, 2)
        self.assertEqual(sender.success_count, 1)
        self.assertEqual(delivery.status, DeliveryStatus.SENT)
        self.assertEqual(delivery.attempt_count, 2)
        self.assertEqual(delivery.telegram_message_id, 1001)
        self.assertIsNotNone(delivery.sent_at)
        self.assertIsNone(delivery.failure_code)
        self.assertEqual(
            self.metrics.counter(
                MetricNames.DELIVERIES_RETRIED,
                tags={"failure_code": "ConnectionError"},
            ),
            1,
        )
        self.assertEqual(self.metrics.counter(MetricNames.DELIVERIES_SENT), 1)

    async def test_terminal_failure_is_bounded_and_auditable(self):
        run_id, _, _ = await self._matched_run(external_user_ids=("7000003",))
        await self._service().schedule_run(run_id, rendered_at=NOW)
        sender = _RecordingSender(failures_before_success=10)

        await self._run_delivery_worker(
            sender,
            expected_delivery_status=DeliveryStatus.FAILED,
            expected_count=1,
        )

        delivery = (await self._deliveries(run_id))[0]
        self.assertEqual(sender.attempt_count, 3)
        self.assertEqual(sender.success_count, 0)
        self.assertEqual(delivery.attempt_count, 3)
        self.assertEqual(delivery.status, DeliveryStatus.FAILED)
        self.assertEqual(delivery.failure_code, "ConnectionError")
        async with self.database.connect() as connection:
            job = (
                await connection.execute(
                    sa.select(durable_jobs).where(durable_jobs.c.id == delivery.job_id)
                )
            ).mappings().one()
        self.assertEqual(job["state"], "failed")
        self.assertEqual(job["attempt_count"], 3)
        self.assertEqual(job["failure_code"], "DeliverySendError")
        self.assertNotIn("Build private Telegram automation", self.log_output.getvalue())

    async def test_changed_opportunity_is_suppressed_before_send(self):
        run_id, _, _ = await self._matched_run(external_user_ids=("7000004",))
        report = await self._service().schedule_run(run_id, rendered_at=NOW)
        opportunity_id = report.deliveries[0].delivery.opportunity_id
        async with self.database.transaction() as connection:
            await connection.execute(
                opportunities.update()
                .where(opportunities.c.id == opportunity_id)
                .values(
                    lifecycle_status="stale",
                    lifecycle_changed_at=NOW + timedelta(minutes=1),
                    updated_at=sa.func.now(),
                )
            )
        sender = _RecordingSender()

        await self._run_delivery_worker(
            sender,
            expected_delivery_status=DeliveryStatus.SUPPRESSED,
            expected_count=1,
        )

        delivery = (await self._deliveries(run_id))[0]
        self.assertEqual(delivery.failure_code, "OpportunityIneligible")
        self.assertEqual(sender.attempt_count, 0)
        async with self.database.connect() as connection:
            state = await connection.scalar(
                sa.select(durable_jobs.c.state).where(
                    durable_jobs.c.id == delivery.job_id
                )
            )
        self.assertEqual(state, "completed")

    async def test_deactivated_profile_is_suppressed_before_send(self):
        run_id, profile_ids, _ = await self._matched_run(
            external_user_ids=("7000006",)
        )
        await self._service().schedule_run(run_id, rendered_at=NOW)
        async with self.database.transaction() as connection:
            await connection.execute(
                search_profiles.update()
                .where(search_profiles.c.id == profile_ids[0])
                .values(
                    is_active=False,
                    is_primary=False,
                    deactivated_at=NOW + timedelta(minutes=1),
                    updated_at=sa.func.now(),
                )
            )
        sender = _RecordingSender()

        await self._run_delivery_worker(
            sender,
            expected_delivery_status=DeliveryStatus.SUPPRESSED,
            expected_count=1,
        )

        delivery = (await self._deliveries(run_id))[0]
        self.assertEqual(delivery.failure_code, "ProfileIneligible")
        self.assertEqual(sender.attempt_count, 0)

    async def test_expired_pre_send_attempt_is_reclaimed_and_sent_once(self):
        run_id, _, _ = await self._matched_run(external_user_ids=("7000007",))
        await self._service().schedule_run(run_id, rendered_at=NOW)
        jobs = DurableJobRepository(self.metrics)
        deliveries = PersonalizedDeliveryRepository(
            entitlement_checker=TrialEntitlementChecker(clock=lambda: NOW),
        )
        async with self.database.transaction() as connection:
            first_claim = await jobs.claim_next(
                connection,
                worker_id="crashed-delivery-worker",
                lease_duration=timedelta(seconds=30),
                job_types=(PERSONALIZED_DELIVERY_JOB_TYPE,),
            )
            self.assertIsNotNone(first_claim)
            attempt = await deliveries.prepare_attempt(connection, first_claim)
            self.assertIsNotNone(attempt)
            await connection.execute(
                durable_jobs.update()
                .where(durable_jobs.c.id == first_claim.id)
                .values(
                    lease_expires_at=sa.func.now() - sa.text("INTERVAL '1 second'")
                )
            )

        sender = _RecordingSender()
        await self._run_delivery_worker(
            sender,
            expected_delivery_status=DeliveryStatus.SENT,
            expected_count=1,
        )

        delivery = (await self._deliveries(run_id))[0]
        self.assertEqual(delivery.attempt_count, 2)
        self.assertEqual(sender.attempt_count, 1)
        self.assertEqual(sender.success_count, 1)
        self.assertEqual(
            self.metrics.counter(
                MetricNames.JOBS_LEASE_RECLAIMED,
                tags={"job_type": PERSONALIZED_DELIVERY_JOB_TYPE},
            ),
            1,
        )

    async def test_invalid_recipient_does_not_rollback_valid_delivery(self):
        run_id, _, _ = await self._matched_run(
            external_user_ids=("7000005", "not-a-chat-id"),
        )

        report = await self._service().schedule_run(run_id, rendered_at=NOW)

        self.assertEqual(len(report.deliveries), 1)
        self.assertEqual(report.created_count, 1)
        self.assertEqual(len(report.failures), 1)
        self.assertEqual(report.failures[0].failure_code, "ValueError")
        self.assertEqual(len(await self._deliveries(run_id)), 1)

    async def test_actions_are_owned_source_linked_and_concurrently_idempotent(self):
        run_id, profile_ids, user_ids = await self._matched_run(
            external_user_ids=("7000008",)
        )
        await self._service().schedule_run(run_id, rendered_at=NOW)
        sender = _RecordingSender()
        await self._run_delivery_worker(
            sender,
            expected_delivery_status=DeliveryStatus.SENT,
            expected_count=1,
            worker_count=2,
        )
        delivery = (await self._deliveries(run_id))[0]
        actions = DeliveryActionService(
            self.database,
            metrics=self.metrics,
            logger=self.logger,
        )

        opens = await asyncio.gather(
            *(
                actions.record(
                    delivery_id=delivery.id,
                    action_type=DeliveryActionType.OPEN,
                    actor_external_user_id="7000008",
                )
                for _ in range(8)
            )
        )
        not_suitable_attempts = await asyncio.gather(
            *(
                actions.record(
                    delivery_id=delivery.id,
                    action_type=DeliveryActionType.NOT_SUITABLE,
                    actor_external_user_id="7000008",
                )
                for _ in range(8)
            )
        )
        got_job_attempts = await asyncio.gather(
            *(
                actions.record(
                    delivery_id=delivery.id,
                    action_type=DeliveryActionType.GOT_JOB,
                    actor_external_user_id="7000008",
                )
                for _ in range(8)
            )
        )

        self.assertEqual(sum(item.created for item in opens), 1)
        self.assertEqual(len({item.event.id for item in opens}), 1)
        self.assertEqual(sum(item.created for item in not_suitable_attempts), 1)
        self.assertEqual(sum(item.created for item in got_job_attempts), 1)
        async with self.database.connect() as connection:
            records = await DeliveryActionRepository().list_for_delivery(
                connection,
                delivery.id,
            )
            current_delivery = await PersonalizedDeliveryRepository().get(
                connection,
                delivery.id,
            )
            feedback = await FeedbackRepository().list_for_delivery(
                connection,
                delivery.id,
            )
            source_signal = await SourceFeedbackSignalRepository().get(
                connection,
                records[0].source_id,
            )
            opportunity_lifecycle = await connection.scalar(
                sa.select(opportunities.c.lifecycle_status).where(
                    opportunities.c.id == delivery.opportunity_id
                )
            )
            ai_calls = await connection.scalar(
                sa.select(sa.func.count()).select_from(ai_call_telemetry)
            )
        self.assertEqual(
            {item.action_type for item in records},
            {
                DeliveryActionType.OPEN,
                DeliveryActionType.NOT_SUITABLE,
                DeliveryActionType.GOT_JOB,
            },
        )
        self.assertTrue(all(item.delivery_id == delivery.id for item in records))
        self.assertTrue(
            all(item.match_trace_id == delivery.match_trace_id for item in records)
        )
        self.assertTrue(
            all(item.opportunity_id == delivery.opportunity_id for item in records)
        )
        self.assertTrue(
            all(item.search_profile_id == profile_ids[0] for item in records)
        )
        self.assertTrue(all(item.user_id == user_ids[0] for item in records))
        self.assertTrue(all(item.source_id > 0 for item in records))
        self.assertTrue(
            all(item.source_url == delivery.source_url for item in records)
        )
        self.assertEqual(
            {item.feedback_type for item in feedback},
            {FeedbackType.NOT_SUITABLE, FeedbackType.GOT_JOB},
        )
        self.assertTrue(
            all(item.delivery_id == delivery.id for item in feedback)
        )
        self.assertTrue(
            all(item.match_trace_id == delivery.match_trace_id for item in feedback)
        )
        self.assertTrue(all(item.match_score >= 0 for item in feedback))
        self.assertTrue(
            all(
                item.match_score_version == MATCH_DECISION_ALGORITHM_VERSION
                for item in feedback
            )
        )
        self.assertEqual(opportunity_lifecycle, "active")
        self.assertIsNotNone(source_signal)
        self.assertEqual(source_signal.feedback_count, 2)
        self.assertEqual(source_signal.not_suitable_count, 1)
        self.assertEqual(source_signal.got_job_count, 1)
        self.assertEqual(source_signal.conversion_count, 1)
        self.assertEqual(current_delivery.status, DeliveryStatus.SENT)
        self.assertEqual(sender.success_count, 1)
        self.assertEqual(ai_calls, 0)
        self.assertEqual(
            self.metrics.counter(
                MetricNames.DELIVERY_ACTIONS,
                tags={"action_type": DeliveryActionType.OPEN.value},
            ),
            1,
        )
        self.assertEqual(
            self.metrics.counter(
                MetricNames.DELIVERY_ACTIONS_REUSED,
                tags={"action_type": DeliveryActionType.OPEN.value},
            ),
            7,
        )
        self.assertEqual(
            self.metrics.counter(
                MetricNames.FEEDBACK,
                tags={"action_type": DeliveryActionType.GOT_JOB.value},
            ),
            1,
        )
        action_logs = self.log_output.getvalue()
        self.assertNotIn("7000008", action_logs)
        self.assertNotIn(delivery.source_url, action_logs)
        self.assertNotIn("Build private Telegram automation", action_logs)

    async def test_feedback_keeps_multiple_search_profiles_isolated(self):
        run_id, profile_ids, user_ids = await self._matched_run(
            external_user_ids=("7000011", "7000012"),
        )
        await self._service().schedule_run(run_id, rendered_at=NOW)
        await self._run_delivery_worker(
            _RecordingSender(),
            expected_delivery_status=DeliveryStatus.SENT,
            expected_count=2,
            worker_count=2,
        )
        deliveries = await self._deliveries(run_id)
        actions = DeliveryActionService(self.database)
        await asyncio.gather(
            *(
                actions.record(
                    delivery_id=delivery.id,
                    action_type=DeliveryActionType.NOT_SUITABLE,
                    actor_external_user_id=delivery.recipient_external_user_id,
                )
                for delivery in deliveries
            )
        )

        async with self.database.connect() as connection:
            feedback_by_delivery = [
                await FeedbackRepository().list_for_delivery(connection, delivery.id)
                for delivery in deliveries
            ]
            feedback = tuple(
                item for delivery_feedback in feedback_by_delivery for item in delivery_feedback
            )
            source_signal = await SourceFeedbackSignalRepository().get(
                connection,
                feedback[0].source_id,
            )

        self.assertEqual({item.search_profile_id for item in feedback}, set(profile_ids))
        self.assertEqual({item.user_id for item in feedback}, set(user_ids))
        self.assertEqual({item.signal_scope for item in feedback}, {"personal_match"})
        self.assertEqual(source_signal.feedback_count, 2)
        self.assertEqual(source_signal.not_suitable_count, 2)
        self.assertEqual(source_signal.got_job_count, 0)

    async def test_actions_reject_unsent_or_wrong_actor_but_keep_stale_history(self):
        run_id, profile_ids, _ = await self._matched_run(
            external_user_ids=("7000009",)
        )
        await self._service().schedule_run(run_id, rendered_at=NOW)
        delivery = (await self._deliveries(run_id))[0]
        actions = DeliveryActionService(self.database)

        with self.assertRaises(DeliveryActionUnavailable):
            await actions.record(
                delivery_id=delivery.id,
                action_type=DeliveryActionType.NOT_SUITABLE,
                actor_external_user_id="7000009",
            )
        await self._run_delivery_worker(
            _RecordingSender(),
            expected_delivery_status=DeliveryStatus.SENT,
            expected_count=1,
        )
        with self.assertRaises(DeliveryActionOwnershipError):
            await actions.record(
                delivery_id=delivery.id,
                action_type=DeliveryActionType.NOT_SUITABLE,
                actor_external_user_id="7999999",
            )
        with self.assertRaises(DeliveryActionUnavailable):
            await actions.record(
                delivery_id=uuid4(),
                action_type=DeliveryActionType.OPEN,
                actor_external_user_id="7000009",
            )

        first = await actions.record(
            delivery_id=delivery.id,
            action_type=DeliveryActionType.NOT_SUITABLE,
            actor_external_user_id="7000009",
        )
        async with self.database.transaction() as connection:
            await connection.execute(
                opportunities.update()
                .where(opportunities.c.id == delivery.opportunity_id)
                .values(
                    lifecycle_status="closed",
                    lifecycle_changed_at=NOW + timedelta(minutes=1),
                    updated_at=sa.func.now(),
                )
            )
            await connection.execute(
                search_profiles.update()
                .where(search_profiles.c.id == profile_ids[0])
                .values(
                    is_active=False,
                    is_primary=False,
                    deactivated_at=NOW + timedelta(minutes=1),
                    updated_at=sa.func.now(),
                )
            )
        replay = await actions.record(
            delivery_id=delivery.id,
            action_type=DeliveryActionType.NOT_SUITABLE,
            actor_external_user_id="7000009",
        )
        conversion = await actions.record(
            delivery_id=delivery.id,
            action_type=DeliveryActionType.GOT_JOB,
            actor_external_user_id="7000009",
        )

        self.assertEqual(replay.event.id, first.event.id)
        self.assertFalse(replay.created)
        self.assertTrue(conversion.created)
        async with self.database.connect() as connection:
            count = await connection.scalar(
                sa.select(sa.func.count()).select_from(delivery_action_events)
            )
        self.assertEqual(count, 2)

    async def _matched_run(
        self,
        *,
        external_user_ids: tuple[str, ...],
        same_user: bool = False,
    ) -> tuple[UUID, tuple[UUID, ...], tuple[UUID, ...]]:
        if same_user and len(set(external_user_ids)) != 1:
            raise ValueError("same-user fixture requires one Telegram identity")
        user_ids = (
            (uuid4(),) * len(external_user_ids)
            if same_user
            else tuple(uuid4() for _ in external_user_ids)
        )
        unique_users = tuple(dict.fromkeys(zip(user_ids, external_user_ids, strict=True)))
        profile_ids = tuple(uuid4() for _ in external_user_ids)
        opportunity_id = uuid4()
        raw_message_id = uuid4()
        raw_job_id = uuid4()
        correlation_id = uuid4()
        source_key = opportunity_id.hex
        source_url = f"https://t.me/delivery_{source_key}/42"
        preferences = {
            "schema_version": SEARCH_PROFILE_PREFERENCES_SCHEMA_VERSION,
            "work_types": ["project"],
            "minimum_budget": None,
            "currency": None,
            "budget_policy": "allow_unknown",
            "languages": None,
            "geographies": None,
            "work_modes": ["remote"],
            "excluded_categories": None,
        }
        async with self.database.transaction() as connection:
            await connection.execute(
                users.insert(),
                tuple(
                    {
                        "id": user_id,
                        "platform": "telegram",
                        "external_user_id": external_id,
                        "created_at": NOW - timedelta(days=5),
                        "trial_started_at": NOW - timedelta(minutes=2),
                        "trial_expires_at": NOW + timedelta(days=3) - timedelta(minutes=2),
                        "trial_policy_version": TRIAL_POLICY_VERSION,
                    }
                    for user_id, external_id in unique_users
                ),
            )
            await connection.execute(
                search_profiles.insert(),
                tuple(
                    {
                        "id": profile_id,
                        "user_id": user_id,
                        "schema_version": SEARCH_PROFILE_SCHEMA_VERSION,
                        "parser_version": SEARCH_PROFILE_PARSER_VERSION,
                        "roles": [_term("Python developer")],
                        "skills": [_term("Python"), _term("Telegram API")],
                        "categories": [_term("Telegram")],
                        "semantic_text_original": "Python Telegram automation",
                        "semantic_text_normalized": "Python Telegram automation",
                        "preferences": preferences,
                        "confirmation_status": "confirmed",
                        "revision": 1,
                        "confirmed_at": NOW - timedelta(minutes=2),
                        "is_active": True,
                        "is_primary": (not same_user or index == 0),
                        "activated_at": NOW - timedelta(minutes=1),
                    }
                    for index, (user_id, profile_id) in enumerate(
                        zip(user_ids, profile_ids, strict=True)
                    )
                ),
            )
            collector_account_id = await connection.scalar(
                collector_accounts.insert()
                .values(
                    platform="telegram",
                    external_account_id=f"delivery:{source_key}",
                    display_name="Delivery fixture collector",
                )
                .returning(collector_accounts.c.id)
            )
            source_id = await connection.scalar(
                sources.insert()
                .values(
                    platform="telegram",
                    external_id=f"username:delivery_{source_key}",
                    access_type="public",
                    lifecycle_status="approved",
                    display_name="Delivery fixture source",
                    handle=f"@delivery_{source_key}",
                    canonical_url=f"https://t.me/delivery_{source_key}",
                )
                .returning(sources.c.id)
            )
            await connection.execute(
                durable_jobs.insert().values(
                    id=raw_job_id,
                    job_type="telegram.raw_message.v1",
                    idempotency_key=f"delivery-fixture:{source_key}",
                    correlation_id=correlation_id,
                )
            )
            await connection.execute(
                raw_messages.insert().values(
                    id=raw_message_id,
                    source_id=source_id,
                    collector_account_id=collector_account_id,
                    processing_job_id=raw_job_id,
                    schema_version="telegram.raw_message.v1",
                    platform="telegram",
                    external_source_id=f"username:delivery_{source_key}",
                    external_message_id=42,
                    message_date=NOW - timedelta(minutes=5),
                    observed_at=NOW - timedelta(minutes=5),
                    message_url=source_url,
                    content="Build private Telegram automation",
                    transport_metadata={},
                    ingestion_origin="live",
                    correlation_id=correlation_id,
                )
            )
            await connection.execute(
                opportunities.insert().values(
                    id=opportunity_id,
                    schema_version="canonical_opportunity.v1",
                    canonical_title="Нужен Python-разработчик",
                    task_summary="Build private Telegram automation",
                    market_direction="buyer_to_specialist",
                    intent_stage="active",
                    opportunity_type="project",
                    category="Telegram",
                    role_title="Python developer",
                    skills=["Python", "Telegram API"],
                    budget_known=False,
                    budget_explicit=False,
                    work_remote=True,
                    analysis_confidence=Decimal("0.9000"),
                    quality_actionability=Decimal("0.8000"),
                    quality_commercial_plausibility=Decimal("0.8000"),
                    quality_specificity=Decimal("0.8000"),
                    quality_credibility=Decimal("0.8000"),
                    red_flags=[],
                    first_seen_at=NOW - timedelta(minutes=5),
                    last_seen_at=NOW - timedelta(minutes=5),
                    lifecycle_status="active",
                    lifecycle_changed_at=NOW - timedelta(minutes=5),
                    preferred_raw_message_id=raw_message_id,
                    preferred_source_policy_version=PREFERRED_SOURCE_POLICY_VERSION,
                )
            )
            await connection.execute(
                opportunity_source_messages.insert().values(
                    raw_message_id=raw_message_id,
                    opportunity_id=opportunity_id,
                )
            )
        with (
            mock.patch.object(
                OpenAIOpportunityAnalyzer,
                "analyze",
                side_effect=AssertionError("delivery fixture invoked AI"),
            ),
            mock.patch.object(
                RoutedOpportunityAnalyzer,
                "analyze",
                side_effect=AssertionError("delivery fixture invoked routed AI"),
            ),
        ):
            generated = await CandidateMatchingService(self.database).generate_matches(
                (opportunity_id,),
                evaluated_at=NOW,
                decision_policy=MatchDecisionPolicy(
                    minimum_relevance_score=Decimal("0.0000"),
                    minimum_rank_score=Decimal("0.0000"),
                ),
            )
        self.assertEqual(generated.report.user_specific_llm_calls, 0)
        return generated.persistence.run.id, profile_ids, user_ids

    async def _expire_trial(self, user_id: UUID) -> None:
        async with self.database.transaction() as connection:
            await connection.execute(
                users.update()
                .where(users.c.id == user_id)
                .values(
                    trial_started_at=NOW - timedelta(days=4),
                    trial_expires_at=NOW - timedelta(days=1),
                    trial_policy_version=TRIAL_POLICY_VERSION,
                )
            )

    def _service(self) -> PersonalizedDeliveryService:
        jobs = DurableJobRepository(self.metrics)
        return PersonalizedDeliveryService(
            self.database,
            jobs=jobs,
            entitlement_checker=TrialEntitlementChecker(clock=lambda: NOW),
            metrics=self.metrics,
            logger=self.logger,
        )

    async def _deliveries(self, run_id: UUID):
        async with self.database.connect() as connection:
            return await PersonalizedDeliveryRepository().list_for_run(
                connection,
                run_id,
            )

    async def _run_delivery_worker(
        self,
        sender: "_RecordingSender",
        *,
        expected_delivery_status: DeliveryStatus,
        expected_count: int,
        worker_count: int = 1,
    ) -> None:
        jobs = DurableJobRepository(self.metrics)
        processor = PersonalizedDeliveryJobProcessor(
            self.database,
            sender,
            entitlement_checker=TrialEntitlementChecker(clock=lambda: NOW),
            metrics=self.metrics,
            logger=self.logger,
        )
        workers = tuple(
            DurableWorker(
                self.database,
                repository=jobs,
                worker_id=f"delivery-test-{uuid4().hex[:8]}",
                handlers={PERSONALIZED_DELIVERY_JOB_TYPE: processor},
                logger=self.logger,
                metrics=self.metrics,
                options=WorkerOptions(
                    poll_interval=0.005,
                    lease_duration=0.5,
                    heartbeat_interval=0.05,
                    retry_delay=0,
                    shutdown_timeout=0.2,
                ),
                close_database_on_exit=False,
            )
            for _ in range(worker_count)
        )
        tasks = tuple(
            asyncio.create_task(worker.run(install_signal_handlers=False))
            for worker in workers
        )
        try:
            for _ in range(300):
                async with self.database.connect() as connection:
                    delivery_states = tuple(
                        (
                            await connection.execute(
                                sa.select(personalized_deliveries.c.status)
                            )
                        ).scalars()
                    )
                    job_states = tuple(
                        (
                            await connection.execute(
                                sa.select(durable_jobs.c.state).where(
                                    durable_jobs.c.job_type
                                    == PERSONALIZED_DELIVERY_JOB_TYPE
                                )
                            )
                        ).scalars()
                    )
                expected_job_state = (
                    "failed"
                    if expected_delivery_status is DeliveryStatus.FAILED
                    else "completed"
                )
                if (
                    len(delivery_states) == expected_count
                    and set(delivery_states) == {expected_delivery_status.value}
                    and len(job_states) == expected_count
                    and set(job_states) == {expected_job_state}
                ):
                    break
                await asyncio.sleep(0.01)
            else:
                self.fail(
                    f"delivery worker did not reach {expected_delivery_status.value}: "
                    f"deliveries={delivery_states}, jobs={job_states}"
                )
        finally:
            for worker in workers:
                worker.request_stop()
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=1)

    async def _run_idle_worker(self, sender: "_RecordingSender") -> None:
        jobs = DurableJobRepository(self.metrics)
        worker = DurableWorker(
            self.database,
            repository=jobs,
            worker_id="delivery-idle-test",
            handlers={
                PERSONALIZED_DELIVERY_JOB_TYPE: PersonalizedDeliveryJobProcessor(
                    self.database,
                    sender,
                    entitlement_checker=TrialEntitlementChecker(clock=lambda: NOW),
                    metrics=self.metrics,
                    logger=self.logger,
                )
            },
            logger=self.logger,
            options=WorkerOptions(
                poll_interval=0.01,
                lease_duration=0.5,
                heartbeat_interval=0.05,
                retry_delay=0,
                shutdown_timeout=0.1,
            ),
            close_database_on_exit=False,
        )
        task = asyncio.create_task(worker.run(install_signal_handlers=False))
        await asyncio.sleep(0.03)
        worker.request_stop()
        await asyncio.wait_for(task, timeout=1)


class _RecordingSender:
    def __init__(self, *, failures_before_success: int = 0) -> None:
        self.failures_before_success = failures_before_success
        self.attempt_count = 0
        self.success_count = 0
        self.calls: list[dict[str, object]] = []

    async def send(self, **kwargs) -> TelegramSendReceipt:
        self.attempt_count += 1
        if self.attempt_count <= self.failures_before_success:
            raise ConnectionError("private transport failure detail")
        self.success_count += 1
        self.calls.append(dict(kwargs))
        return TelegramSendReceipt(message_id=1000 + self.success_count)


class _FakeBotClient:
    def __init__(self) -> None:
        self.calls = []

    async def send_message(self, chat_id, body, **kwargs):
        self.calls.append((chat_id, body, kwargs))
        return type("SentMessage", (), {"id": 91})()


def _term(value: str) -> dict[str, str]:
    return {
        "value": value,
        "normalized_value": value.casefold(),
        "origin": "explicit",
        "evidence": value,
    }


if __name__ == "__main__":
    unittest.main()
