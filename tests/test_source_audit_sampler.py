from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
import unittest

from freelancer_bot.source_audit_sampler import (
    AuditFetchPurpose,
    SourceAuditHistoryReader,
    SourceAuditMessage,
    SourceAuditPolicy,
    SourceAuditSampler,
    SourceAuditTarget,
    TelethonSourceAuditHistoryReader,
)


NOW = datetime(2026, 8, 9, 18, 0, tzinfo=timezone.utc)
TARGET = SourceAuditTarget(
    source_id=101,
    platform="telegram",
    lookup="@audit_fixture",
)


class RecordingHistoryReader:
    def __init__(self, messages):
        self.messages = tuple(messages)
        self.calls = []

    async def fetch_window(
        self,
        target,
        *,
        window_started_at,
        window_ended_at,
        limit,
    ):
        self.calls.append(
            (target.source_id, window_started_at, window_ended_at, limit)
        )
        matching = [
            message
            for message in self.messages
            if window_started_at <= message.occurred_at <= window_ended_at
        ]
        return tuple(
            sorted(
                matching,
                key=lambda message: (message.occurred_at, message.message_id),
                reverse=True,
            )[:limit]
        )


class FakeTelethonAuditClient:
    def __init__(self, messages):
        self.entity = SimpleNamespace(id=777)
        self.messages = tuple(messages)
        self.entity_calls = []
        self.iter_calls = []

    async def get_entity(self, lookup):
        self.entity_calls.append(lookup)
        return self.entity

    def iter_messages(self, entity, *, offset_date, limit):
        self.iter_calls.append((entity, offset_date, limit))

        async def iterate():
            for message in self.messages[:limit]:
                yield message

        return iterate()


class SourceAuditSamplerTest(unittest.IsolatedAsyncioTestCase):
    async def test_default_recent_window_records_actual_sampled_range(self):
        recent = [
            _message(index, NOW - timedelta(hours=index))
            for index in range(1, 41)
        ]
        old = [_message(1000, NOW - timedelta(days=20))]
        reader = RecordingHistoryReader(recent + old)
        sampler = SourceAuditSampler(reader)

        sample = await sampler.sample(TARGET, audited_at=NOW)

        self.assertIsInstance(reader, SourceAuditHistoryReader)
        self.assertFalse(sample.expanded)
        self.assertFalse(sample.high_volume)
        self.assertEqual(sample.window_started_at, NOW - timedelta(days=3))
        self.assertEqual(sample.window_ended_at, NOW)
        self.assertEqual(sample.sampled_message_count, 40)
        self.assertEqual(sample.sampled_from, NOW - timedelta(hours=40))
        self.assertEqual(sample.sampled_to, NOW - timedelta(hours=1))
        self.assertEqual(
            [fetch.purpose for fetch in sample.fetches],
            [AuditFetchPurpose.INITIAL_PROBE],
        )
        self.assertEqual(sample.fetches[0].limit, 151)
        self.assertNotIn(1000, {message.message_id for message in sample.messages})

    async def test_configured_full_audit_capacity_reaches_rejection_floor(self):
        recent = [
            _message(index, NOW - timedelta(hours=index))
            for index in range(1, 81)
        ]
        reader = RecordingHistoryReader(recent)
        sampler = SourceAuditSampler(
            reader,
            policy=SourceAuditPolicy(
                sample_size=60,
                minimum_evidence_messages=30,
                distribution_buckets=2,
            ),
        )

        sample = await sampler.sample(TARGET, audited_at=NOW)

        self.assertEqual(sample.probe_message_count, 61)
        self.assertEqual(sample.sampled_message_count, 60)
        self.assertEqual(sample.fetches[0].limit, 61)
        self.assertTrue(all(fetch.limit <= 61 for fetch in sample.fetches))

    async def test_high_volume_uses_bounded_distributed_sample(self):
        window_start = NOW - timedelta(days=3)
        messages = [
            _message(
                index,
                window_start + timedelta(seconds=index * 250),
            )
            for index in range(1, 1001)
        ]
        reader = RecordingHistoryReader(messages)
        sampler = SourceAuditSampler(reader)

        sample = await sampler.sample(TARGET, audited_at=NOW)

        self.assertTrue(sample.high_volume)
        self.assertFalse(sample.expanded)
        self.assertEqual(sample.probe_message_count, 151)
        self.assertEqual(sample.sampled_message_count, 150)
        self.assertEqual(len(sample.fetches), 11)
        self.assertEqual(sample.fetches[0].purpose, AuditFetchPurpose.INITIAL_PROBE)
        distributed = sample.fetches[1:]
        self.assertTrue(
            all(fetch.purpose is AuditFetchPurpose.DISTRIBUTED_BUCKET for fetch in distributed)
        )
        self.assertEqual([fetch.limit for fetch in distributed], [15] * 10)
        self.assertTrue(all(fetch.limit <= 151 for fetch in sample.fetches))
        self.assertLess(sample.sampled_from, NOW - timedelta(days=2, hours=12))
        self.assertGreater(sample.sampled_to, NOW - timedelta(hours=8))
        represented_buckets = {
            min(
                9,
                int(
                    (message.occurred_at - window_start)
                    / timedelta(days=3)
                    * 10
                ),
            )
            for message in sample.messages
        }
        self.assertEqual(represented_buckets, set(range(10)))

    async def test_quiet_source_expands_to_bounded_fourteen_days_only(self):
        messages = [
            _message(1, NOW - timedelta(days=1)),
            _message(2, NOW - timedelta(days=5)),
            _message(3, NOW - timedelta(days=10)),
            _message(4, NOW - timedelta(days=13)),
            _message(5, NOW - timedelta(days=15)),
            _message(6, NOW - timedelta(days=60)),
        ]
        reader = RecordingHistoryReader(messages)
        sampler = SourceAuditSampler(reader)

        sample = await sampler.sample(TARGET, audited_at=NOW)

        self.assertTrue(sample.expanded)
        self.assertFalse(sample.high_volume)
        self.assertEqual(sample.initial_window_started_at, NOW - timedelta(days=3))
        self.assertEqual(sample.window_started_at, NOW - timedelta(days=14))
        self.assertEqual(sample.window_ended_at, NOW)
        self.assertEqual(sample.sampled_message_count, 4)
        self.assertEqual(sample.sampled_from, NOW - timedelta(days=13))
        self.assertEqual(sample.sampled_to, NOW - timedelta(days=1))
        self.assertEqual(
            [fetch.purpose for fetch in sample.fetches],
            [AuditFetchPurpose.INITIAL_PROBE, AuditFetchPurpose.EXPANDED_PROBE],
        )
        self.assertEqual([fetch.limit for fetch in sample.fetches], [151, 151])
        self.assertEqual(len(reader.calls), 2)
        self.assertTrue(
            all(call[1] >= NOW - timedelta(days=14) for call in reader.calls)
        )
        self.assertEqual(
            {message.message_id for message in sample.messages},
            {1, 2, 3, 4},
        )

    async def test_telethon_reader_applies_time_and_count_bounds(self):
        messages = (
            SimpleNamespace(
                id=1,
                date=NOW - timedelta(hours=1),
                message="inside",
                media=None,
            ),
            SimpleNamespace(
                id=2,
                date=NOW - timedelta(days=5),
                message="outside",
                media=object(),
            ),
        )
        client = FakeTelethonAuditClient(messages)
        reader = TelethonSourceAuditHistoryReader(client)

        first = await reader.fetch_window(
            TARGET,
            window_started_at=NOW - timedelta(days=3),
            window_ended_at=NOW,
            limit=20,
        )
        second = await reader.fetch_window(
            TARGET,
            window_started_at=NOW - timedelta(days=3),
            window_ended_at=NOW,
            limit=10,
        )

        self.assertEqual([message.message_id for message in first], [1])
        self.assertEqual([message.message_id for message in second], [1])
        self.assertEqual(client.entity_calls, ["@audit_fixture"])
        self.assertEqual(
            [(call[1], call[2]) for call in client.iter_calls],
            [(NOW, 20), (NOW, 10)],
        )

    async def test_bounded_reader_caps_each_history_request(self):
        client = FakeTelethonAuditClient(
            (
                SimpleNamespace(
                    id=1,
                    date=NOW - timedelta(hours=1),
                    message="inside",
                    media=None,
                ),
            )
        )
        reader = TelethonSourceAuditHistoryReader(
            client,
            max_messages_per_pass=25,
        )

        await reader.fetch_window(
            TARGET,
            window_started_at=NOW - timedelta(days=3),
            window_ended_at=NOW,
            limit=151,
        )

        self.assertEqual(client.iter_calls, [(client.entity, NOW, 25)])


class SourceAuditPolicyTest(unittest.TestCase):
    def test_policy_enforces_required_window_and_sample_bounds(self):
        for arguments in (
            {"initial_window_days": 1},
            {"initial_window_days": 5},
            {"expanded_window_days": 6},
            {"expanded_window_days": 15},
            {"sample_size": 19},
            {"sample_size": 201},
        ):
            with self.subTest(arguments=arguments):
                with self.assertRaises(ValueError):
                    SourceAuditPolicy(**arguments)


def _message(message_id, occurred_at):
    return SourceAuditMessage(
        message_id=message_id,
        occurred_at=occurred_at,
        text=f"message {message_id}",
    )


if __name__ == "__main__":
    unittest.main()
