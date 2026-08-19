from __future__ import annotations

import unittest
from types import SimpleNamespace

from telethon.tl.types import InputPeerChannel

from freelancer_bot.source_discovery_runtime import _source_audit_lookup


class SourceDiscoveryAuditLookupTest(unittest.TestCase):
    def test_private_chat_discovery_source_uses_persisted_peer_access_hash(self):
        source = SimpleNamespace(
            platform="telegram",
            access_type="private",
            handle=None,
            canonical_url=None,
            external_id="peer:private",
        )
        peer = SimpleNamespace(
            peer_type="supergroup",
            telegram_peer_id=123,
            telegram_access_hash=456,
            username=None,
            canonical_url=None,
        )

        lookup = _source_audit_lookup(source, peer)

        self.assertIsInstance(lookup, InputPeerChannel)
        self.assertEqual(lookup.channel_id, 123)
        self.assertEqual(lookup.access_hash, 456)

    def test_public_source_keeps_provider_visible_lookup(self):
        source = SimpleNamespace(
            platform="telegram",
            access_type="public",
            handle="@public_source",
            canonical_url="https://t.me/public_source",
            external_id="username:public_source",
        )

        self.assertEqual(_source_audit_lookup(source, None), "@public_source")
