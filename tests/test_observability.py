import io
import json
import logging
import unittest
from uuid import UUID

from freelancer_bot.config import RuntimeConfig
from freelancer_bot.observability import (
    CONTENT_REDACTED,
    REDACTED,
    Redactor,
    configure_structured_logger,
    log_event,
    trace_context,
)


CANARY_SECRET = "CANARY_SECRET_VALUE_847263"
RAW_TELEGRAM_CANARY = "RAW_TELEGRAM_USER_MESSAGE_938451"


class StructuredObservabilityTest(unittest.TestCase):
    def setUp(self):
        config = RuntimeConfig(
            api_id=847263,
            api_hash=CANARY_SECRET,
            bot_token=f"123456:{CANARY_SECRET}abcdefghijklmnop",
            database_url="postgresql" + f"+psycopg://worker:{CANARY_SECRET}@db.internal/jobs",
        )
        self.redactor = Redactor.from_config(config)
        self.output = io.StringIO()
        self.logger = configure_structured_logger(
            f"test.observability.{id(self)}",
            redactor=self.redactor,
            stream=self.output,
        )

    def test_configuration_reactivates_logger_disabled_by_other_tooling(self):
        self.logger.disabled = True

        configured = configure_structured_logger(
            self.logger.name,
            redactor=self.redactor,
            stream=self.output,
        )

        self.assertFalse(configured.disabled)

    def test_structured_log_redacts_values_nested_errors_causes_and_stack(self):
        try:
            try:
                raise ValueError(f"database rejected {CANARY_SECRET}")
            except ValueError as cause:
                raise RuntimeError(
                    "Connection failed for " + "post" + f"gres://user:{CANARY_SECRET}@host/db"
                ) from cause
        except RuntimeError as error:
            correlation_id = UUID("11111111-1111-1111-1111-111111111111")
            with trace_context(correlation_id):
                log_event(
                    self.logger,
                    logging.ERROR,
                    "job.handler_failed",
                    job_id="22222222-2222-2222-2222-222222222222",
                    job_type="fixture",
                    attempt=2,
                    worker_id="worker-a",
                    state_transition="running->queued",
                    timestamp=CANARY_SECRET,
                    message=f"retry after {CANARY_SECRET}",
                    authorization=f"Bearer {CANARY_SECRET}",
                    cookie=f"session={CANARY_SECRET}",
                    nested={
                        "access_token": CANARY_SECRET,
                        "database": "postgres" + f"ql://user:{CANARY_SECRET}@host/name",
                    },
                    error=error,
                )

        serialized = self.output.getvalue()
        payload = json.loads(serialized)
        self.assertNotIn(CANARY_SECRET, serialized)
        self.assertEqual(payload["event"], "job.handler_failed")
        self.assertEqual(payload["correlation_id"], "11111111-1111-1111-1111-111111111111")
        self.assertEqual(payload["job_type"], "fixture")
        self.assertEqual(payload["attempt"], 2)
        self.assertEqual(payload["level"], "error")
        self.assertNotEqual(payload["timestamp"], REDACTED)
        self.assertEqual(payload["nested"]["access_token"], REDACTED)
        self.assertIn(REDACTED, payload["error"]["message"])
        self.assertIn(REDACTED, payload["error"]["stack"])
        self.assertIn(REDACTED, payload["error"]["cause"]["message"])

    def test_config_metadata_redacts_sensitive_scalar_values(self):
        redacted = self.redactor.redact(
            {
                "safe": f"id={847263}",
                "api_hash_copy": CANARY_SECRET,
                "database": "postgres" + f"ql://worker:{CANARY_SECRET}@host/jobs",
            }
        )
        serialized = json.dumps(redacted)
        self.assertNotIn(CANARY_SECRET, serialized)
        self.assertNotIn("847263", serialized)

    def test_raw_telegram_and_user_content_fields_are_never_serialized(self):
        log_event(
            self.logger,
            logging.INFO,
            "message.fixture",
            source_id="source-7",
            message_id=42,
            message_length=len(RAW_TELEGRAM_CANARY),
            raw_message=RAW_TELEGRAM_CANARY,
            user_content=RAW_TELEGRAM_CANARY,
        )

        serialized = self.output.getvalue()
        payload = json.loads(serialized)
        self.assertNotIn(RAW_TELEGRAM_CANARY, serialized)
        self.assertEqual(payload["raw_message"], CONTENT_REDACTED)
        self.assertEqual(payload["user_content"], CONTENT_REDACTED)
        self.assertEqual(payload["message_id"], 42)


if __name__ == "__main__":
    unittest.main()
