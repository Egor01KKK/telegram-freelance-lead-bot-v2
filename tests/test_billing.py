from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import unittest

from freelancer_bot.billing import (
    DEFAULT_TRIAL_POLICY,
    BillingPlan,
    EntitlementState,
    PaidEntitlementPeriod,
    SubscriptionState,
    TrialPolicy,
    evaluate_subscription_entitlement,
    evaluate_trial_entitlement,
)
from uuid import uuid4


STARTED_AT = datetime(2026, 8, 14, 20, 0, tzinfo=timezone.utc)


class BillingPolicyTest(unittest.TestCase):
    def test_trial_is_active_before_three_day_boundary_and_expired_at_boundary(self):
        active = evaluate_trial_entitlement(
            STARTED_AT,
            evaluated_at=STARTED_AT + timedelta(days=3) - timedelta(microseconds=1),
        )
        expired = evaluate_trial_entitlement(
            STARTED_AT,
            evaluated_at=STARTED_AT + timedelta(days=3),
        )

        self.assertEqual(active.state, EntitlementState.TRIAL_ACTIVE)
        self.assertTrue(active.can_receive_deliveries)
        self.assertEqual(
            active.trial_expires_at,
            STARTED_AT + timedelta(days=3),
        )
        self.assertEqual(expired.state, EntitlementState.TRIAL_EXPIRED)
        self.assertFalse(expired.can_receive_deliveries)
        self.assertEqual(expired.failure_code, "TrialExpired")

    def test_draft_or_confirmation_does_not_create_entitlement(self):
        decision = evaluate_trial_entitlement(
            None,
            evaluated_at=STARTED_AT,
        )

        self.assertEqual(decision.state, EntitlementState.TRIAL_NOT_STARTED)
        self.assertFalse(decision.can_receive_deliveries)
        self.assertEqual(decision.failure_code, "TrialNotStarted")

    def test_persisted_expiry_is_not_extended_by_a_later_policy(self):
        decision = evaluate_trial_entitlement(
            STARTED_AT,
            trial_expires_at=STARTED_AT + timedelta(days=3),
            evaluated_at=STARTED_AT + timedelta(days=4),
            policy=TrialPolicy(duration=timedelta(days=30)),
        )

        self.assertEqual(decision.state, EntitlementState.TRIAL_EXPIRED)
        self.assertEqual(
            decision.trial_expires_at,
            STARTED_AT + timedelta(days=3),
        )

    def test_billing_plan_formats_configured_amount_without_domain_literal(self):
        plan = BillingPlan(
            amount=Decimal("1250.50"),
            currency="rub",
            interval="MONTH",
        )

        self.assertEqual(plan.price_label, "1250.5 RUB/month")
        self.assertEqual(DEFAULT_TRIAL_POLICY.duration, timedelta(days=3))

    def test_paid_period_supersedes_expired_trial_without_mutating_trial_dates(self):
        period = PaidEntitlementPeriod(
            id=uuid4(),
            provider="robokassa",
            period_start_at=STARTED_AT + timedelta(days=3),
            period_end_at=STARTED_AT + timedelta(days=34),
        )

        decision = evaluate_subscription_entitlement(
            STARTED_AT,
            trial_expires_at=STARTED_AT + timedelta(days=3),
            trial_policy_version="trial-entitlement.v1",
            paid_periods=(period,),
            evaluated_at=STARTED_AT + timedelta(days=5),
        )

        self.assertEqual(decision.state, EntitlementState.PAID_ACTIVE)
        self.assertEqual(decision.subscription_state, SubscriptionState.PAID_ACTIVE)
        self.assertTrue(decision.can_receive_deliveries)
        self.assertEqual(decision.trial_expires_at, STARTED_AT + timedelta(days=3))
        self.assertEqual(decision.paid_period_id, period.id)
        self.assertEqual(decision.paid_provider, "robokassa")

    def test_paid_period_is_half_open_and_future_period_does_not_grant_access(self):
        period = PaidEntitlementPeriod(
            id=uuid4(),
            provider="another-provider",
            period_start_at=STARTED_AT + timedelta(days=4),
            period_end_at=STARTED_AT + timedelta(days=35),
        )

        before_start = evaluate_subscription_entitlement(
            STARTED_AT,
            trial_expires_at=STARTED_AT + timedelta(days=3),
            paid_periods=(period,),
            evaluated_at=STARTED_AT + timedelta(days=3),
        )
        at_end = evaluate_subscription_entitlement(
            STARTED_AT,
            trial_expires_at=STARTED_AT + timedelta(days=3),
            paid_periods=(period,),
            evaluated_at=period.period_end_at,
        )

        self.assertEqual(before_start.state, EntitlementState.TRIAL_EXPIRED)
        self.assertFalse(before_start.can_receive_deliveries)
        self.assertEqual(at_end.state, EntitlementState.TRIAL_EXPIRED)
        self.assertFalse(at_end.can_receive_deliveries)

    def test_paused_and_cancelled_local_states_override_active_paid_period(self):
        period = PaidEntitlementPeriod(
            id=uuid4(),
            provider="robokassa",
            period_start_at=STARTED_AT,
            period_end_at=STARTED_AT + timedelta(days=31),
        )

        for lifecycle_state, expected_failure in (
            (SubscriptionState.PAUSED, "SubscriptionPaused"),
            (SubscriptionState.CANCELLED, "SubscriptionCancelled"),
        ):
            with self.subTest(lifecycle_state=lifecycle_state):
                decision = evaluate_subscription_entitlement(
                    STARTED_AT,
                    trial_expires_at=STARTED_AT + timedelta(days=3),
                    paid_periods=(period,),
                    lifecycle_state=lifecycle_state,
                    evaluated_at=STARTED_AT + timedelta(days=1),
                )
                self.assertEqual(decision.state.value, lifecycle_state.value)
                self.assertFalse(decision.can_receive_deliveries)
                self.assertEqual(decision.failure_code, expected_failure)


if __name__ == "__main__":
    unittest.main()
