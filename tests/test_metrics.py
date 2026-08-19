import unittest

from freelancer_bot.metrics import InMemoryMetrics, MetricNames


class MetricsPrimitiveTest(unittest.TestCase):
    def test_counters_gauges_and_observations_keep_stable_tags(self):
        metrics = InMemoryMetrics()
        tags = {"job_type": "fixture"}

        metrics.increment(MetricNames.JOBS_CREATED, tags=tags)
        metrics.increment(MetricNames.JOBS_CREATED, 2, tags=tags)
        metrics.gauge(MetricNames.ACTIVE_SOURCES, 13)
        metrics.observe(MetricNames.JOB_PROCESSING_SECONDS, 0.25, tags=tags)

        self.assertEqual(metrics.counter(MetricNames.JOBS_CREATED, tags=tags), 3)
        self.assertEqual(
            metrics.observations(MetricNames.JOB_PROCESSING_SECONDS, tags=tags),
            (0.25,),
        )
        snapshot = metrics.snapshot()
        self.assertIn((MetricNames.ACTIVE_SOURCES, ()), snapshot.gauges)


if __name__ == "__main__":
    unittest.main()
