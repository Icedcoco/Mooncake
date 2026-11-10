"""防止类型颠倒、收尾混入供压窗口、累计删除重复求和及序号缺口。"""

import unittest
from statistics_report import (
    audit_minutes,
    parse_metrics,
    histogram_delta,
    get_reconciliation,
)


class StatisticsTests(unittest.TestCase):
    def test_get_ratio_counts_attempts_and_reconciles_master(self):
        result = get_reconciliation(
            dict(
                attempted=128,
                success=120,
                get_attempted=256,
                get_success=240,
                missing=16,
                get_errors=0,
                get_batches=2,
            ),
            {
                "master_batch_get_replica_list_items_total": 256,
                "master_batch_get_replica_list_failed_items_total": 16,
                "master_batch_get_replica_list_requests_total": 2,
            },
        )
        self.assertEqual(result["get_to_put_attempt_ratio"], 2)
        self.assertEqual(result["get_outcome_difference"], 0)
        self.assertEqual(result["master_client_get_difference"], 0)
        self.assertEqual(result["master_get_success"], 240)

    def test_missing_master_get_metrics_are_not_reported_as_zero(self):
        result = get_reconciliation(dict(attempted=1, get_attempted=2), {})
        self.assertIsNone(result["master_get_items"])
        self.assertIsNone(result["master_client_get_difference"])

    def test_types_minute_boundaries_and_tail_are_separate(self):
        rows = [
            dict(
                batch=1,
                first=1,
                last=4,
                entries=4,
                counts={"1": 3, "5": 1},
                time=100,
                bytes=40,
                deleted=0,
            ),
            dict(
                batch=2,
                first=5,
                last=9,
                entries=5,
                counts={"3": 5},
                time=159.9,
                bytes=50,
                deleted=1,
            ),
            dict(
                batch=3,
                first=10,
                last=11,
                entries=2,
                counts={"1": 2},
                time=160,
                bytes=20,
                deleted=1,
            ),
            dict(
                batch=4,
                first=12,
                last=14,
                entries=3,
                counts={"3": 3},
                time=220,
                bytes=30,
                deleted=2,
            ),
        ]
        m, total = audit_minutes(rows, 100, 120)
        self.assertEqual(len(m), 2)
        self.assertEqual((m[0]["durable_put_end"], m[0]["durable_remove"]), (3, 5))
        self.assertEqual(m[1]["durable_put_end"], 2)
        self.assertEqual(m[1]["durable_remove"], 0)
        self.assertAlmostEqual(m[0]["durable_batches_per_s"], 2 / 60)
        self.assertEqual(total["tail_entries"], 3)
        self.assertEqual(total["pruned_batches_observed"], 2)
        self.assertEqual(total["last_seq"], 14)

    def test_audit_gaps_are_never_silently_accepted(self):
        with self.assertRaises(ValueError):
            audit_minutes(
                [
                    dict(
                        batch=2,
                        first=5,
                        last=9,
                        entries=5,
                        counts={"3": 5},
                        time=101,
                        bytes=50,
                        deleted=0,
                    )
                ],
                100,
                60,
            )

    def test_histogram_uses_counter_deltas_and_reports_overflow(self):
        a = parse_metrics(
            'x_count 10\nx_sum 100\nx_bucket{le="100"} 10\nx_bucket{le="+Inf"} 10\n'
        )
        b = parse_metrics(
            'x_count 12\nx_sum 700\nx_bucket{le="100"} 11\nx_bucket{le="+Inf"} 12\n'
        )
        d = histogram_delta(a, b, "x")
        self.assertEqual(d["mean"], 300)
        self.assertEqual(d["p95_upper"], "inf")
        self.assertEqual(d["count"], 2)

    def test_counter_reset_is_not_reported_as_zero_latency(self):
        self.assertIsNone(
            histogram_delta(
                {"x_count": 10, "x_sum": 50}, {"x_count": 1, "x_sum": 3}, "x"
            )["mean"]
        )


if __name__ == "__main__":
    unittest.main()
