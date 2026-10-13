"""레이크 운영 로직 단위 테스트 (R-3, 2026-10-13) — MinIO·Spark에 접속하지 않는 순수 함수만.

실행(airflow 이미지, Python 3.8 — DAG와 같은 환경):
  docker compose run --rm --no-deps -T -v "$PWD/dags:/opt/airflow/dags:ro" -v "$PWD/tests:/opt/airflow/tests:ro" \
      spark python -m unittest discover -s /opt/airflow/tests -p 'test_lake_logic.py' -v
"""
import os
import sys
import unittest

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
sys.path.insert(0, os.path.join(ROOT, "src"))
sys.path.insert(0, os.path.join(ROOT, "dags"))

import lake_reprocess as r  # noqa: E402
from flight_lakehouse_etl import _gold_eligible  # noqa: E402

# 2026-09-30에 실제로 쓴 제외 마커와 같은 모양(첫 줄 형식이 핵심 — 1-13 첫 구현은 이걸 못 알아봤다)
EXCLUDED_MARKER = (
    "status=EXCLUDED (not processed)\n"
    "reason=collected before the 0-6b fix\n"
    "undo=delete this marker\n"
)


class IsExcludedTest(unittest.TestCase):
    def test_real_marker_format_is_protected(self):
        self.assertTrue(r.is_excluded(EXCLUDED_MARKER))

    def test_marker_starting_with_excluded_is_protected(self):
        self.assertTrue(r.is_excluded("EXCLUDED: manual"))

    def test_normal_marker_is_not_protected(self):
        self.assertFalse(r.is_excluded("rows=25314\ncode_version=0123456789ab\n"))
        self.assertFalse(r.is_excluded(""))

    def test_word_in_the_middle_does_not_count(self):
        self.assertFalse(r.is_excluded("note=was EXCLUDED once, now processed"))


class PlanTest(unittest.TestCase):
    def test_without_cascade_only_the_named_stage(self):
        self.assertEqual(r.plan(["silver"], ["2026-10-10"], cascade=False), [("silver", "2026-10-10")])

    def test_cascade_reaches_gold_same_day_and_next_day(self):
        self.assertEqual(
            r.plan(["silver"], ["2026-10-10"], cascade=True),
            [("silver", "2026-10-10"), ("gold", "2026-10-10"), ("gold", "2026-10-11")],
        )

    def test_cascade_crosses_month_end(self):
        self.assertIn(("gold", "2026-10-01"), r.plan(["silver"], ["2026-09-30"], cascade=True))

    def test_duplicates_are_removed_in_order(self):
        out = r.plan(["silver", "gold"], ["2026-10-10", "2026-10-11"], cascade=True)
        self.assertEqual(len(out), len(set(out)))
        self.assertEqual(out[0], ("silver", "2026-10-10"))


class GoldEligibleTest(unittest.TestCase):
    TODAY = "2026-10-13"

    def test_failed_silver_holds_only_that_day_and_the_next(self):
        bronze = {"2026-10-01", "2026-10-02", "2026-10-03"}
        silver = {"2026-10-01", "2026-10-03"}  # 02 Silver 실패
        ready, held = _gold_eligible(bronze, silver, set(), self.TODAY)
        self.assertEqual(ready, ["2026-10-01"])
        self.assertEqual(dict(held), {
            "2026-10-02": "silver_missing",
            "2026-10-03": "context_silver_missing:2026-10-02",
        })

    def test_day_absent_from_bronze_is_not_waited_for(self):
        bronze = {"2026-10-01", "2026-10-03"}  # 02는 스택이 꺼져 있던 날
        ready, held = _gold_eligible(bronze, bronze, set(), self.TODAY)
        self.assertEqual(ready, ["2026-10-01", "2026-10-03"])
        self.assertEqual(held, [])

    def test_done_days_and_today_are_excluded(self):
        bronze = {"2026-10-11", "2026-10-12", self.TODAY}
        ready, _ = _gold_eligible(bronze, bronze, {"2026-10-11"}, self.TODAY)
        self.assertEqual(ready, ["2026-10-12"])

    def test_order_of_input_does_not_matter(self):
        bronze = {"2026-10-05", "2026-10-03", "2026-10-04"}
        ready, _ = _gold_eligible(bronze, bronze, set(), self.TODAY)
        self.assertEqual(ready, sorted(bronze))


if __name__ == "__main__":
    unittest.main()
