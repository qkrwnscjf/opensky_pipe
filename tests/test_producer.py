"""producer 순수 로직 단위 테스트 (R-3, 2026-10-13).

OpenSky·Kafka에 접속하지 않는다: KafkaProducer는 import 전에 가짜로 바꾸고, HTTP 응답도 가짜를 쓴다.
실행(producer 이미지, Python 3.11):
  docker compose run --rm --no-deps -T -v "$PWD/src:/app/src:ro" -v "$PWD/tests:/app/tests:ro" \
      producer python -m unittest discover -s tests -p 'test_producer.py' -v
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))

import kafka  # noqa: E402
import requests  # noqa: E402
from kafka.errors import KafkaTimeoutError  # noqa: E402


class FakeKafkaProducer:
    def __init__(self, *args, **kwargs):
        self.sent = []
        self.flush_error = None

    def send(self, topic, key=None, value=None):
        self.sent.append((topic, key, value))

    def flush(self):
        if self.flush_error is not None:
            raise self.flush_error


kafka.KafkaProducer = FakeKafkaProducer  # producer.py가 import 시점에 만드는 KafkaProducer를 가짜로
import producer as p  # noqa: E402


def state(icao24="abc123", time_position=1_700_000_000):
    # OpenSky state 벡터 17칸(인덱스 0~16)
    return [icao24, "KAL123  ", "Republic of Korea", time_position, time_position, 127.0, 37.0,
            10000.0, False, 250.0, 90.0, 0.0, None, 10500.0, "1234", False, 0]


class FakeResponse:
    def __init__(self, status=200, payload=None, bad_json=False):
        self.status_code = status
        self._payload = payload
        self._bad_json = bad_json

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} error", response=self)

    def json(self):
        if self._bad_json:
            raise ValueError("Expecting value")
        return self._payload


class SnapshotDecisionTest(unittest.TestCase):
    def test_first_snapshot_is_sent(self):
        self.assertEqual(p.snapshot_decision(100, None), "send")

    def test_same_snapshot_is_repeat(self):
        self.assertEqual(p.snapshot_decision(100, 100), "repeat")

    def test_older_snapshot_is_skipped(self):
        self.assertEqual(p.snapshot_decision(99, 100), "older")

    def test_newer_snapshot_is_sent(self):
        self.assertEqual(p.snapshot_decision(101, 100), "send")

    def test_flip_flop_does_not_resend(self):
        # T → X(더 오래된 것) → T: 세 번째 T를 다시 보내면 같은 스냅샷이 Kafka에 두 번 실린다(2026-10-10 관측).
        watermark, decisions = None, []
        for snap in (100, 99, 100):
            d = p.snapshot_decision(snap, watermark)
            decisions.append(d)
            if d == "send":
                watermark = snap
        self.assertEqual(decisions, ["send", "older", "repeat"])


class NextPollDelayTest(unittest.TestCase):
    def fresh(self, last=None, offset=3.0):
        return {"offset": offset, "last_sent_time": last, "retried": False}

    def test_sent_targets_next_snapshot_and_pulls_offset_earlier(self):
        st = self.fresh()
        delay = p.next_poll_delay("sent", 1000.0, st, now=1001.0)
        self.assertAlmostEqual(st["offset"], 2.9)
        self.assertAlmostEqual(delay, 1000.0 + 10.0 + 2.9 - 1001.0)

    def test_repeat_retries_once_then_waits_for_next_slot(self):
        st = self.fresh(last=1000.0)
        first = p.next_poll_delay("repeat", 1000.0, st, now=1013.0)
        self.assertAlmostEqual(first, p.ALIGN_RETRY_DELAY_S)
        self.assertAlmostEqual(st["offset"], 4.0)
        second = p.next_poll_delay("repeat", 1000.0, st, now=1014.5)
        self.assertGreater(second, p.ALIGN_RETRY_DELAY_S)  # 연속 재요청 없음
        self.assertFalse(st["retried"])

    def test_error_without_history_waits_one_period(self):
        self.assertAlmostEqual(p.next_poll_delay("error", None, self.fresh(), now=5.0), 10.0)

    def test_delay_is_bounded(self):
        for outcome in ("sent", "repeat", "older", "error", "empty"):
            st = self.fresh(last=0.0)
            d = p.next_poll_delay(outcome, 0.0, st, now=1_000.0)
            self.assertGreaterEqual(d, p.MIN_REQUEST_GAP_S)
            self.assertLessEqual(d, 15.0)


class FetchAndSendTest(unittest.TestCase):
    def setUp(self):
        p.producer = FakeKafkaProducer()
        p._sent_watermark = None
        p._last_error = None
        self._get = p.session.get

    def tearDown(self):
        p.session.get = self._get

    def respond(self, response=None, raises=None):
        def get(*args, **kwargs):
            if raises is not None:
                raise raises
            return response
        p.session.get = get

    def test_sent_records_are_keyed_by_icao24_and_move_watermark(self):
        self.respond(FakeResponse(payload={"time": 200, "states": [state("aaa111"), state("bbb222")]}))
        self.assertEqual(p.fetch_and_send(), ("sent", 200))
        self.assertEqual([k for _, k, _ in p.producer.sent], ["aaa111", "bbb222"])
        self.assertEqual(p._sent_watermark, 200)
        record = p.producer.sent[0][2]
        self.assertEqual(record["timestamp"], 200)
        self.assertIsNotNone(record["ingested_at"])

    def test_failed_flush_keeps_watermark_so_the_snapshot_is_retried(self):
        self.respond(FakeResponse(payload={"time": 300, "states": [state()]}))
        p.producer.flush_error = KafkaTimeoutError("flush timed out")
        self.assertEqual(p.fetch_and_send(), ("error", None))
        self.assertIsNone(p._sent_watermark)
        self.assertEqual(p._last_error["kind"], "kafka")

    def test_repeat_snapshot_sends_nothing(self):
        p._sent_watermark = 400
        self.respond(FakeResponse(payload={"time": 400, "states": [state()]}))
        self.assertEqual(p.fetch_and_send(), ("repeat", 400))
        self.assertEqual(p.producer.sent, [])

    # R-10: 실패 원인 구분
    def test_http_429_is_classified_with_status(self):
        self.respond(FakeResponse(status=429))
        self.assertEqual(p.fetch_and_send(), ("error", None))
        self.assertEqual(p._last_error, {"kind": "http", "status": 429})

    def test_connection_failure_is_network(self):
        self.respond(raises=requests.ConnectionError("refused"))
        p.fetch_and_send()
        self.assertEqual(p._last_error["kind"], "network")

    def test_timeout_is_network(self):
        self.respond(raises=requests.Timeout("read timeout"))
        p.fetch_and_send()
        self.assertEqual(p._last_error["kind"], "network")

    def test_bad_json_is_parse(self):
        self.respond(FakeResponse(bad_json=True))
        p.fetch_and_send()
        self.assertEqual(p._last_error["kind"], "parse")


if __name__ == "__main__":
    unittest.main()
