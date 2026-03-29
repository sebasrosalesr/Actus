import unittest
from datetime import UTC, datetime
from pathlib import Path
import sqlite3
import sys
from unittest.mock import patch

sys.path.append(str(Path(__file__).resolve().parents[2]))
from app.quality.store import (
    prune_quality_events,
    quality_summary,
    quality_trends,
    record_quality_event,
)


class TestQualityStore(unittest.TestCase):
    def setUp(self) -> None:
        self.db_path = Path("backend/rag_data/test_quality_metrics.sqlite").resolve()
        if self.db_path.exists():
            self.db_path.unlink()

    def tearDown(self) -> None:
        if self.db_path.exists():
            self.db_path.unlink()

    def test_quality_summary_with_delta(self) -> None:
        now = datetime(2026, 3, 16, 12, 0, 0, tzinfo=UTC)

        # Current window events (last 7 days)
        record_quality_event(
            {
                "ts_utc": "2026-03-15T12:00:00+00:00",
                "query": "analyze ticket R-1",
                "intent_id": "ticket_analysis",
                "provider": "actus",
                "latency_ms": 2000,
                "ok": True,
                "result_count": 3,
                "has_ticket_analysis": True,
                "release_tag": "v2",
            },
            db_path=str(self.db_path),
        )
        record_quality_event(
            {
                "ts_utc": "2026-03-14T12:00:00+00:00",
                "query": "help",
                "intent_id": "",
                "provider": "actus",
                "latency_ms": 1000,
                "ok": True,
                "result_count": 0,
                "is_help": True,
                "release_tag": "v2",
            },
            db_path=str(self.db_path),
        )

        # Previous window events (7 to 14 days ago)
        record_quality_event(
            {
                "ts_utc": "2026-03-08T12:00:00+00:00",
                "query": "bad request",
                "intent_id": "",
                "provider": "actus",
                "latency_ms": 3000,
                "ok": False,
                "error": "boom",
                "result_count": 0,
                "release_tag": "v2",
            },
            db_path=str(self.db_path),
        )

        summary = quality_summary(
            window="7d",
            now_utc=now,
            release_tag="v2",
            db_path=str(self.db_path),
        )

        self.assertEqual("7d", summary["window"])
        self.assertEqual("v2", summary["release_tag"])
        self.assertEqual(2, summary["metrics"]["ask_requests"])
        self.assertAlmostEqual(0.5, float(summary["metrics"]["intent_hit_rate"]), places=4)
        self.assertAlmostEqual(0.0, float(summary["metrics"]["error_rate"]), places=4)
        self.assertEqual(1, summary["delta_vs_prev_window"]["ask_requests"])
        self.assertAlmostEqual(-1.0, float(summary["delta_vs_prev_window"]["error_rate"]), places=4)

    def test_quality_trends_groups_by_week(self) -> None:
        now = datetime(2026, 3, 16, 12, 0, 0, tzinfo=UTC)

        record_quality_event(
            {
                "ts_utc": "2026-03-15T12:00:00+00:00",
                "query": "ticket status",
                "intent_id": "ticket_status",
                "provider": "actus",
                "latency_ms": 1400,
                "ok": True,
                "result_count": 2,
            },
            db_path=str(self.db_path),
        )
        record_quality_event(
            {
                "ts_utc": "2026-03-05T12:00:00+00:00",
                "query": "credit trends",
                "intent_id": "credit_trends",
                "provider": "actus",
                "latency_ms": 1200,
                "ok": True,
                "result_count": 0,
            },
            db_path=str(self.db_path),
        )

        trends = quality_trends(
            window="4w",
            group_by="week",
            now_utc=now,
            db_path=str(self.db_path),
        )

        self.assertEqual("week", trends["group_by"])
        self.assertGreaterEqual(len(trends["points"]), 2)
        total_requests = sum(int(point["metrics"]["ask_requests"]) for point in trends["points"])
        self.assertEqual(2, total_requests)

    def test_prune_quality_events_removes_rows_older_than_retention(self) -> None:
        now = datetime(2026, 3, 16, 12, 0, 0, tzinfo=UTC)

        record_quality_event(
            {
                "ts_utc": "2025-10-01T12:00:00+00:00",
                "query": "old event",
                "intent_id": "credit_trends",
                "provider": "actus",
                "latency_ms": 1000,
                "ok": True,
                "result_count": 1,
            },
            db_path=str(self.db_path),
        )
        record_quality_event(
            {
                "ts_utc": "2026-03-15T12:00:00+00:00",
                "query": "recent event",
                "intent_id": "credit_trends",
                "provider": "actus",
                "latency_ms": 1000,
                "ok": True,
                "result_count": 1,
            },
            db_path=str(self.db_path),
        )

        with patch.dict("os.environ", {"ACTUS_ENV": "production", "ACTUS_QUALITY_RETENTION_DAYS": "90"}, clear=False):
            deleted = prune_quality_events(now_utc=now, db_path=str(self.db_path))

        self.assertEqual(1, deleted)

        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute("SELECT COUNT(*) FROM quality_events").fetchone()
        self.assertEqual(1, int(row[0]))


if __name__ == "__main__":
    unittest.main()
