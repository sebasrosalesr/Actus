from __future__ import annotations

from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from app.rag.new_design.service import ActusHybridRAGService
from app.rag.new_design.snapshot import load_canonical_tickets, save_canonical_tickets


def _sample_ticket() -> dict[str, object]:
    return {
        "ticket_id": "R-000001",
        "customer_numbers": ["ABC01"],
        "sales_reps": ["DW"],
        "invoice_numbers": ["INV1"],
        "item_numbers": ["ITEM1"],
        "credit_numbers": ["RTN1"],
        "credit_request_totals": [25.5],
        "status_raw_list": ["[2026-01-20 15:26:18] Submitted to Billing: January 16th, 2026."],
        "root_cause_ids": ["sub_price_mismatch"],
        "root_cause_primary_id": "sub_price_mismatch",
        "line_map": {
            "INV1|ITEM1": [
                {
                    "invoice_number": "INV1",
                    "item_number": "ITEM1",
                    "credit_number": "RTN1",
                    "credit_request_total": 25.5,
                    "root_cause_ids": ["sub_price_mismatch"],
                    "root_cause_primary_id": "sub_price_mismatch",
                    "investigation_notes": [],
                    "investigation_chunks": [],
                    "reason_for_credit_raw_list": ["Should have been matched when subbed."],
                }
            ]
        },
        "account_prefixes": ["ABC"],
    }


class SnapshotTests(unittest.TestCase):
    def test_snapshot_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "canonical.json.gz"
            save_canonical_tickets({"R-000001": _sample_ticket()}, path)
            loaded = load_canonical_tickets(path)

        self.assertIn("R-000001", loaded)
        self.assertEqual(loaded["R-000001"]["ticket_id"], "R-000001")
        self.assertEqual(loaded["R-000001"]["invoice_numbers"], ["INV1"])

    def test_service_analyze_ticket_uses_snapshot_before_refresh(self) -> None:
        service = ActusHybridRAGService()
        with (
            patch.object(service, "_load_canonical_snapshot", return_value={"R-000001": _sample_ticket()}),
            patch.object(service, "_ensure_catalog_ready"),
            patch.object(service, "refresh_from_firebase", side_effect=AssertionError("should not refresh")),
        ):
            payload = service.analyze_ticket("R-000001", threshold_days=30)

        self.assertEqual(payload["ticket_id"], "R-000001")
        self.assertEqual(payload["primary_root_cause"], "sub_price_mismatch")
        self.assertEqual(payload["credit_total"], 25.5)


if __name__ == "__main__":
    unittest.main()
