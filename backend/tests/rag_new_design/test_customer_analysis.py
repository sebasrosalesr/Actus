from __future__ import annotations

from pathlib import Path
import sys
import unittest
from unittest.mock import patch

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from app.rag.new_design.analytics import analyze_customer_actus
from app.rag.new_design.service import ActusHybridRAGService


def _make_line(
    invoice_number: str,
    item_number: str,
    amount: float,
    credit_number: str | None,
    root_cause_primary_id: str,
    root_cause_ids: list[str],
) -> dict[str, object]:
    return {
        "invoice_number": invoice_number,
        "item_number": item_number,
        "credit_number": credit_number,
        "credit_request_total": amount,
        "root_cause_primary_id": root_cause_primary_id,
        "root_cause_ids": root_cause_ids,
        "investigation_notes": [],
        "investigation_chunks": [],
        "reason_for_credit_raw_list": [],
    }


def _make_ticket(
    *,
    ticket_id: str,
    customer_numbers: list[str],
    sales_reps: list[str],
    root_cause_primary_id: str,
    root_cause_ids: list[str],
    lines: list[dict[str, object]],
) -> dict[str, object]:
    return {
        "ticket_id": ticket_id,
        "customer_numbers": customer_numbers,
        "sales_reps": sales_reps,
        "invoice_numbers": [str(line["invoice_number"]) for line in lines],
        "item_numbers": [str(line["item_number"]) for line in lines],
        "credit_numbers": [str(line["credit_number"]) for line in lines if line.get("credit_number")],
        "credit_request_totals": [float(line["credit_request_total"]) for line in lines],
        "reason_for_credit_raw_list": [],
        "status_raw_list": [],
        "root_cause_ids": root_cause_ids,
        "root_cause_labels": root_cause_ids,
        "root_cause_primary_id": root_cause_primary_id,
        "root_cause_primary_label": root_cause_primary_id,
        "root_cause_triggers": [],
        "line_map": {
            f"{line['invoice_number']}|{line['item_number']}": [line]
            for line in lines
        },
        "account_prefixes": [],
    }


def _sample_tickets() -> dict[str, dict[str, object]]:
    return {
        "R-100001": _make_ticket(
            ticket_id="R-100001",
            customer_numbers=["SGP1001"],
            sales_reps=["DW"],
            root_cause_primary_id="price_discrepancy",
            root_cause_ids=["price_discrepancy", "sub_price_mismatch"],
            lines=[
                _make_line("INV100", "ITEM1", 100.0, "RTN100", "price_discrepancy", ["price_discrepancy"]),
                _make_line("INV101", "ITEM2", 25.0, None, "price_discrepancy", ["sub_price_mismatch"]),
            ],
        ),
        "R-100002": _make_ticket(
            ticket_id="R-100002",
            customer_numbers=["SGP2002"],
            sales_reps=["AB"],
            root_cause_primary_id="ppd_mismatch",
            root_cause_ids=["ppd_mismatch"],
            lines=[
                _make_line("INV200", "ITEM1", 50.0, "RTN200", "ppd_mismatch", ["ppd_mismatch"]),
            ],
        ),
        "R-100003": _make_ticket(
            ticket_id="R-100003",
            customer_numbers=["NSH3003"],
            sales_reps=["ZZ"],
            root_cause_primary_id="freight_error",
            root_cause_ids=["freight_error"],
            lines=[
                _make_line("INV300", "ITEM9", 75.0, None, "freight_error", ["freight_error"]),
            ],
        ),
    }


class CustomerAnalysisTests(unittest.TestCase):
    def test_account_prefix_analysis_aggregates_related_tickets(self) -> None:
        payload = analyze_customer_actus("SGP", _sample_tickets(), match_mode="account_prefix")

        self.assertEqual(payload["match_mode"], "account_prefix")
        self.assertEqual(payload["normalized_query"], "SGP")
        self.assertEqual(payload["ticket_count"], 2)
        self.assertEqual(payload["credit_total"], 175.0)
        self.assertEqual(payload["credited_line_count"], 2)
        self.assertEqual(payload["pending_line_count"], 1)
        self.assertEqual(payload["credited_line_exposure"], 150.0)
        self.assertEqual(payload["pending_line_exposure"], 25.0)
        self.assertEqual(payload["fully_credited_ticket_count"], 1)
        self.assertEqual(payload["partially_credited_ticket_count"], 1)
        self.assertEqual(payload["open_ticket_count"], 0)
        self.assertEqual(payload["matched_account_prefixes"], ["SGP"])
        self.assertEqual(set(payload["matched_customer_numbers"]), {"SGP1001", "SGP2002"})
        self.assertEqual(payload["root_cause_counts_primary"]["price_discrepancy"], 1)
        self.assertEqual(payload["root_cause_counts_primary"]["ppd_mismatch"], 1)

    def test_customer_number_analysis_matches_only_exact_customer(self) -> None:
        payload = analyze_customer_actus("SGP1001", _sample_tickets(), match_mode="customer_number")

        self.assertEqual(payload["match_mode"], "customer_number")
        self.assertEqual(payload["ticket_count"], 1)
        self.assertEqual(payload["tickets"], ["R-100001"])
        self.assertEqual(payload["credit_total"], 125.0)
        self.assertEqual(payload["pending_line_count"], 1)

    def test_auto_mode_prefers_account_prefix_when_prefix_matches(self) -> None:
        payload = analyze_customer_actus("SGP1001", _sample_tickets(), match_mode="auto")

        self.assertEqual(payload["match_mode"], "account_prefix")
        self.assertEqual(payload["normalized_query"], "SGP")
        self.assertEqual(payload["ticket_count"], 2)

    def test_service_customer_analysis_uses_snapshot_before_refresh(self) -> None:
        service = ActusHybridRAGService()
        with (
            patch.object(service, "_load_canonical_snapshot", return_value=_sample_tickets()),
            patch.object(service, "_ensure_catalog_ready"),
            patch.object(service, "refresh_from_firebase", side_effect=AssertionError("should not refresh")),
        ):
            payload = service.analyze_customer("SGP", match_mode="account_prefix")

        self.assertEqual(payload["ticket_count"], 2)
        self.assertEqual(payload["credit_total"], 175.0)


if __name__ == "__main__":
    unittest.main()
