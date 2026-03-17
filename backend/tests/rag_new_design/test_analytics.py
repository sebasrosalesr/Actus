import unittest
from datetime import datetime

from app.rag.new_design.analytics import (
    analyze_item_actus,
    analyze_ticket_actus,
    compute_ticket_timeline_metrics_from_status_list,
)
from app.rag.new_design.index_build import build_pipeline_artifacts
from app.rag.new_design.root_cause import load_root_cause_rules


class TestAnalytics(unittest.TestCase):
    def test_analyze_item_actus_returns_expected_counts(self) -> None:
        credit_rows = [
            {
                "Ticket Number": "R-700001",
                "Invoice Number": "INV700A",
                "Item Number": "1007986",
                "Reason for Credit": "sub not price matched",
                "Credit Request Total": "12.50",
                "Customer Number": "ABC10",
                "Sales Rep": "REP1",
            },
            {
                "Ticket Number": "R-700002",
                "Invoice Number": "INV700B",
                "Item Number": "1007986",
                "Reason for Credit": "wrong price loaded",
                "Credit Request Total": "20.00",
                "Customer Number": "XYZ22",
                "Sales Rep": "REP2",
            },
            {
                "Ticket Number": "R-700003",
                "Invoice Number": "INV700C",
                "Item Number": "9999999",
                "Reason for Credit": "wrong price loaded",
                "Credit Request Total": "30.00",
                "Customer Number": "XYZ22",
                "Sales Rep": "REP2",
            },
        ]
        investigation_rows = [
            {
                "ticket_number": "R-700001",
                "invoice_number": "INV700A",
                "item_number": "1007986",
                "note_id": "N700A",
                "title": "Price Trace",
                "body": "sub not price matched",
                "created_at": "2026-02-01 10:00:00",
                "updated_at": "2026-02-03 11:00:00",
            },
            {
                "ticket_number": "R-700002",
                "invoice_number": "INV700B",
                "item_number": "1007986",
                "note_id": "N700B",
                "title": "Price Trace",
                "body": "wrong price loaded",
                "created_at": "2026-02-05 10:00:00",
                "updated_at": "2026-02-06 11:00:00",
            },
        ]

        artifacts = build_pipeline_artifacts(
            credit_rows=credit_rows,
            investigation_rows=investigation_rows,
            rules=load_root_cause_rules(),
        )

        out = analyze_item_actus("1007986", artifacts.canonical_tickets)
        self.assertEqual(2, out["ticket_count"])
        self.assertEqual(2, out["invoice_count"])
        self.assertEqual(2, out["line_count"])
        self.assertAlmostEqual(32.5, float(out["total_credit"]), places=2)
        self.assertIn("root_cause_counts_all", out)
        self.assertGreaterEqual(len(out["root_cause_counts_all"]), 1)
        self.assertIn("REP1", out["sales_rep_counts"])
        self.assertIn("REP2", out["sales_rep_counts"])
        self.assertIn("answer", out)
        self.assertIn("Item 1007986 analysis", out["answer"])

    def test_analyze_item_actus_no_results(self) -> None:
        out = analyze_item_actus("1007986", {})
        self.assertEqual(0, out["ticket_count"])
        self.assertEqual({}, out["root_cause_counts_all"])
        self.assertIn("No credit activity found", out["answer"])

    def test_analyze_ticket_actus_returns_timeline_and_summary(self) -> None:
        credit_rows = [
            {
                "Ticket Number": "R-800001",
                "Invoice Number": "INV800A",
                "Item Number": "1008001",
                "Reason for Credit": "sub not price matched",
                "Credit Request Total": "15.00",
                "Customer Number": "ABC10",
                "Sales Rep": "REP800",
                "Status": (
                    "2026-02-01 10:00:00\nOpen: Not Started\n"
                    "2026-02-02 11:00:00\nWent through investigation\n"
                    "2026-02-03 12:00:00\nSubmitted to Billing\n"
                    "2026-02-06 09:00:00\nUpdated by the system"
                ),
            }
        ]
        investigation_rows = [
            {
                "ticket_number": "R-800001",
                "invoice_number": "INV800A",
                "item_number": "1008001",
                "note_id": "N800A",
                "title": "Investigation",
                "body": "Pricing mismatch confirmed and corrected.",
                "created_at": "2026-02-02 10:00:00",
            }
        ]

        artifacts = build_pipeline_artifacts(
            credit_rows=credit_rows,
            investigation_rows=investigation_rows,
            rules=load_root_cause_rules(),
        )

        out = analyze_ticket_actus("R-800001", artifacts.canonical_tickets, threshold_days=30)
        self.assertEqual("R-800001", out["ticket_id"])
        self.assertEqual(15.0, out["credit_total"])
        self.assertEqual(1, out["line_count"])
        self.assertTrue(out["is_credited"])
        self.assertIsNotNone(out["entered_to_credited_days"])
        self.assertIsNotNone(out["investigation_to_credited_days"])
        self.assertIn("timeline_metrics", out)
        self.assertIn("answer", out)
        self.assertIn("Ticket R-800001", out["answer"])

    def test_analyze_ticket_actus_not_found(self) -> None:
        out = analyze_ticket_actus("R-999999", {})
        self.assertEqual("R-999999", out["ticket_id"])
        self.assertIn("was not found", out["answer"])

    def test_timeline_metrics_fallback_sets_days_open_without_entered_marker(self) -> None:
        status_list = [
            (
                "2026-02-27 11:11:21\nWIP\n"
                "2026-03-01 10:00:00\nstill reviewing"
            )
        ]
        now_dt = datetime(2026, 3, 16, 12, 0, 0)
        out = compute_ticket_timeline_metrics_from_status_list(status_list, now_dt=now_dt)
        self.assertFalse(out["is_credited"])
        self.assertIsNotNone(out["days_open"])
        self.assertGreater(out["days_open"], 0)
        self.assertEqual("2026-03-01 10:00:00", out["last_status_timestamp"])

    def test_timeline_marks_credited_from_closure_status_language(self) -> None:
        status_list = [
            (
                "2026-02-01 10:00:00\nUpdate: Credit number sent. Ticket is resolved and will be closed officially in 14 days.\n"
                "2026-02-18 10:00:00\n[SYSTEM] Update: CR number verified and credit processing completed. Ticket automatically closed by the system."
            )
        ]
        out = compute_ticket_timeline_metrics_from_status_list(status_list, now_dt=datetime(2026, 3, 16, 12, 0, 0))
        self.assertTrue(out["is_credited"])
        self.assertEqual("2026-02-01 10:00:00", out["credited_timestamp"])
        self.assertIsNone(out["days_open"])

    def test_timeline_marks_credited_from_rtn_fallback(self) -> None:
        status_list = ["2026-02-27 11:11:21\nWIP\n2026-03-01 10:00:00\nstill reviewing"]
        out = compute_ticket_timeline_metrics_from_status_list(
            status_list,
            now_dt=datetime(2026, 3, 16, 12, 0, 0),
            has_credit_number=True,
        )
        self.assertTrue(out["is_credited"])
        self.assertTrue(out["credited_inferred_from_rtn"])
        self.assertEqual("2026-03-01 10:00:00", out["credited_timestamp"])
        self.assertIsNone(out["days_open"])


if __name__ == "__main__":
    unittest.main()
