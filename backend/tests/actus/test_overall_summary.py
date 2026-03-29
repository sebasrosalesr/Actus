import unittest
from unittest.mock import patch

import pandas as pd

from actus.intents.overall_summary import intent_overall_summary


class TestOverallSummaryIntent(unittest.TestCase):
    def test_monthly_overview_returns_operational_metrics(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "Date": "2026-03-05",
                    "Ticket Number": "R-1",
                    "Invoice Number": "INV1",
                    "Item Number": "1001",
                    "Customer Number": "JHC11",
                    "Credit Request Total": "100.00",
                    "RTN_CR_No": "",
                    "Status": "[2026-03-15 10:00:00] Submitted to billing",
                },
                {
                    "Date": "2026-03-03",
                    "Ticket Number": "R-2",
                    "Invoice Number": "INV2",
                    "Item Number": "1002",
                    "Customer Number": "SOS26",
                    "Credit Request Total": "200.00",
                    "RTN_CR_No": "",
                    "Status": "[2026-03-10 10:00:00] Investigation in progress",
                },
                {
                    "Date": "2026-03-08",
                    "Ticket Number": "R-3",
                    "Invoice Number": "INV3",
                    "Item Number": "1001",
                    "Customer Number": "JHC11",
                    "Credit Request Total": "150.00",
                    "RTN_CR_No": "CR-9",
                    "Status": "[2026-03-20 10:00:00] Credited",
                },
            ]
        )

        root_cause_payload = pd.DataFrame(
            {
                "Root Causes (Primary)": ["Price Discrepancy", "Sub Price Mismatch", "Price Discrepancy"],
                "Root Causes (All)": ["Price Discrepancy", "Sub Price Mismatch", "Price Discrepancy"],
                "Root Cause Mixed": [False, False, False],
            },
            index=df.index,
        )

        system_updates_meta = {
            "show_table": True,
            "suggestions": [
                {
                    "id": "system_updates",
                    "label": "System RTN updates preview (2026-03-01 → 2026-03-28)",
                    "prefix": "system rtn updates analysis from 2026-03-01 to 2026-03-28",
                }
            ],
            "csv_rows": pd.DataFrame(
                [
                    {
                        "Ticket Number": "R-3",
                        "Invoice Number": "INV3",
                        "Item Number": "1001",
                        "Customer Number": "JHC11",
                        "RTN_CR_No": "CR-9",
                        "Credit Request Total": 150.0,
                        "Update Source": "system",
                        "Update Event Time": "2026-03-20 10:00:00",
                        "Days To RTN Update": 12.0,
                    }
                ]
            ),
            "system_updates_summary": {
                "total_records": 1,
                "credit_total": 150.0,
                "avg_days_to_system_credit": 12.0,
                "median_days_to_system_credit": 12.0,
                "outlier_count": 0,
                "outlier_ticket_ids": [],
                "batch_dates": 1,
                "batched_dates": 0,
                "batched_records": 0,
                "batched_credit_total": 0.0,
                "largest_batch_count": 1,
                "largest_batch_date": "2026-03-20",
                "largest_batch_credit_total": 150.0,
                "manual_record_count": 0,
                "manual_credit_total": 0.0,
                "manual_avg_days_to_update": 0.0,
                "manual_median_days_to_update": 0.0,
                "manual_outlier_count": 0,
                "manual_outlier_ticket_ids": [],
                "manual_batch_dates": 0,
                "manual_batched_dates": 0,
                "manual_batched_records": 0,
                "manual_batched_credit_total": 0.0,
                "manual_largest_batch_count": 0,
                "manual_largest_batch_date": "N/A",
                "manual_largest_batch_credit_total": 0.0,
                "preview_total_records": 1,
            },
        }

        with patch("actus.intents.overall_summary._lookup_root_causes", return_value=root_cause_payload):
            with patch(
                "actus.intents.overall_summary.intent_system_updates",
                return_value=("System RTN updates analysis", None, system_updates_meta),
            ):
                text, rows, meta = intent_overall_summary("give me a credit overview this month", df)

        self.assertIsNone(rows)
        self.assertIn("Open exposure: **$300.00** across **2** record(s)", text)
        self.assertIn("Unique credited records in period: **$150.00** across **1** record(s)", text)
        self.assertIn("RTN update events captured: **1** event(s) / **$150.00**", text)
        self.assertIn("System-updated RTN events: **1** event(s) / **$150.00**", text)
        self.assertIn("Manual RTN-provided events: **0** event(s) / **$0.00**", text)
        self.assertIn("Records with both system and manual RTN activity: **0**", text)
        self.assertIn("Billing queue delay: **1** record(s) / **$100.00**", text)
        self.assertIn("Stale investigation: **1** record(s) / **$200.00**", text)
        self.assertIn("**JHC11** — $250.00", text)
        self.assertIn("**1001** — $250.00", text)
        self.assertIn("**Price Discrepancy** — **2** record(s) / $250.00", text)
        self.assertEqual(2, meta["overall_summary"]["open_record_count"])
        self.assertEqual(150.0, meta["overall_summary"]["credited_in_period"]["credited_credit_total"])
        self.assertEqual(1, meta["overall_summary"]["credited_in_period"]["credited_event_count"])
        self.assertEqual(0, meta["overall_summary"]["credited_in_period"]["records_with_both_sources"])
        self.assertEqual(
            {
                "id": "credit_ops_snapshot",
                "label": "Credit ops snapshot (2026-03-01 → 2026-03-29)",
                "prefix": "credit ops snapshot from 2026-03-01 to 2026-03-29",
            },
            meta["suggestions"][0],
        )
        self.assertEqual(
            {
                "id": "system_updates",
                "label": "System RTN updates preview (2026-03-01 → 2026-03-28)",
                "prefix": "system rtn updates analysis from 2026-03-01 to 2026-03-28",
            },
            meta["suggestions"][1],
        )
        self.assertEqual(
            {
                "id": "credit_amount_plot",
                "label": "Credit amount chart (2026-03-01 → 2026-03-29)",
                "prefix": "credit amount chart from 2026-03-01 to 2026-03-29",
            },
            meta["suggestions"][2],
        )

    def test_credit_overview_preserves_event_counts_when_same_record_has_both_sources(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "Date": "2026-03-05",
                    "Ticket Number": "R-10",
                    "Invoice Number": "INV10",
                    "Item Number": "1010",
                    "Customer Number": "JHC11",
                    "Credit Request Total": "125.00",
                    "RTN_CR_No": "CR-10",
                    "Status": "[2026-03-20 10:00:00] Credited",
                }
            ]
        )

        root_cause_payload = pd.DataFrame(
            {
                "Root Causes (Primary)": ["Price Discrepancy"],
                "Root Causes (All)": ["Price Discrepancy"],
                "Root Cause Mixed": [False],
            },
            index=df.index,
        )

        system_updates_meta = {
            "show_table": True,
            "suggestions": [],
            "csv_rows": pd.DataFrame(
                [
                    {
                        "Ticket Number": "R-10",
                        "Invoice Number": "INV10",
                        "Item Number": "1010",
                        "Customer Number": "JHC11",
                        "RTN_CR_No": "CR-10",
                        "Credit Request Total": 125.0,
                        "Update Source": "system",
                        "Update Event Time": "2026-03-18 10:00:00",
                        "Days To RTN Update": 13.0,
                    },
                    {
                        "Ticket Number": "R-10",
                        "Invoice Number": "INV10",
                        "Item Number": "1010",
                        "Customer Number": "JHC11",
                        "RTN_CR_No": "CR-10",
                        "Credit Request Total": 125.0,
                        "Update Source": "manual",
                        "Update Event Time": "2026-03-20 10:00:00",
                        "Days To RTN Update": 15.0,
                    },
                ]
            ),
            "system_updates_summary": {
                "total_records": 1,
                "credit_total": 125.0,
                "avg_days_to_system_credit": 13.0,
                "median_days_to_system_credit": 13.0,
                "outlier_count": 0,
                "outlier_ticket_ids": [],
                "batch_dates": 1,
                "batched_dates": 0,
                "batched_records": 0,
                "batched_credit_total": 0.0,
                "largest_batch_count": 1,
                "largest_batch_date": "2026-03-18",
                "largest_batch_credit_total": 125.0,
                "manual_record_count": 1,
                "manual_credit_total": 125.0,
                "manual_avg_days_to_update": 15.0,
                "manual_median_days_to_update": 15.0,
                "manual_outlier_count": 0,
                "manual_outlier_ticket_ids": [],
                "manual_batch_dates": 1,
                "manual_batched_dates": 0,
                "manual_batched_records": 0,
                "manual_batched_credit_total": 0.0,
                "manual_largest_batch_count": 1,
                "manual_largest_batch_date": "2026-03-20",
                "manual_largest_batch_credit_total": 125.0,
                "preview_total_records": 2,
            },
        }

        with patch("actus.intents.overall_summary._lookup_root_causes", return_value=root_cause_payload):
            with patch(
                "actus.intents.overall_summary.intent_system_updates",
                return_value=("System RTN updates analysis", None, system_updates_meta),
            ):
                text, rows, meta = intent_overall_summary("give me a credit overview this month", df)

        self.assertIsNone(rows)
        self.assertIn("Unique credited records in period: **$125.00** across **1** record(s)", text)
        self.assertIn("RTN update events captured: **2** event(s) / **$250.00**", text)
        self.assertIn("System-updated RTN events: **1** event(s) / **$125.00**", text)
        self.assertIn("Manual RTN-provided events: **1** event(s) / **$125.00**", text)
        self.assertIn("Records with both system and manual RTN activity: **1**", text)
        credited = meta["overall_summary"]["credited_in_period"]
        self.assertEqual(1, credited["credited_record_count"])
        self.assertEqual(125.0, credited["credited_credit_total"])
        self.assertEqual(2, credited["credited_event_count"])
        self.assertEqual(250.0, credited["credited_event_credit_total"])
        self.assertEqual(1, credited["records_with_both_sources"])


if __name__ == "__main__":
    unittest.main()
