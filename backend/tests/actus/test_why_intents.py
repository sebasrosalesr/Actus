import unittest
from unittest.mock import patch

import pandas as pd

from actus.intents.billing_queue_hotspots import intent_billing_queue_hotspots
from actus.intents.credit_root_causes import intent_root_cause_summary
from actus.intents.root_cause_rtn_timing import intent_root_cause_rtn_timing
from actus.intents.top_accounts import intent_top_accounts
from actus.intents.top_items import intent_top_items


class TestWhyIntents(unittest.TestCase):
    def test_top_accounts_uses_credited_scope(self) -> None:
        credited_rows = pd.DataFrame(
            [
                {
                    "Ticket Number": "R-1",
                    "Invoice Number": "INV1",
                    "Item Number": "1001",
                    "Customer Number": "ACD160",
                    "Credit Request Total": 1200.0,
                },
                {
                    "Ticket Number": "R-2",
                    "Invoice Number": "INV2",
                    "Item Number": "1002",
                    "Customer Number": "ETC07",
                    "Credit Request Total": 900.0,
                },
                {
                    "Ticket Number": "R-3",
                    "Invoice Number": "INV3",
                    "Item Number": "1003",
                    "Customer Number": "ACD160",
                    "Credit Request Total": 600.0,
                },
            ]
        )

        with patch(
            "actus.intents.top_accounts.credited_records_in_window",
            return_value=(credited_rows, {}, None, None, "2025-09-29 → 2026-03-29"),
        ):
            text, rows, meta = intent_top_accounts(
                "which customers are driving the most credited volume in the last 6 months",
                pd.DataFrame(),
            )

        self.assertIsNotNone(rows)
        assert rows is not None
        self.assertIn("credited volume", text.lower())
        self.assertEqual("ACD160", rows.iloc[0]["Account"])
        self.assertEqual("credited", meta["top_accounts_summary"]["scope"])

    def test_top_items_uses_open_scope(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "Date": "2026-03-05",
                    "Ticket Number": "R-1",
                    "Item Number": "1005365",
                    "Credit Request Total": "500.00",
                    "RTN_CR_No": "",
                },
                {
                    "Date": "2026-03-07",
                    "Ticket Number": "R-2",
                    "Item Number": "1005365",
                    "Credit Request Total": "250.00",
                    "RTN_CR_No": "",
                },
                {
                    "Date": "2026-03-06",
                    "Ticket Number": "R-3",
                    "Item Number": "1007986",
                    "Credit Request Total": "300.00",
                    "RTN_CR_No": "",
                },
                {
                    "Date": "2026-03-06",
                    "Ticket Number": "R-4",
                    "Item Number": "1005365",
                    "Credit Request Total": "100.00",
                    "RTN_CR_No": "CR-4",
                },
            ]
        )

        text, rows, meta = intent_top_items(
            "which items are driving the most open exposure this month",
            df,
        )

        self.assertIsNotNone(rows)
        assert rows is not None
        self.assertIn("open exposure", text.lower())
        self.assertEqual("1005365", rows.iloc[0]["Item"])
        self.assertEqual("open", meta["top_items_summary"]["scope"])

    def test_billing_queue_hotspots_groups_delays(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "Date": "2026-03-01",
                    "Ticket Number": "R-1",
                    "Invoice Number": "INV1",
                    "Item Number": "1001",
                    "Customer Number": "ETC07",
                    "Credit Request Total": "900.00",
                    "RTN_CR_No": "",
                    "Status": "[2026-03-05 10:00:00] Submitted to billing",
                },
                {
                    "Date": "2026-03-03",
                    "Ticket Number": "R-2",
                    "Invoice Number": "INV2",
                    "Item Number": "1002",
                    "Customer Number": "ETC07",
                    "Credit Request Total": "700.00",
                    "RTN_CR_No": "",
                    "Status": "[2026-03-06 10:00:00] Submitted to billing",
                },
                {
                    "Date": "2026-03-07",
                    "Ticket Number": "R-3",
                    "Invoice Number": "INV3",
                    "Item Number": "1003",
                    "Customer Number": "JHC11",
                    "Credit Request Total": "200.00",
                    "RTN_CR_No": "",
                    "Status": "[2026-03-08 10:00:00] Investigation in progress",
                },
            ]
        )

        text, rows, meta = intent_billing_queue_hotspots(
            "where are billing queue delays accumulating",
            df,
        )

        self.assertIsNotNone(rows)
        assert rows is not None
        self.assertIn("billing queue delay hotspots", text.lower())
        self.assertEqual("ETC07", meta["billing_queue_hotspots"]["top_customers"][0]["label"])

    def test_root_cause_rtn_timing_groups_by_avg_days(self) -> None:
        credited_rows = pd.DataFrame(
            [
                {
                    "Ticket Number": "R-1",
                    "Invoice Number": "INV1",
                    "Item Number": "1001",
                    "Credit Request Total": 100.0,
                    "Days To RTN Update": 12.0,
                },
                {
                    "Ticket Number": "R-2",
                    "Invoice Number": "INV2",
                    "Item Number": "1002",
                    "Credit Request Total": 200.0,
                    "Days To RTN Update": 31.0,
                },
            ]
        )
        root_causes = pd.DataFrame(
            {
                "Root Causes (Primary)": ["Price Discrepancy", "Item should be PPD"],
            },
            index=credited_rows.index,
        )

        with patch(
            "actus.intents.root_cause_rtn_timing.credited_records_in_window",
            return_value=(credited_rows, {}, None, None, "2025-09-29 → 2026-03-29"),
        ):
            with patch(
                "actus.intents.root_cause_rtn_timing._lookup_root_causes",
                return_value=root_causes,
            ):
                text, rows, meta = intent_root_cause_rtn_timing(
                    "which root causes are taking the longest to reach RTN assignment",
                    pd.DataFrame(),
                )

        self.assertIsNotNone(rows)
        assert rows is not None
        self.assertIn("longest time to rtn assignment", text.lower())
        self.assertEqual("Item should be PPD", rows.iloc[0]["Root Cause"])
        self.assertEqual(31.0, rows.iloc[0]["Avg Days To RTN"])
        self.assertEqual("Item should be PPD", meta["root_cause_rtn_timing"]["data"][0]["root_cause"])

    def test_root_cause_summary_respects_open_window_scope(self) -> None:
        now = pd.Timestamp.now().normalize()
        quarter_start_month = ((now.month - 1) // 3) * 3 + 1
        quarter_start = now.replace(month=quarter_start_month, day=1)
        prior_date = (quarter_start - pd.Timedelta(days=5)).strftime("%Y-%m-%d")
        in_scope = quarter_start.strftime("%Y-%m-%d")

        df = pd.DataFrame(
            [
                {
                    "Date": in_scope,
                    "Ticket Number": "R-1",
                    "Invoice Number": "INV1",
                    "Item Number": "1001",
                    "Credit Request Total": "100.00",
                    "RTN_CR_No": "",
                },
                {
                    "Date": in_scope,
                    "Ticket Number": "R-2",
                    "Invoice Number": "INV2",
                    "Item Number": "1002",
                    "Credit Request Total": "200.00",
                    "RTN_CR_No": "CR-2",
                },
                {
                    "Date": prior_date,
                    "Ticket Number": "R-3",
                    "Invoice Number": "INV3",
                    "Item Number": "1003",
                    "Credit Request Total": "500.00",
                    "RTN_CR_No": "",
                },
            ]
        )
        root_causes = pd.DataFrame(
            {
                "Root Causes (Primary)": ["Price Discrepancy", "Item should be PPD", "Sub Price Mismatch"],
                "Root Causes (All)": ["Price Discrepancy", "Item should be PPD", "Sub Price Mismatch"],
                "Root Cause Mixed": [False, False, False],
            },
            index=df.index,
        )

        with patch("actus.intents.credit_root_causes._lookup_root_causes", return_value=root_causes):
            _text, _rows, meta = intent_root_cause_summary(
                "what are the main root causes driving open exposure this quarter",
                df,
            )

        self.assertEqual("$100.00", meta["rootCauses"]["total"])
        self.assertEqual("Price Discrepancy", meta["rootCauses"]["data"][0]["root_cause"])


if __name__ == "__main__":
    unittest.main()
