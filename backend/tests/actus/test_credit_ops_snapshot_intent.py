import unittest

import pandas as pd

from actus.intents.credit_ops_snapshot import intent_credit_ops_snapshot


class TestCreditOpsSnapshotIntent(unittest.TestCase):
    def test_snapshot_includes_total_credited_and_primary_root_cause(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "Date": "2026-03-01",
                    "Ticket Number": "R-1",
                    "Status": "[2026-03-02 10:00:00] Credited",
                    "RTN_CR_No": "CR-1",
                    "Credit Request Total": 150.0,
                    "Root Causes": "Price discrepancy",
                },
                {
                    "Date": "2026-03-03",
                    "Ticket Number": "R-2",
                    "Status": "[2026-03-04 10:00:00] Submitted to billing",
                    "RTN_CR_No": "",
                    "Credit Request Total": 50.0,
                    "Root Causes": "Price discrepancy",
                },
                {
                    "Date": "2026-03-05",
                    "Ticket Number": "R-3",
                    "Status": "[2026-03-06 10:00:00] WIP review",
                    "RTN_CR_No": "",
                    "Credit Request Total": 75.0,
                    "Root Causes": "Item should be PPD",
                },
            ]
        )

        response = intent_credit_ops_snapshot(
            "credit ops snapshot between 2026-01-01 and 2026-03-31",
            df,
        )

        self.assertIsNotNone(response)
        assert response is not None
        text, rows, meta = response
        self.assertIn("- Total credited: **$275.00**", text)
        self.assertIn("- Primary root cause: **Price discrepancy**", text)
        self.assertEqual(3, len(rows))
        self.assertTrue(meta.get("show_table"))


if __name__ == "__main__":
    unittest.main()
