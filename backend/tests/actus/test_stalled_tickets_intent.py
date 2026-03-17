import unittest

import pandas as pd

from actus.intents.stalled_tickets import intent_stalled_tickets


def _make_df() -> pd.DataFrame:
    today = pd.Timestamp.today().normalize()
    return pd.DataFrame(
        [
            {
                "Ticket Number": "R-900001",
                "Customer Number": "ACCT1",
                "Update Timestamp": today - pd.Timedelta(days=40),
                "Date": today - pd.Timedelta(days=55),
                "Status": "Submitted to Billing",
                "Credit Request Total": 5000.0,
                "RTN_CR_No": "",
                "Reason for Credit": "test",
            },
            {
                "Ticket Number": "R-900002",
                "Customer Number": "ACCT2",
                "Update Timestamp": today - pd.Timedelta(days=22),
                "Date": today - pd.Timedelta(days=25),
                "Status": "Went through investigation",
                "Credit Request Total": 1200.0,
                "RTN_CR_No": "",
                "Reason for Credit": "test",
            },
            {
                "Ticket Number": "R-900003",
                "Customer Number": "ACCT3",
                "Update Timestamp": today - pd.Timedelta(days=35),
                "Date": today - pd.Timedelta(days=50),
                "Status": "Pending: intake",
                "Credit Request Total": 800.0,
                "RTN_CR_No": "",
                "Reason for Credit": "test",
            },
        ]
    )


class TestStalledTicketsIntent(unittest.TestCase):
    def test_time_reasoning_highlights_include_avg_median_exposure(self) -> None:
        response = intent_stalled_tickets("show stalled tickets", _make_df())
        self.assertIsNotNone(response)
        assert response is not None
        text, rows, meta = response

        self.assertIsNotNone(rows)
        self.assertEqual(3, len(rows))
        self.assertTrue(meta.get("show_table"))

        self.assertIn("Aging not submitted: 1 ticket(s) • avg ", text)
        self.assertIn("Billing queue delay: 1 ticket(s) • avg ", text)
        self.assertIn("Stale investigation: 1 ticket(s) • avg ", text)
        self.assertIn("• median ", text)
        self.assertIn("• exposure $", text)


if __name__ == "__main__":
    unittest.main()
