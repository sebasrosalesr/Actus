import unittest

import pandas as pd

from actus.intent_router import actus_answer
from actus.intents.credit_amount_plot import intent_credit_amount_plot


def _sample_df() -> pd.DataFrame:
    now_utc = pd.Timestamp.now(tz="UTC")
    return pd.DataFrame(
        [
            {
                "Date": now_utc - pd.Timedelta(days=2),
                "Credit Request Total": "120.50",
                "RTN_CR_No": "CR-1",
            },
            {
                "Date": now_utc - pd.Timedelta(days=10),
                "Credit Request Total": "80.00",
                "RTN_CR_No": "",
            },
            {
                "Date": now_utc - pd.Timedelta(days=40),
                "Credit Request Total": "30.00",
                "RTN_CR_No": "CR-2",
            },
            {
                "Date": now_utc - pd.Timedelta(days=150),
                "Credit Request Total": "25.00",
                "RTN_CR_No": "",
            },
            {
                "Date": now_utc - pd.Timedelta(days=300),
                "Credit Request Total": "15.00",
                "RTN_CR_No": "CR-3",
            },
        ]
    )


class TestCreditAmountPlotIntent(unittest.TestCase):
    def test_plot_last_3_weeks(self) -> None:
        text, rows, meta = intent_credit_amount_plot("plot last 3 weeks", _sample_df())
        self.assertIsNone(rows)
        self.assertIn("Window used", text)
        self.assertIn("chart", meta)
        self.assertEqual("credit_amount_trend", meta["chart"]["kind"])
        self.assertIn("last 3 weeks", meta["chart"]["window"])
        self.assertGreater(len(meta["chart"]["data"]), 0)

    def test_plot_last_4_months(self) -> None:
        text, rows, meta = intent_credit_amount_plot("chart last 4 months", _sample_df())
        self.assertIsNone(rows)
        self.assertIn("Window used", text)
        self.assertIn("chart", meta)
        self.assertIn("last 4 months", meta["chart"]["window"])
        self.assertGreater(len(meta["chart"]["data"]), 0)

    def test_plot_a_year_ago(self) -> None:
        text, rows, meta = intent_credit_amount_plot("graph a year ago", _sample_df())
        self.assertIsNone(rows)
        self.assertIn("Window used", text)
        self.assertIn("chart", meta)
        self.assertIn("from 1 year ago", meta["chart"]["window"])
        self.assertGreater(len(meta["chart"]["data"]), 0)

    def test_router_runs_plot_directly(self) -> None:
        text, rows, meta = actus_answer("plot last 3 weeks", _sample_df())
        self.assertIsNone(rows)
        self.assertIn("chart", meta)
        self.assertNotIn("What date range should I use", text)


if __name__ == "__main__":
    unittest.main()
