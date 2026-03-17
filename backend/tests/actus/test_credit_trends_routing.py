import unittest

import pandas as pd

from actus.intent_router import actus_answer


def _trend_df() -> pd.DataFrame:
    today = pd.Timestamp.today().normalize()
    rows = []
    for i in range(70):
        rows.append(
            {
                "Date": today - pd.Timedelta(days=i),
                "Credit Request Total": 10.0 + (i % 5),
                "Customer Number": f"CUST{i % 3}",
                "Item Number": f"ITEM{i % 4}",
                "Sales Rep": f"REP{i % 2}",
                "RTN_CR_No": "" if i % 2 else f"RTN{i:04d}",
            }
        )
    return pd.DataFrame(rows)


class TestCreditTrendsRouting(unittest.TestCase):
    def test_credit_trends_query_routes_to_trends_intent(self) -> None:
        text, rows, meta = actus_answer("credit trends", _trend_df())
        self.assertIsNone(rows)
        self.assertIn("Credit Trends Analysis", text)
        self.assertIn("creditTrends", meta)
        self.assertEqual("credit_trends", meta.get("intent_id"))
        self.assertNotIn("follow_up", meta)

    def test_plot_query_still_routes_to_plot_intent(self) -> None:
        text, rows, meta = actus_answer("plot credit trends", _trend_df())
        self.assertIsNone(rows)
        self.assertIn("What date range should I use", text)
        self.assertEqual("credit_amount_plot", meta.get("intent_id"))
        self.assertIn("follow_up", meta)
        self.assertEqual("credit_amount_plot", meta["follow_up"]["intent"])


if __name__ == "__main__":
    unittest.main()
