import unittest

import pandas as pd

from actus.intent_router import actus_answer


def _anomaly_df() -> pd.DataFrame:
    today = pd.Timestamp.today().normalize()
    rows = [
        {
            "Date": today - pd.Timedelta(days=i),
            "Credit Request Total": 100.0 + float(i % 5) * 10.0,
            "Customer Number": f"CUST{i % 3}",
            "Item Number": f"ITEM{i % 4}",
            "Sales Rep": f"REP{i % 2}",
        }
        for i in range(20)
    ]
    rows.append(
        {
            "Date": today - pd.Timedelta(days=1),
            "Credit Request Total": 6000.0,
            "Customer Number": "CUST0",
            "Item Number": "ITEM0",
            "Sales Rep": "REP0",
        }
    )
    return pd.DataFrame(rows)


class TestCreditAnomaliesRouting(unittest.TestCase):
    def test_anomalies_shortcut_uses_default_90_day_window(self) -> None:
        text, rows, meta = actus_answer("anomalies", _anomaly_df())
        self.assertIsNotNone(rows)
        self.assertIn("Credit Anomaly Scan", text)
        self.assertIn("Last 90 Days", text)
        self.assertEqual("credit_anomalies", meta.get("intent_id"))
        self.assertEqual("alias", meta.get("intent_matched_by"))

    def test_misspelled_anomaly_dection_routes_to_anomaly_scan(self) -> None:
        text, rows, meta = actus_answer("anomaly dection", _anomaly_df())
        self.assertIsNotNone(rows)
        self.assertIn("Credit Anomaly Scan", text)
        self.assertIn("Last 90 Days", text)
        self.assertEqual("credit_anomalies", meta.get("intent_id"))
        self.assertEqual("alias", meta.get("intent_matched_by"))


if __name__ == "__main__":
    unittest.main()
