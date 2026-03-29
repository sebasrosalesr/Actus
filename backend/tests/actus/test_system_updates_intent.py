import unittest

import pandas as pd

from actus.intents.system_updates import intent_system_updates


class TestSystemUpdatesIntent(unittest.TestCase):
    def test_returns_collapsible_summary_metadata(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "Status": "[2026-03-19 10:00:00] Updated by the system [2026-03-18 09:00:00] Manual review",
                    "RTN_CR_No": "CR-1",
                    "Date": "2026-03-01",
                    "Ticket Number": "R-1",
                    "Credit Request Total": 10.0,
                },
                {
                    "Status": "[2026-03-18 08:00:00] Updated by the system",
                    "RTN_CR_No": "CR-2",
                    "Date": "2026-03-02",
                    "Ticket Number": "R-2",
                    "Credit Request Total": 20.0,
                },
                {
                    "Status": "[2026-03-17 07:00:00] Updated by the system",
                    "RTN_CR_No": "CR-3",
                    "Date": "2026-03-03",
                    "Ticket Number": "R-3",
                    "Credit Request Total": 30.0,
                },
                {
                    "Status": "[2026-03-16 06:00:00] Updated by the system",
                    "RTN_CR_No": "CR-4",
                    "Date": "2026-03-04",
                    "Ticket Number": "R-4",
                    "Credit Request Total": 40.0,
                },
            ]
        )

        response = intent_system_updates("show system updates with rtn", df)

        self.assertIsNotNone(response)
        assert response is not None
        text, rows, meta = response
        self.assertIn("System RTN updates analysis:", text)
        self.assertIn("System-updated records with RTN/CR: **4**", text)
        self.assertEqual(4, len(rows))
        self.assertIn("Credit Request Total", rows.columns)
        self.assertEqual([10.0, 20.0, 30.0, 40.0], rows["Credit Request Total"].tolist())
        summary = meta.get("system_updates_summary")
        self.assertIsInstance(summary, dict)
        assert isinstance(summary, dict)
        self.assertEqual(4, summary.get("total_records"))
        self.assertEqual(0, summary.get("manual_record_count"))
        self.assertEqual(4, summary.get("batch_dates"))
        self.assertEqual(10.0, float(rows.iloc[0]["Batch Credit Total"]))


if __name__ == "__main__":
    unittest.main()
