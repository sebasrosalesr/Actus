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
                },
                {
                    "Status": "[2026-03-18 08:00:00] Updated by the system",
                    "RTN_CR_No": "CR-2",
                    "Date": "2026-03-02",
                    "Ticket Number": "R-2",
                },
                {
                    "Status": "[2026-03-17 07:00:00] Updated by the system",
                    "RTN_CR_No": "CR-3",
                    "Date": "2026-03-03",
                    "Ticket Number": "R-3",
                },
                {
                    "Status": "[2026-03-16 06:00:00] Updated by the system",
                    "RTN_CR_No": "CR-4",
                    "Date": "2026-03-04",
                    "Ticket Number": "R-4",
                },
            ]
        )

        response = intent_system_updates("show system updates with rtn", df)

        self.assertIsNotNone(response)
        assert response is not None
        text, rows, meta = response
        self.assertIn("showing the 3 most recent", text)
        self.assertEqual(4, len(rows))
        summary = meta.get("system_updates_summary")
        self.assertIsInstance(summary, dict)
        assert isinstance(summary, dict)
        self.assertEqual(4, summary.get("total_records"))
        self.assertEqual(4, summary.get("total_update_dates"))
        self.assertEqual(3, summary.get("recent_limit"))
        self.assertEqual(
            [
                {"date": "2026-03-19", "count": 1},
                {"date": "2026-03-18", "count": 1},
                {"date": "2026-03-17", "count": 1},
                {"date": "2026-03-16", "count": 1},
            ],
            summary.get("batches"),
        )


if __name__ == "__main__":
    unittest.main()
