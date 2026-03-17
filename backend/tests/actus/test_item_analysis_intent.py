import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from actus.intent_router import actus_answer
from actus.intents.item_analysis import intent_item_analysis


class TestItemAnalysisIntent(unittest.TestCase):
    def test_intent_item_analysis_explicit_query(self) -> None:
        fake_analysis = {
            "item_number": "1007986",
            "ticket_count": 13,
            "invoice_count": 35,
            "line_count": 37,
            "total_credit": 56855.09,
            "answer": "Item 1007986 analysis summary.",
        }
        service = MagicMock()
        service.analyze_item.return_value = fake_analysis

        with patch("actus.intents.item_analysis.get_runtime_service", return_value=service):
            text, rows, meta = intent_item_analysis("analyze item 1007986", pd.DataFrame())

        self.assertIsNone(rows)
        self.assertIn("analysis generated", text.lower())
        self.assertIn("item_analysis", meta)
        self.assertEqual("1007986", meta["item_analysis"]["item_number"])
        service.analyze_item.assert_called_once_with(item_number="1007986")

    def test_intent_item_analysis_missing_item_number(self) -> None:
        response = intent_item_analysis("analyze item", pd.DataFrame())
        self.assertIsNotNone(response)
        assert response is not None
        text, rows, meta = response
        self.assertIsNone(rows)
        self.assertEqual({}, meta)
        self.assertIn("Please provide an item number", text)

    def test_intent_item_analysis_does_not_capture_top_items(self) -> None:
        response = intent_item_analysis("top items last month", pd.DataFrame())
        self.assertIsNone(response)

    def test_router_routes_analyze_item_to_new_intent(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "Ticket Number": "R-058284",
                    "Item Number": "1007986",
                    "Date": "2026-03-01",
                }
            ]
        )
        fake_analysis = {
            "item_number": "1007986",
            "ticket_count": 2,
            "invoice_count": 4,
            "line_count": 5,
            "total_credit": 120.0,
            "answer": "Item 1007986 analysis summary.",
        }
        service = MagicMock()
        service.analyze_item.return_value = fake_analysis

        with patch("actus.intents.item_analysis.get_runtime_service", return_value=service):
            text, rows, meta = actus_answer("analyze item 1007986", df)

        self.assertIsNone(rows)
        self.assertIn("analysis generated", text.lower())
        self.assertIn("item_analysis", meta)
        self.assertEqual("1007986", meta["item_analysis"]["item_number"])
        service.analyze_item.assert_called_once_with(item_number="1007986")


if __name__ == "__main__":
    unittest.main()
