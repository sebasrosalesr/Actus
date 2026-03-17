import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from actus.intent_router import actus_answer
from actus.intents.customer_analysis import intent_customer_analysis


class TestCustomerAnalysisIntent(unittest.TestCase):
    def test_intent_customer_analysis_explicit_query(self) -> None:
        fake_analysis = {
            "query": "SGP",
            "match_mode": "account_prefix",
            "normalized_query": "SGP",
            "ticket_count": 2,
            "credit_total": 446.79,
            "answer": "Account prefix SGP analysis summary.",
        }
        service = MagicMock()
        service.analyze_customer.return_value = fake_analysis

        with patch("actus.intents.customer_analysis.get_runtime_service", return_value=service):
            text, rows, meta = intent_customer_analysis("analyze account SGP", pd.DataFrame())

        self.assertIsNone(rows)
        self.assertIn("analysis generated", text.lower())
        self.assertIn("customer_analysis", meta)
        self.assertEqual("SGP", meta["customer_analysis"]["normalized_query"])
        self.assertIn("suggestions", meta)
        service.analyze_customer.assert_called_once_with(
            customer_query="SGP",
            match_mode="account_prefix",
            threshold_days=30,
        )

    def test_intent_customer_analysis_missing_query(self) -> None:
        response = intent_customer_analysis("analyze account", pd.DataFrame())
        self.assertIsNotNone(response)
        assert response is not None
        text, rows, meta = response
        self.assertIsNone(rows)
        self.assertEqual({}, meta)
        self.assertIn("Please provide an account prefix", text)

    def test_router_routes_analyze_account_to_new_intent(self) -> None:
        fake_analysis = {
            "query": "SGP",
            "match_mode": "account_prefix",
            "normalized_query": "SGP",
            "ticket_count": 2,
            "credit_total": 446.79,
            "answer": "Account prefix SGP analysis summary.",
        }
        service = MagicMock()
        service.analyze_customer.return_value = fake_analysis

        with patch("actus.intents.customer_analysis.get_runtime_service", return_value=service):
            text, rows, meta = actus_answer("analyze account SGP", pd.DataFrame())

        self.assertIsNone(rows)
        self.assertIn("analysis generated", text.lower())
        self.assertEqual("customer_analysis", meta.get("intent_id"))
        self.assertIn("customer_analysis", meta)
        service.analyze_customer.assert_called_once()


if __name__ == "__main__":
    unittest.main()
