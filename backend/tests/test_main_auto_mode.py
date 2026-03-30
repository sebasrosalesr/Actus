import unittest
from unittest.mock import patch

import pandas as pd

import main


class TestAskAutoMode(unittest.TestCase):
    def setUp(self) -> None:
        main._ASK_CACHE.clear()

    def test_ask_request_defaults_to_manual_mode(self) -> None:
        payload = main.AskRequest(query="credit trends")
        self.assertEqual("manual", payload.mode)

    def test_ask_uses_manual_router_by_default(self) -> None:
        with patch("main._get_df", return_value=pd.DataFrame()) as mock_get_df:
            with patch(
                "main.actus_answer",
                return_value=("manual answer", None, {"intent_id": "credit_trends"}),
            ) as mock_manual:
                with patch("main.auto_mode_answer") as mock_auto:
                    response = main.ask(main.AskRequest(query="credit trends"))

        self.assertEqual("manual answer", response["text"])
        self.assertEqual("credit_trends", response["meta"]["intent_id"])
        mock_get_df.assert_called_once()
        mock_manual.assert_called_once()
        mock_auto.assert_not_called()

    def test_ask_uses_auto_orchestrator_when_requested(self) -> None:
        with patch("main._get_df", return_value=pd.DataFrame()) as mock_get_df:
            with patch(
                "main.auto_mode_answer",
                return_value=(
                    "auto answer",
                    None,
                    {
                        "intent_id": "auto_mode",
                        "auto_mode": {
                            "enabled": True,
                            "planner": "deterministic_first",
                            "primary_intent": "ticket_analysis",
                            "executed_intents": [
                                {
                                    "id": "ticket_analysis",
                                    "label": "Analyze ticket",
                                    "status": "ok",
                                }
                            ],
                            "subintent_count": 1,
                        },
                    },
                ),
            ) as mock_auto:
                with patch("main.actus_answer") as mock_manual:
                    response = main.ask(
                        main.AskRequest(
                            query="analyze ticket R-065314",
                            mode="auto",
                        )
                    )

        self.assertEqual("auto answer", response["text"])
        self.assertEqual("auto_mode", response["meta"]["intent_id"])
        mock_get_df.assert_called_once()
        mock_auto.assert_called_once()
        mock_manual.assert_not_called()

    def test_auto_ask_reuses_cached_response_for_same_dataframe_window(self) -> None:
        df = pd.DataFrame()
        with patch("main._get_df", return_value=df) as mock_get_df:
            with patch(
                "main.auto_mode_answer",
                return_value=(
                    "auto answer",
                    None,
                    {
                        "intent_id": "auto_mode",
                        "auto_mode": {
                            "enabled": True,
                            "planner": "deterministic_first",
                            "primary_intent": "overall_summary",
                            "executed_intents": [],
                            "subintent_count": 1,
                        },
                    },
                ),
            ) as mock_auto:
                first = main.ask(main.AskRequest(query="give me a credit overview", mode="auto"))
                second = main.ask(main.AskRequest(query="give me a credit overview", mode="auto"))

        self.assertEqual("auto answer", first["text"])
        self.assertEqual("auto answer", second["text"])
        self.assertEqual("auto_mode", second["meta"]["intent_id"])
        self.assertEqual(2, mock_get_df.call_count)
        mock_auto.assert_called_once()
        self.assertEqual(1, len(main._ASK_CACHE))


if __name__ == "__main__":
    unittest.main()
