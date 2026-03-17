import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from actus.intent_router import actus_answer
from actus.intents.ticket_analysis import intent_ticket_analysis


class TestTicketAnalysisIntent(unittest.TestCase):
    def test_intent_ticket_analysis_explicit_query(self) -> None:
        fake_analysis = {
            "ticket_id": "R-058284",
            "primary_root_cause": "ppd_mismatch",
            "supporting_root_causes": ["sub_price_mismatch"],
            "credit_total": 120.5,
            "line_count": 3,
            "is_credited": True,
            "answer": "Ticket R-058284 analysis summary.",
        }
        service = MagicMock()
        service.analyze_ticket.return_value = fake_analysis

        with patch("actus.intents.ticket_analysis.get_runtime_service", return_value=service):
            text, rows, meta = intent_ticket_analysis("analyze ticket R-058284", pd.DataFrame())

        self.assertIsNone(rows)
        self.assertIn("analysis generated", text.lower())
        self.assertIn("ticket_analysis", meta)
        self.assertEqual("R-058284", meta["ticket_analysis"]["ticket_id"])
        self.assertIn("suggestions", meta)
        self.assertEqual(2, len(meta["suggestions"]))
        self.assertEqual("ticket status R-058284", meta["suggestions"][0]["prefix"])
        service.analyze_ticket.assert_called_once_with(ticket_id="R-058284", threshold_days=30)

    def test_intent_ticket_analysis_missing_ticket_id(self) -> None:
        response = intent_ticket_analysis("analyze ticket", pd.DataFrame())
        self.assertIsNotNone(response)
        assert response is not None
        text, rows, meta = response
        self.assertIsNone(rows)
        self.assertEqual({}, meta)
        self.assertIn("Please provide a ticket id", text)

    def test_intent_ticket_analysis_does_not_capture_ticket_status(self) -> None:
        response = intent_ticket_analysis("ticket status R-058284", pd.DataFrame())
        self.assertIsNone(response)

    def test_router_routes_analyze_ticket_to_new_intent(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "Ticket Number": "R-058284",
                    "Date": "2026-03-01",
                    "Status": "open",
                }
            ]
        )
        fake_analysis = {
            "ticket_id": "R-058284",
            "primary_root_cause": "ppd_mismatch",
            "supporting_root_causes": [],
            "credit_total": 12.0,
            "line_count": 1,
            "is_credited": False,
            "answer": "Ticket R-058284 analysis summary.",
        }
        service = MagicMock()
        service.analyze_ticket.return_value = fake_analysis

        with patch("actus.intents.ticket_analysis.get_runtime_service", return_value=service):
            text, rows, meta = actus_answer("analyze ticket R-058284", df)

        self.assertIsNone(rows)
        self.assertIn("analysis generated", text.lower())
        self.assertIn("ticket_analysis", meta)
        self.assertEqual("ticket_analysis", meta.get("intent_id"))
        self.assertEqual("R-058284", meta["ticket_analysis"]["ticket_id"])
        self.assertIn("suggestions", meta)
        service.analyze_ticket.assert_called_once()

    def test_router_keeps_ticket_status_behavior(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "Date": pd.Timestamp("2026-03-01"),
                    "Ticket Number": "R-058284",
                    "Customer Number": "ABC123",
                    "Invoice Number": "INV123456",
                    "Item Number": "1007986",
                    "Credit Request Total": "100.00",
                    "Status": "2026-03-01 10:00:00 Open",
                }
            ]
        )
        with patch("actus.intents.ticket_analysis.get_runtime_service") as mock_service:
            text, rows, meta = actus_answer("ticket status R-058284", df)

        self.assertIsNotNone(rows)
        self.assertIn("snapshot", text.lower())
        self.assertNotIn("ticket_analysis", meta)
        mock_service.assert_not_called()

    def test_intent_highlights_are_compact_when_openrouter_fails(self) -> None:
        huge_highlight = (
            "Case File INV14948550008-731-01: Background: - Case Number: R-064044 - Case Title: ASV Credits "
            "- Date Opened: 2026-02-19 - Status: Open - Invoice Numbers: INV14948550 INV14998028 INV14779181 "
            "Price Trace Review All items configured under PPD and invoice prices aligned at billing time. "
            "Order History Review - No substitutions - No manual overrides - Standard fulfillment. "
            "Price History Review values loaded before PPD updates. "
            "Finding: All invoice prices align with active price sheets at billing; no unstable pricing entries."
        )
        fake_analysis = {
            "ticket_id": "R-064044",
            "primary_root_cause": "ppd_mismatch",
            "supporting_root_causes": ["price_discrepancy"],
            "credit_total": 230.08,
            "line_count": 4,
            "is_credited": False,
            "answer": "Ticket R-064044 analysis summary.",
            "investigation_highlights": [huge_highlight, huge_highlight],
        }
        service = MagicMock()
        service.analyze_ticket.return_value = fake_analysis

        with patch("actus.intents.ticket_analysis.get_runtime_service", return_value=service):
            with patch("actus.intents.ticket_analysis.openrouter_chat", side_effect=RuntimeError("upstream unavailable")):
                _, _, meta = intent_ticket_analysis("analyze ticket R-064044", pd.DataFrame())

        highlights = meta["ticket_analysis"]["investigation_highlights"]
        self.assertGreaterEqual(len(highlights), 1)
        self.assertLessEqual(len(highlights), 4)
        self.assertTrue(all(len(str(h)) <= 180 for h in highlights))
        self.assertTrue(all("️⃣" not in str(h) for h in highlights))

    def test_intent_ticket_analysis_adds_mixed_lines_suggestion_when_partial(self) -> None:
        fake_analysis = {
            "ticket_id": "R-062817",
            "primary_root_cause": "price_loaded_after_invoice",
            "supporting_root_causes": [],
            "credit_total": 6278.40,
            "line_count": 34,
            "is_credited": False,
            "is_partially_credited": True,
            "answer": "Ticket R-062817 analysis summary.",
        }
        service = MagicMock()
        service.analyze_ticket.return_value = fake_analysis

        with patch("actus.intents.ticket_analysis.get_runtime_service", return_value=service):
            _, _, meta = intent_ticket_analysis("analyze ticket R-062817", pd.DataFrame())

        suggestions = meta.get("suggestions", [])
        self.assertEqual(3, len(suggestions))
        self.assertEqual("mixed_lines", suggestions[2]["id"])
        self.assertEqual("mixed lines R-062817", suggestions[2]["prefix"])


if __name__ == "__main__":
    unittest.main()
