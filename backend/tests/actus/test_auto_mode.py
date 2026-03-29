import unittest
from unittest.mock import patch

import pandas as pd

from actus import auto_mode


class TestAutoModePlanner(unittest.TestCase):
    def test_ticket_query_uses_ticket_analysis_only(self) -> None:
        plan = auto_mode.plan_auto_mode("analyze ticket R-065314")

        self.assertIsNotNone(plan)
        assert plan is not None
        self.assertEqual("entity", plan.family)
        self.assertEqual("ticket_analysis", plan.primary_intent)
        self.assertEqual(["ticket_analysis"], [item.id for item in plan.intents])

    def test_ticket_query_with_notes_adds_investigation_notes(self) -> None:
        plan = auto_mode.plan_auto_mode(
            "analyze ticket R-065314 and include investigation notes and evidence"
        )

        self.assertIsNotNone(plan)
        assert plan is not None
        self.assertEqual(
            ["ticket_analysis", "investigation_notes"],
            [item.id for item in plan.intents],
        )

    def test_portfolio_query_orders_specialists_by_priority(self) -> None:
        plan = auto_mode.plan_auto_mode(
            "show anomalies, root causes, and aging for the last 90 days"
        )

        self.assertIsNotNone(plan)
        assert plan is not None
        self.assertEqual("portfolio", plan.family)
        self.assertEqual(
            ["credit_anomalies", "credit_root_causes", "credit_aging"],
            [item.id for item in plan.intents],
        )

    def test_entity_query_wins_over_portfolio_terms(self) -> None:
        plan = auto_mode.plan_auto_mode(
            "analyze ticket R-065314 with root causes and anomalies"
        )

        self.assertIsNotNone(plan)
        assert plan is not None
        self.assertEqual("entity", plan.family)
        self.assertEqual(["ticket_analysis"], [item.id for item in plan.intents])
        self.assertTrue(any(item.get("id") == "credit_anomalies" for item in plan.suggestions))

    def test_system_updates_query_selects_system_updates(self) -> None:
        plan = auto_mode.plan_auto_mode(
            "show me system RTN updates analysis for the last 2 months"
        )

        self.assertIsNotNone(plan)
        assert plan is not None
        self.assertEqual("portfolio", plan.family)
        self.assertEqual(["system_updates"], [item.id for item in plan.intents])

    def test_plural_customer_credit_query_uses_top_accounts(self) -> None:
        plan = auto_mode.plan_auto_mode(
            "which customers are driving the most credited volume in the last 6 months"
        )

        self.assertIsNotNone(plan)
        assert plan is not None
        self.assertEqual("portfolio", plan.family)
        self.assertEqual(["top_accounts"], [item.id for item in plan.intents])

    def test_item_open_exposure_query_uses_top_items(self) -> None:
        plan = auto_mode.plan_auto_mode(
            "which items are driving the most open exposure this month"
        )

        self.assertIsNotNone(plan)
        assert plan is not None
        self.assertEqual("portfolio", plan.family)
        self.assertEqual(["top_items"], [item.id for item in plan.intents])

    def test_billing_queue_query_uses_hotspot_intent(self) -> None:
        plan = auto_mode.plan_auto_mode("where are billing queue delays accumulating")

        self.assertIsNotNone(plan)
        assert plan is not None
        self.assertEqual("portfolio", plan.family)
        self.assertEqual(["billing_queue_hotspots"], [item.id for item in plan.intents])

    def test_root_cause_timing_query_uses_timing_intent(self) -> None:
        plan = auto_mode.plan_auto_mode(
            "which root causes are taking the longest to reach RTN assignment"
        )

        self.assertIsNotNone(plan)
        assert plan is not None
        self.assertEqual("portfolio", plan.family)
        self.assertEqual(["root_cause_rtn_timing"], [item.id for item in plan.intents])


class TestAutoModeExecution(unittest.TestCase):
    def test_partial_failure_returns_successful_specialists(self) -> None:
        def fake_ticket(_query: str, _df: pd.DataFrame):
            return (
                "Ticket analysis generated.",
                None,
                {
                    "ticket_analysis": {
                        "ticket_id": "R-065314",
                        "primary_root_cause": "price_discrepancy",
                        "supporting_root_causes": [],
                        "credit_total": 4987.42,
                        "line_count": 2,
                        "is_credited": False,
                        "pending_line_count": 2,
                        "credited_line_count": 0,
                        "investigation_highlights": ["Conflicting price evidence was captured."],
                    },
                    "suggestions": [
                        {
                            "id": "ticket_status",
                            "label": "Ticket status (R-065314)",
                            "prefix": "ticket status R-065314",
                        }
                    ],
                },
            )

        def fake_notes(_query: str, _df: pd.DataFrame):
            raise RuntimeError("note service unavailable")

        fake_defs = [
            {
                "id": "ticket_analysis",
                "label": "Analyze ticket",
                "prefix": "analyze ticket",
                "func": fake_ticket,
                "aliases": [],
            },
            {
                "id": "investigation_notes",
                "label": "Investigation notes",
                "prefix": "investigation notes",
                "func": fake_notes,
                "aliases": [],
            },
        ]

        with patch.object(auto_mode, "INTENT_DEFS", fake_defs):
            with patch("actus.auto_mode.openrouter_chat", side_effect=RuntimeError("offline")):
                text, rows, meta = auto_mode.auto_mode_answer(
                    "analyze ticket R-065314 with investigation notes",
                    pd.DataFrame(),
                )

        self.assertIsNone(rows)
        self.assertEqual("auto_mode", meta.get("intent_id"))
        self.assertEqual(1, meta["auto_mode"]["subintent_count"])
        self.assertEqual(
            ["ok", "error"],
            [item["status"] for item in meta["auto_mode"]["executed_intents"]],
        )
        self.assertIn("Execution Status", text)
        self.assertIn("Ticket status", text)

    def test_llm_synthesis_is_used_when_available(self) -> None:
        def fake_ticket(_query: str, _df: pd.DataFrame):
            return (
                "Ticket analysis generated.",
                None,
                {
                    "ticket_analysis": {
                        "ticket_id": "R-067298",
                        "primary_root_cause": "price_discrepancy",
                        "supporting_root_causes": ["sub_price_mismatch"],
                        "credit_total": 4900.66,
                        "line_count": 98,
                        "is_credited": False,
                        "pending_line_count": 98,
                        "credited_line_count": 0,
                        "investigation_highlights": [
                            "Price for item 1005365 was inconsistently communicated as $11.80, $14.16, and $11.76."
                        ],
                    },
                    "suggestions": [
                        {
                            "id": "ticket_status",
                            "label": "Ticket status (R-067298)",
                            "prefix": "ticket status R-067298",
                        }
                    ],
                },
            )

        def fake_notes(_query: str, _df: pd.DataFrame):
            return (
                "Here are the investigation notes for ticket **R-067298**.\n\n"
                "Available notes:\n"
                "- **INV15003283|1005365** — Investigation for INV15003283 · 1005365 "
                "(Updated: 2026-03-27T18:19:50Z) • Note ID: `-OokWXF5wGWNitjpYmaZ` "
                "• `ask:Show%20investigation%20note|Open note`",
                None,
                {"show_table": False},
            )

        fake_defs = [
            {
                "id": "ticket_analysis",
                "label": "Analyze ticket",
                "prefix": "analyze ticket",
                "func": fake_ticket,
                "aliases": [],
            },
            {
                "id": "investigation_notes",
                "label": "Investigation notes",
                "prefix": "investigation notes",
                "func": fake_notes,
                "aliases": [],
            },
        ]

        llm_payload = {
            "executive_summary": (
                "Ticket R-067298 is an open pricing dispute with material exposure and corroborating investigation evidence."
            ),
            "specialists": [
                {
                    "bullets": [
                        "Ticket **R-067298** centers on a **Price Discrepancy** with supporting **Sub Price Mismatch** evidence.",
                        "Exposure remains **$4,900.66** across **98** invoice lines and the ticket is still open.",
                    ]
                },
                {
                    "bullets": [
                        "Investigation notes confirm conflicting communicated prices for item **1005365**.",
                        "The note trail points to **$11.76/cs** as the supported price while the billed price remained higher.",
                    ]
                },
            ],
        }

        with patch.object(auto_mode, "INTENT_DEFS", fake_defs):
            with patch("actus.auto_mode.openrouter_chat", return_value=auto_mode.json.dumps(llm_payload)):
                text, rows, meta = auto_mode.auto_mode_answer(
                    "analyze ticket R-067298 with investigation notes and evidence",
                    pd.DataFrame(),
                )

        self.assertIsNone(rows)
        self.assertEqual("auto_mode", meta.get("intent_id"))
        self.assertIn("Ticket R-067298 is an open pricing dispute", text)
        self.assertIn("The note trail points to **$11.76/cs** as the supported price", text)
        self.assertNotIn("ask:Show%20investigation%20note", text)

    def test_llm_synthesis_falls_back_to_deterministic_renderer(self) -> None:
        def fake_ticket(_query: str, _df: pd.DataFrame):
            return (
                "Ticket analysis generated.",
                None,
                {
                    "ticket_analysis": {
                        "ticket_id": "R-067298",
                        "primary_root_cause": "price_discrepancy",
                        "supporting_root_causes": [],
                        "credit_total": 4900.66,
                        "line_count": 98,
                        "is_credited": False,
                    }
                },
            )

        fake_defs = [
            {
                "id": "ticket_analysis",
                "label": "Analyze ticket",
                "prefix": "analyze ticket",
                "func": fake_ticket,
                "aliases": [],
            },
        ]

        with patch.object(auto_mode, "INTENT_DEFS", fake_defs):
            with patch("actus.auto_mode.openrouter_chat", side_effect=RuntimeError("offline")):
                text, rows, meta = auto_mode.auto_mode_answer(
                    "analyze ticket R-067298",
                    pd.DataFrame(),
                )

        self.assertIsNone(rows)
        self.assertEqual("auto_mode", meta.get("intent_id"))
        self.assertIn("## Executive Summary", text)
        self.assertIn("Auto Mode reviewed `R-067298`", text)

    def test_investigation_note_preview_bullets_strip_action_tokens(self) -> None:
        bullets = auto_mode._investigation_note_bullets(
            {},
            "Here are the investigation notes for ticket **R-067298**.\n\n"
            "Available notes:\n"
            "- **INV15003283|1005365** — Investigation for INV15003283 · 1005365 "
            "(Updated: 2026-03-27T18:19:50Z) • Note ID: `-OokWXF5wGWNitjpYmaZ` "
            "• `ask:Show%20investigation%20note|Open note`",
        )

        self.assertTrue(bullets)
        self.assertIn("Investigation notes are available", bullets[0])
        self.assertFalse(any("ask:" in bullet for bullet in bullets))

    def test_overall_summary_bullets_extract_core_metrics(self) -> None:
        bullets = auto_mode._overall_summary_bullets(
            {
                "overall_summary": {
                    "window": "2026-03-01 → 2026-03-28",
                    "open_record_count": 1440,
                    "open_credit_total": 176802.90,
                    "avg_days_open": 18.4,
                    "avg_days_since_last_status": 6.2,
                    "billing_queue_delay_count": 24,
                    "billing_queue_delay_total": 8250.75,
                    "stale_investigation_count": 13,
                    "stale_investigation_total": 4111.50,
                    "credited_in_period": {
                        "credited_record_count": 320,
                        "credited_credit_total": 55677.11,
                        "credited_event_count": 355,
                        "credited_event_credit_total": 61220.11,
                        "system_record_count": 290,
                        "system_credit_total": 50220.11,
                        "manual_record_count": 30,
                        "manual_credit_total": 5457.00,
                        "records_with_both_sources": 35,
                        "avg_days_to_rtn_assignment": 14.6,
                    },
                    "top_customers": [{"label": "JHC11", "credit_total": 2217.50}],
                    "top_items": [{"label": "1005365", "credit_total": 1980.0}],
                    "top_root_causes": [{"root_cause": "Price discrepancy", "record_count": 41}],
                }
            },
            "📊 **Overall Credit Overview**\n\n"
            "- Window: **2026-03-01 → 2026-03-28**\n"
        )

        self.assertEqual(4, len(bullets))
        self.assertIn("2026-03-01 → 2026-03-28", bullets[0])
        self.assertIn("$176,802.90", bullets[0])
        self.assertIn("18.4", bullets[1])
        self.assertIn("billing queue delay", bullets[1])
        self.assertIn("$55,677.11", bullets[2])
        self.assertIn("14.6", bullets[2])
        self.assertIn("JHC11", bullets[3])
        self.assertIn("1005365", bullets[3])
        self.assertIn("Price discrepancy", bullets[3])

    def test_no_plan_falls_back_to_standard_router(self) -> None:
        with patch(
            "actus.auto_mode.actus_answer",
            return_value=("fallback answer", None, {"intent_id": "help"}),
        ) as mock_answer:
            text, rows, meta = auto_mode.auto_mode_answer("hello there", pd.DataFrame())

        self.assertEqual("fallback answer", text)
        self.assertIsNone(rows)
        self.assertEqual("help", meta.get("intent_id"))
        mock_answer.assert_called_once()


if __name__ == "__main__":
    unittest.main()
