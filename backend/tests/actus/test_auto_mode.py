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

    def test_overview_with_trends_keeps_overview_primary(self) -> None:
        plan = auto_mode.plan_auto_mode(
            "give me a credit overview for last 3 months then show trends"
        )

        self.assertIsNotNone(plan)
        assert plan is not None
        self.assertEqual("portfolio", plan.family)
        self.assertEqual("overall_summary", plan.primary_intent)
        self.assertEqual(
            ["overall_summary", "credit_trends"],
            [item.id for item in plan.intents],
        )


class TestAutoModeExecution(unittest.TestCase):
    def setUp(self) -> None:
        auto_mode._SPECIALIST_RESULT_CACHE.clear()

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

    def test_system_updates_reuses_cross_request_cache_for_same_window(self) -> None:
        calls = {"count": 0}

        def fake_system_updates(query: str, _df: pd.DataFrame):
            calls["count"] += 1
            return (
                f"system updates for {query}",
                None,
                {
                    "system_updates_summary": {
                        "window": "2026-01-01 → 2026-03-31",
                        "total_records": 10,
                        "credit_total": 1000.0,
                        "avg_days_to_system_credit": 12.3,
                        "median_days_to_system_credit": 10.5,
                        "outlier_count": 1,
                        "outlier_ticket_ids": ["R-000001"],
                        "batch_dates": 2,
                        "batched_dates": 1,
                        "batched_records": 9,
                        "batched_credit_total": 900.0,
                        "largest_batch_count": 9,
                        "largest_batch_date": "2026-02-01",
                        "largest_batch_credit_total": 900.0,
                        "manual_record_count": 0,
                        "manual_credit_total": 0.0,
                        "manual_avg_days_to_update": 0.0,
                        "manual_outlier_count": 0,
                        "manual_outlier_ticket_ids": [],
                    }
                },
            )

        fake_defs = [
            {
                "id": "system_updates",
                "label": "System updates with RTN",
                "prefix": "system updates",
                "func": fake_system_updates,
                "aliases": [],
            },
        ]
        df = pd.DataFrame()
        df.attrs["_actus_df_cache_token"] = "token-1"

        with patch.object(auto_mode, "INTENT_DEFS", fake_defs):
            with patch("actus.auto_mode.openrouter_chat", side_effect=AssertionError("LLM should not run")):
                first_text, _rows, first_meta = auto_mode.auto_mode_answer(
                    "show me system RTN updates analysis from 2026-01-01 to 2026-03-31",
                    df,
                )
                second_text, _rows, second_meta = auto_mode.auto_mode_answer(
                    "system rtn updates analysis from 2026-01-01 to 2026-03-31",
                    df,
                )

        self.assertEqual(1, calls["count"])
        self.assertEqual("auto_mode", first_meta.get("intent_id"))
        self.assertEqual("auto_mode", second_meta.get("intent_id"))
        self.assertIn("## Executive Summary", first_text)
        self.assertIn("## Executive Summary", second_text)

    def test_overall_summary_reuses_cross_request_cache_for_same_window(self) -> None:
        calls = {"count": 0}

        def fake_overall_summary(query: str, _df: pd.DataFrame):
            calls["count"] += 1
            return (
                f"overview for {query}",
                None,
                {
                    "overall_summary": {
                        "window": "2026-02-26 → 2026-03-29",
                        "open_record_count": 198,
                        "open_credit_total": 19677.83,
                        "avg_days_open": 8.7,
                        "avg_days_since_last_status": 7.7,
                        "billing_queue_delay_count": 34,
                        "billing_queue_delay_total": 4150.24,
                        "stale_investigation_count": 38,
                        "stale_investigation_total": 2572.62,
                        "credited_in_period": {
                            "credited_record_count": 175,
                            "credited_credit_total": 32309.15,
                            "primary_system_record_count": 175,
                            "primary_system_credit_total": 32309.15,
                            "primary_manual_record_count": 0,
                            "primary_manual_credit_total": 0.0,
                            "avg_days_to_rtn_assignment": 26.2,
                            "reopened_after_terminal_count": 2,
                        },
                    }
                },
            )

        fake_defs = [
            {
                "id": "overall_summary",
                "label": "Credit overview",
                "prefix": "credit overview",
                "func": fake_overall_summary,
                "aliases": [],
            },
        ]
        df = pd.DataFrame()
        df.attrs["_actus_df_cache_token"] = "token-1"

        with patch.object(auto_mode, "INTENT_DEFS", fake_defs):
            with patch("actus.auto_mode.openrouter_chat", side_effect=AssertionError("LLM should not run")):
                first_text, _rows, _meta = auto_mode.auto_mode_answer(
                    "give me a credit overview from 2026-02-26 to 2026-03-29",
                    df,
                )
                second_text, _rows, _meta = auto_mode.auto_mode_answer(
                    "show me a credit summary from 2026-02-26 to 2026-03-29",
                    df,
                )

        self.assertEqual(1, calls["count"])
        self.assertIn("### Section 1: Period Activity", first_text)
        self.assertIn("### Section 1: Period Activity", second_text)

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
                        "primary_system_record_count": 290,
                        "primary_system_credit_total": 50220.11,
                        "primary_manual_record_count": 30,
                        "primary_manual_credit_total": 5457.00,
                        "records_with_both_sources": 35,
                        "reopened_after_terminal_count": 8,
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
        self.assertIn("system-led 290 / $50,220.11", bullets[3])
        self.assertIn("manual-led 30 / $5,457.00", bullets[3])
        self.assertIn("reopened after terminal totals **8**", bullets[3].lower())

    def test_portfolio_credit_brief_renders_sectioned_summary(self) -> None:
        plan = auto_mode.AutoPlan(
            family="portfolio",
            primary_intent="overall_summary",
            target_label=None,
            intents=(
                auto_mode.PlannedIntent("system_updates", "System updates with RTN", "system rtn updates"),
                auto_mode.PlannedIntent("credit_root_causes", "Root causes", "root causes"),
                auto_mode.PlannedIntent("overall_summary", "Credit overview", "credit overview"),
            ),
            suggestions=(),
        )

        runs = {
            "system_updates": auto_mode.SpecialistRun(
                plan=plan.intents[0],
                text="system updates",
                rows=None,
                meta={
                    "system_updates_summary": {
                        "window": "2025-09-29 → 2026-03-29",
                        "total_records": 594,
                        "credit_total": 119033.58,
                        "avg_days_to_system_credit": 59.9,
                        "median_days_to_system_credit": 38.6,
                        "outlier_count": 3,
                        "outlier_ticket_ids": ["R-036446", "R-042604", "R-045605"],
                        "batch_dates": 9,
                        "batched_dates": 7,
                        "batched_records": 592,
                        "batched_credit_total": 118333.08,
                        "largest_batch_count": 179,
                        "largest_batch_date": "2026-02-02",
                        "largest_batch_credit_total": 18027.36,
                        "manual_record_count": 695,
                        "manual_credit_total": 121219.75,
                        "manual_avg_days_to_update": 123.8,
                        "manual_outlier_count": 10,
                        "manual_outlier_ticket_ids": ["R-038692", "R-039188", "R-041888", "R-038247", "R-036895"],
                    },
                    "suggestions": [
                        {
                            "id": "system_updates",
                            "label": "System RTN updates preview (2025-09-29 → 2026-03-29)",
                            "prefix": "system rtn updates analysis from 2025-09-29 to 2026-03-29",
                        }
                    ],
                },
            ),
            "credit_root_causes": auto_mode.SpecialistRun(
                plan=plan.intents[1],
                text="root causes",
                rows=None,
                meta={
                    "rootCauses": {
                        "period": "2025-09-29 → 2026-03-29",
                        "total": "$209,765.46",
                        "data": [
                            {"root_cause": "Item should be PPD", "credit_request_total": 79140.89, "record_count": 503},
                            {"root_cause": "Item not price matched when subbing", "credit_request_total": 57646.81, "record_count": 307},
                            {"root_cause": "Unspecified", "credit_request_total": 49917.77, "record_count": 165},
                        ],
                    }
                },
            ),
            "overall_summary": auto_mode.SpecialistRun(
                plan=plan.intents[2],
                text="overall summary",
                rows=None,
                meta={
                    "overall_summary": {
                        "window": "2025-09-29 → 2026-03-29",
                        "open_record_count": 594,
                        "open_credit_total": 70381.28,
                        "avg_days_open": 68.7,
                        "avg_days_since_last_status": 52.9,
                        "billing_queue_delay_count": 224,
                        "billing_queue_delay_total": 37725.55,
                        "stale_investigation_count": 38,
                        "stale_investigation_total": 2572.62,
                        "credited_in_period": {
                            "credited_record_count": 1101,
                            "credited_credit_total": 201046.75,
                            "primary_system_record_count": 602,
                            "primary_system_credit_total": 119301.25,
                            "primary_manual_record_count": 499,
                            "primary_manual_credit_total": 81745.50,
                            "avg_days_to_rtn_assignment": 94.9,
                            "reopened_after_terminal_count": 4,
                        },
                    },
                    "suggestions": [
                        {
                            "id": "credit_ops_snapshot",
                            "label": "Credit ops snapshot (2025-09-29 → 2026-03-29)",
                            "prefix": "credit ops snapshot from 2025-09-29 to 2026-03-29",
                        }
                    ],
                },
            ),
        }

        def fake_execute(planned_intent: auto_mode.PlannedIntent, _df: pd.DataFrame):
            return runs[planned_intent.id]

        with patch("actus.auto_mode.plan_auto_mode", return_value=plan):
            with patch("actus.auto_mode._execute_planned_intent", side_effect=fake_execute):
                with patch("actus.auto_mode.openrouter_chat", side_effect=AssertionError("LLM should not run")):
                    text, rows, meta = auto_mode.auto_mode_answer(
                        "give me a credit overview with RTN updates and root causes for the last 6 months",
                        pd.DataFrame(),
                    )

        self.assertIsNone(rows)
        self.assertEqual("auto_mode", meta.get("intent_id"))
        self.assertIn("## Executive Summary", text)
        self.assertIn("Period: September 29, 2025 – March 29, 2026", text)
        self.assertIn("### Section 1: Volume & Activity", text)
        self.assertIn("1,101", text)
        self.assertIn("$201,046.75", text)
        self.assertIn("### Section 2: Time-to-Resolution", text)
        self.assertIn("System outliers (3):\nR-036446, R-042604, R-045605", text)
        self.assertIn("Manual outliers (10):\nR-038692, R-039188, R-041888, R-038247, R-036895", text)
        self.assertIn("\n\nProcessing note:", text)
        self.assertIn("### Section 3: Open Exposure", text)
        self.assertIn("Reopened after terminal: **4** record(s).", text)
        self.assertIn("### Section 4: Root Causes", text)
        self.assertIn("Item should be PPD", text)
        self.assertNotIn("## Key Findings By Specialist", text)

    def test_overall_summary_auto_renders_four_section_brief(self) -> None:
        plan = auto_mode.AutoPlan(
            family="portfolio",
            primary_intent="overall_summary",
            target_label=None,
            intents=(
                auto_mode.PlannedIntent("overall_summary", "Credit overview", "credit overview"),
            ),
            suggestions=(),
        )

        run = auto_mode.SpecialistRun(
            plan=plan.intents[0],
            text="overall summary",
            rows=None,
            meta={
                "overall_summary": {
                    "window": "2026-02-26 → 2026-03-29",
                    "open_record_count": 198,
                    "open_credit_total": 19677.83,
                    "avg_days_open": 8.7,
                    "avg_days_since_last_status": 7.7,
                    "billing_queue_delay_count": 34,
                    "billing_queue_delay_total": 4150.24,
                    "stale_investigation_count": 38,
                    "stale_investigation_total": 2572.62,
                    "credited_in_period": {
                        "credited_record_count": 175,
                        "credited_credit_total": 32309.15,
                        "primary_system_record_count": 175,
                        "primary_system_credit_total": 32309.15,
                        "primary_manual_record_count": 0,
                        "primary_manual_credit_total": 0.0,
                        "avg_days_to_rtn_assignment": 26.2,
                        "reopened_after_terminal_count": 2,
                    },
                },
                "suggestions": [
                    {
                        "id": "credit_ops_snapshot",
                        "label": "Credit ops snapshot (2026-02-26 → 2026-03-29)",
                        "prefix": "credit ops snapshot from 2026-02-26 to 2026-03-29",
                    }
                ],
            },
        )

        def fake_execute(_planned_intent: auto_mode.PlannedIntent, _df: pd.DataFrame):
            return run

        with patch("actus.auto_mode.plan_auto_mode", return_value=plan):
            with patch("actus.auto_mode._execute_planned_intent", side_effect=fake_execute):
                with patch("actus.auto_mode.openrouter_chat", side_effect=AssertionError("LLM should not run")):
                    text, rows, meta = auto_mode.auto_mode_answer(
                        "give me a credit overview for the last month",
                        pd.DataFrame(),
                    )

        self.assertIsNone(rows)
        self.assertEqual("auto_mode", meta.get("intent_id"))
        self.assertIn("## Executive Summary", text)
        self.assertIn("### Section 1: Period Activity", text)
        self.assertIn("175** unique record(s) were credited totaling **$32,309.15**", text)
        self.assertIn("198** record(s) remained open totaling **$19,677.83**", text)
        self.assertIn("### Section 2: Time-to-Resolution", text)
        self.assertIn("Average open age was **8.7** day(s), with **7.7** day(s) since the last update. Average time to RTN assignment was **26.2** day(s).", text)
        self.assertIn("### Section 3: Open Exposure", text)
        self.assertIn("Billing queue delay affects **34** record(s) totaling **$4,150.24**", text)
        self.assertIn("stale investigation affects **38** record(s) totaling **$2,572.62**", text)
        self.assertIn("Reopened after terminal totals **2** record(s).", text)
        self.assertIn("### Section 4: Attribution", text)
        self.assertIn("All credited activity in the period was system-led: **175** record(s) / **$32,309.15**; manual-led activity was **0** record(s) / **$0.00**.", text)
        self.assertNotIn("## Key Findings By Specialist", text)

    def test_single_portfolio_prompt_uses_deterministic_renderer(self) -> None:
        plan = auto_mode.AutoPlan(
            family="portfolio",
            primary_intent="credit_root_causes",
            target_label=None,
            intents=(
                auto_mode.PlannedIntent("credit_root_causes", "Root causes", "root causes"),
            ),
            suggestions=(),
        )

        run = auto_mode.SpecialistRun(
            plan=plan.intents[0],
            text="root causes",
            rows=None,
            meta={
                "rootCauses": {
                    "period": "2026-01-01 → 2026-03-31",
                    "total": "$37,973.15",
                    "data": [
                        {"root_cause": "Item should be PPD", "credit_request_total": 23278.10, "record_count": 245},
                        {"root_cause": "Item not price matched when subbing", "credit_request_total": 10561.02, "record_count": 97},
                    ],
                }
            },
        )

        def fake_execute(_planned_intent: auto_mode.PlannedIntent, _df: pd.DataFrame):
            return run

        with patch("actus.auto_mode.plan_auto_mode", return_value=plan):
            with patch("actus.auto_mode._execute_planned_intent", side_effect=fake_execute):
                with patch("actus.auto_mode.openrouter_chat", side_effect=AssertionError("LLM should not run")):
                    text, rows, meta = auto_mode.auto_mode_answer(
                        "what are the main root causes driving open exposure this quarter",
                        pd.DataFrame(),
                    )

        self.assertIsNone(rows)
        self.assertEqual("auto_mode", meta.get("intent_id"))
        self.assertIn("## Executive Summary", text)
        self.assertIn("Auto Mode ran **1/1** portfolio specialist(s)", text)
        self.assertIn("### Root causes", text)
        self.assertIn("Item should be PPD", text)
        self.assertIn("## Recommended Follow-Ups", text)

    def test_overview_with_trends_renders_overview_led_brief(self) -> None:
        plan = auto_mode.AutoPlan(
            family="portfolio",
            primary_intent="overall_summary",
            target_label=None,
            intents=(
                auto_mode.PlannedIntent("overall_summary", "Credit overview", "credit overview"),
                auto_mode.PlannedIntent("credit_trends", "Credit trends", "credit trends"),
            ),
            suggestions=(),
        )

        runs = {
            "overall_summary": auto_mode.SpecialistRun(
                plan=plan.intents[0],
                text="overall summary",
                rows=None,
                meta={
                    "overall_summary": {
                        "window": "2025-12-30 → 2026-03-30",
                        "open_record_count": 456,
                        "open_credit_total": 37973.15,
                        "avg_days_open": 43.8,
                        "avg_days_since_last_status": 42.2,
                        "billing_queue_delay_count": 135,
                        "billing_queue_delay_total": 5857.61,
                        "stale_investigation_count": 11,
                        "stale_investigation_total": 1505.64,
                        "credited_in_period": {
                            "credited_record_count": 629,
                            "credited_credit_total": 126604.55,
                            "primary_system_record_count": 584,
                            "primary_system_credit_total": 118426.96,
                            "primary_manual_record_count": 45,
                            "primary_manual_credit_total": 8177.59,
                            "avg_days_to_rtn_assignment": 72.9,
                            "reopened_after_terminal_count": 4,
                        },
                    },
                    "suggestions": [],
                },
            ),
            "credit_trends": auto_mode.SpecialistRun(
                plan=plan.intents[1],
                text="credit trends",
                rows=None,
                meta={
                    "creditTrends": {
                        "metrics": [
                            {"label": "Volume (Rows)", "current": 702, "previous": 395, "change": 77.7, "isCurrency": False},
                            {"label": "Total Credits", "current": 96197.84, "previous": 112582.18, "change": -14.6, "isCurrency": True},
                            {"label": "Avg Credit", "current": 137.03, "previous": 285.02, "change": -51.9, "isCurrency": True},
                        ],
                        "window": {
                            "previous": "2025-09-30 → 2025-12-29",
                            "current": "2025-12-30 → 2026-03-30",
                        },
                    },
                    "suggestions": [],
                },
            ),
        }

        def fake_execute(planned_intent: auto_mode.PlannedIntent, _df: pd.DataFrame):
            return runs[planned_intent.id]

        with patch("actus.auto_mode.plan_auto_mode", return_value=plan):
            with patch("actus.auto_mode._execute_planned_intent", side_effect=fake_execute):
                with patch("actus.auto_mode.openrouter_chat", side_effect=AssertionError("LLM should not run")):
                    text, rows, meta = auto_mode.auto_mode_answer(
                        "give me a credit overview for last 3 months then show trends",
                        pd.DataFrame(),
                    )

        self.assertIsNone(rows)
        self.assertEqual("auto_mode", meta.get("intent_id"))
        self.assertEqual(
            ["overall_summary", "credit_trends"],
            [item["id"] for item in meta["auto_mode"]["executed_intents"]],
        )
        self.assertIn("### Section 1: Period Activity", text)
        self.assertIn("### Section 5: Trends", text)
        self.assertIn("**Volume (Rows)**: 702 vs 395 (+77.7%).", text)
        self.assertIn("Comparison window: **2025-09-30 → 2025-12-29** against **2025-12-30 → 2026-03-30**.", text)
        self.assertNotIn("## Key Findings By Specialist", text)

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
