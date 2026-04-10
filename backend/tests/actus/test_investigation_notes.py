import unittest
from unittest.mock import patch

import pandas as pd

from actus.intents import investigation_notes


def _notes_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "Note ID": "note-1",
                "Firebase Key": "firebase-1",
                "Ticket Number": "R-067298",
                "Combo Key": "INV15003283|1005365",
                "Invoice Number": "INV15003283",
                "Item Number": "1005365",
                "Title": "Investigation for INV15003283 · 1005365",
                "Body": (
                    "Background:\n\n"
                    "- Case Number: R-067298\n"
                    "\nMiscellaneous\n\n"
                    "- Price should have been matched to wipes that were replaced\n"
                    "- Saul later confirmed the correct price should be $11.76/cs\n"
                    "- Customer was billed at $14.16/cs\n"
                    "- Jeff approval is needed before moving forward\n"
                ),
                "Created At": "2026-03-27T17:00:00Z",
                "Created By": "saul",
                "Updated At": "2026-03-27T18:19:50Z",
                "Updated By": "saul",
            },
            {
                "Note ID": "note-2",
                "Firebase Key": "firebase-2",
                "Ticket Number": "R-067298",
                "Combo Key": "INV15083281|1005365",
                "Invoice Number": "INV15083281",
                "Item Number": "1005365",
                "Title": "Investigation for INV15083281 · 1005365",
                "Body": (
                    "Background:\n\n"
                    "- Case Number: R-067298\n"
                    "\nMiscellaneous\n\n"
                    "- Mark originally requested $11.80/cs\n"
                    "- Saul later said do not keep $14.16/cs and confirmed $11.76/cs instead\n"
                    "- No substitutions were found in the system\n"
                ),
                "Created At": "2026-03-27T17:05:00Z",
                "Created By": "mark",
                "Updated At": "2026-03-27T18:19:50Z",
                "Updated By": "mark",
            },
            {
                "Note ID": "note-3",
                "Firebase Key": "firebase-3",
                "Ticket Number": "R-067298",
                "Combo Key": "INV15099999|1005365",
                "Invoice Number": "INV15099999",
                "Item Number": "1005365",
                "Title": "Investigation for INV15099999 · 1005365",
                "Body": (
                    "Background:\n\n"
                    "- Case Number: R-067298\n"
                    "\nMiscellaneous\n\n"
                    "- Saul later said do not keep $14.16/cs and confirmed $11.76/cs instead\n"
                    "- No substitutions were found in the system\n"
                ),
                "Created At": "2026-03-27T17:10:00Z",
                "Created By": "mark",
                "Updated At": "2026-03-27T18:20:00Z",
                "Updated By": "mark",
            },
        ]
    )


class TestInvestigationNotesIntent(unittest.TestCase):
    def test_ticket_query_returns_ticket_level_summary(self) -> None:
        llm_response = "\n".join(
            [
                "- Likely issue: correct supported price is $11.76/cs while invoices were billed at $14.16/cs.",
                "- Why this is likely: earlier pricing guidance conflicted across $11.80, $14.16, and $11.76, pointing to communication error in the notes.",
            ]
        )

        with patch(
            "actus.intents.investigation_notes._load_investigation_notes",
            return_value=_notes_frame(),
        ):
            with patch("actus.intents.investigation_notes.openrouter_chat", return_value=llm_response):
                text, rows, meta = investigation_notes.intent_investigation_notes(
                    "investigation notes for ticket R-067298",
                    pd.DataFrame(),
                )

        self.assertIsNone(rows)
        self.assertNotIn("Here are the investigation notes for ticket", text)
        self.assertNotIn("### Key takeaways", text)
        self.assertNotIn("Likely issue:", text)
        self.assertIn("Reviewed **2** unique note body/bodies across **3** ticket note(s).", text)
        self.assertIn("Relevant notes reviewed:", text)
        self.assertEqual(
            [
                "Likely issue: correct supported price is $11.76/cs while invoices were billed at $14.16/cs.",
                "Why this is likely: earlier pricing guidance conflicted across $11.80, $14.16, and $11.76, pointing to communication error in the notes.",
            ],
            meta["note_summary"]["bullets"],
        )
        self.assertTrue(meta["note_summary"]["ticket_level"])

    def test_ticket_query_falls_back_to_existing_note_summary(self) -> None:
        with patch(
            "actus.intents.investigation_notes._load_investigation_notes",
            return_value=_notes_frame().iloc[[0]].copy(),
        ):
            with patch(
                "actus.intents.investigation_notes._summarize_ticket_note_samples",
                return_value=(None, {"source": None, "model": None}),
            ):
                with patch(
                    "actus.intents.investigation_notes._summarize_note_body",
                    return_value=(
                        [
                            "Fallback says the correct price is $11.76/cs.",
                            "Fallback highlights the billed price at $14.16/cs.",
                        ],
                        {"source": "openrouter_primary", "model": "openai/gpt-4o-mini"},
                    ),
                ):
                    text, rows, meta = investigation_notes.intent_investigation_notes(
                        "investigation notes for ticket R-067298",
                        pd.DataFrame(),
                    )

        self.assertIsNone(rows)
        self.assertNotIn("Here are the investigation notes for ticket", text)
        self.assertNotIn("Likely issue: Fallback says the correct price is $11.76/cs.", text)
        self.assertEqual(
            [
                "Likely issue: Fallback says the correct price is $11.76/cs.",
                "Why this is likely: Fallback highlights the billed price at $14.16/cs.",
            ],
            meta["note_summary"]["bullets"],
        )

    def test_ticket_query_synthesizes_from_item_and_background_lines(self) -> None:
        notes = pd.DataFrame(
            [
                {
                    "Note ID": "note-1",
                    "Firebase Key": "firebase-1",
                    "Ticket Number": "R-053236",
                    "Combo Key": "INV13925842|010-005-40",
                    "Invoice Number": "INV13925842",
                    "Item Number": "010-005-40",
                    "Title": "Case file INV13925842 010-005-40",
                    "Body": (
                        "Background\n\n"
                        "Item Number: 010-005-40\n\n"
                        "Notes on Background: 010-102576 WAS SUBBED AND PRICING NOT MATCHED\n"
                    ),
                    "Created At": "2025-12-24T13:59:44Z",
                    "Created By": "ops",
                    "Updated At": "2025-12-24T13:59:44Z",
                    "Updated By": "ops",
                }
            ]
        )

        with patch(
            "actus.intents.investigation_notes._load_investigation_notes",
            return_value=notes,
        ):
            with patch(
                "actus.intents.investigation_notes._summarize_ticket_note_samples",
                return_value=(None, {"source": None, "model": None}),
            ):
                with patch(
                    "actus.intents.investigation_notes._summarize_note_body",
                    return_value=(None, {"source": None, "model": None}),
                ):
                    text, rows, meta = investigation_notes.intent_investigation_notes(
                        "investigation notes for ticket R-053236",
                        pd.DataFrame(),
                    )

        self.assertIsNone(rows)
        self.assertNotIn("Here are the investigation notes for ticket", text)
        self.assertNotIn("Likely issue: Item 010-005-40 appears tied to substituted item 010-102576, and the pricing did not match.", text)
        self.assertEqual(
            [
                "Likely issue: Item 010-005-40 appears tied to substituted item 010-102576, and the pricing did not match.",
                "Why this is likely: The reviewed note explicitly states that 010-102576 was substituted and pricing did not match.",
            ],
            meta["note_summary"]["bullets"],
        )


if __name__ == "__main__":
    unittest.main()
