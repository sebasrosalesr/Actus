import unittest

from app.rag.new_design.chunking import (
    build_investigation_section_chunks,
    clean_investigation_html,
    split_fallback_body,
)
from app.rag.new_design.models import InvestigationNote


class TestChunking(unittest.TestCase):
    def test_clean_html_and_sections(self) -> None:
        note = InvestigationNote(
            ticket_id="R-1",
            invoice_number="INV1",
            item_number="100",
            combo_key="INV1|100",
            note_id="n1",
            title="t",
            body_raw="<p>Order Details</p><p>Bad price.</p><p>Price Trace</p><p>PPD mismatch.</p>",
            body_clean=clean_investigation_html("<p>Order Details</p><p>Bad price.</p><p>Price Trace</p><p>PPD mismatch.</p>"),
            customer_number=None,
            created_at=None,
            created_by=None,
            updated_at=None,
            updated_by=None,
        )

        chunks = build_investigation_section_chunks(note)
        self.assertEqual(2, len(chunks))
        self.assertEqual("Order Details", chunks[0].section_name)
        self.assertIn("Bad price", chunks[0].chunk_text)
        self.assertEqual("Price Trace", chunks[1].section_name)

    def test_fallback_split(self) -> None:
        text = "Sentence one. Sentence two. Sentence three."
        parts = split_fallback_body(text, max_chars=20)
        self.assertGreaterEqual(len(parts), 2)


if __name__ == "__main__":
    unittest.main()
