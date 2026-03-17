import unittest

import numpy as np

from app.rag.new_design.service import ActusHybridRAGService


def _fake_embed(texts: list[str]) -> np.ndarray:
    vecs = []
    for text in texts:
        value = str(text).lower()
        if "r-200001" in value:
            vecs.append([1.0, 0.0, 0.0])
        elif "wrong price" in value:
            vecs.append([0.9, 0.1, 0.0])
        else:
            vecs.append([0.1, 0.9, 0.0])
    return np.asarray(vecs, dtype=np.float32)


def _fake_embed_for_dedupe(texts: list[str]) -> np.ndarray:
    vecs = []
    for text in texts:
        value = str(text).lower()
        if "pricing issue" in value:
            vecs.append([1.0, 0.0, 0.0])
        elif "r-300001" in value:
            vecs.append([1.0, 0.0, 0.0])
        elif "r-300002" in value:
            vecs.append([0.9, 0.0, 0.0])
        else:
            vecs.append([0.0, 1.0, 0.0])
    return np.asarray(vecs, dtype=np.float32)


class TestService(unittest.TestCase):
    def test_service_search_and_answer(self) -> None:
        credit_rows = [
            {
                "Ticket Number": "R-200001",
                "Invoice Number": "INV999",
                "Item Number": "100-XYZ",
                "Reason for Credit": "wrong price loaded",
                "Credit Request Total": "20.00",
                "Customer Number": "ABC10",
                "Sales Rep": "REP2",
            }
        ]
        inv_rows = [
            {
                "ticket_number": "R-200001",
                "invoice_number": "INV999",
                "item_number": "100-XYZ",
                "note_id": "NOTE1",
                "title": "Price Trace",
                "body": "Order Details\nwrong price\nPrice Trace\nsubbed and not price matched",
            }
        ]

        service = ActusHybridRAGService(embed_fn=_fake_embed)
        load_info = service.load_from_rows(credit_rows, inv_rows)
        self.assertEqual(1, load_info["ticket_count"])
        self.assertGreater(load_info["chunk_count"], 0)

        search_payload = service.search("show me ticket R-200001", top_k=3)
        self.assertFalse(search_payload["not_found"])
        self.assertGreaterEqual(len(search_payload["results"]), 1)
        self.assertEqual("R-200001", search_payload["results"][0]["ticket_id"])

        answer_payload = service.answer("show me ticket R-200001", top_k=3)
        self.assertIn("Top results for", answer_payload["answer"])

    def test_search_dedupes_results_by_ticket(self) -> None:
        credit_rows = [
            {
                "Ticket Number": "R-300001",
                "Invoice Number": "INV300A",
                "Item Number": "300-AAA",
                "Reason for Credit": "pricing issue wrong price loaded",
                "Credit Request Total": "11.00",
                "Customer Number": "C300",
                "Sales Rep": "REP300",
            },
            {
                "Ticket Number": "R-300002",
                "Invoice Number": "INV300B",
                "Item Number": "300-BBB",
                "Reason for Credit": "pricing issue wrong price billed",
                "Credit Request Total": "12.00",
                "Customer Number": "C301",
                "Sales Rep": "REP301",
            },
        ]
        inv_rows = [
            {
                "ticket_number": "R-300001",
                "invoice_number": "INV300A",
                "item_number": "300-AAA",
                "note_id": "N300A",
                "title": "Price Trace",
                "body": "Order Details\npricing issue\nPrice Trace\nwrong price loaded",
            },
            {
                "ticket_number": "R-300002",
                "invoice_number": "INV300B",
                "item_number": "300-BBB",
                "note_id": "N300B",
                "title": "Price Trace",
                "body": "Order Details\npricing issue\nPrice Trace\nwrong price billed",
            },
        ]

        service = ActusHybridRAGService(embed_fn=_fake_embed_for_dedupe)
        service.load_from_rows(credit_rows, inv_rows)
        payload = service.search("pricing issue", top_k=5)
        ticket_ids = [row["ticket_id"] for row in payload["results"]]

        self.assertEqual(len(ticket_ids), len(set(ticket_ids)))
        self.assertIn("R-300001", ticket_ids)
        self.assertIn("R-300002", ticket_ids)

    def test_service_item_analysis(self) -> None:
        credit_rows = [
            {
                "Ticket Number": "R-400001",
                "Invoice Number": "INV400A",
                "Item Number": "1007986",
                "Reason for Credit": "wrong price loaded",
                "Credit Request Total": "8.00",
                "Customer Number": "C400",
                "Sales Rep": "REP400",
            }
        ]
        inv_rows = [
            {
                "ticket_number": "R-400001",
                "invoice_number": "INV400A",
                "item_number": "1007986",
                "note_id": "N400A",
                "title": "Price Trace",
                "body": "wrong price loaded",
            }
        ]

        service = ActusHybridRAGService(embed_fn=_fake_embed)
        service.load_from_rows(credit_rows, inv_rows)
        out = service.analyze_item("1007986")

        self.assertEqual("1007986", out["item_number"])
        self.assertEqual(1, out["ticket_count"])
        self.assertEqual(1, out["invoice_count"])
        self.assertEqual(1, out["line_count"])
        self.assertIn("answer", out)

    def test_service_ticket_analysis(self) -> None:
        credit_rows = [
            {
                "Ticket Number": "R-500001",
                "Invoice Number": "INV500A",
                "Item Number": "500-XYZ",
                "Reason for Credit": "wrong price loaded",
                "Credit Request Total": "18.00",
                "Customer Number": "C500",
                "Sales Rep": "REP500",
                "Status": (
                    "2026-02-01 08:00:00\nOpen: Not Started\n"
                    "2026-02-02 09:00:00\nWent through investigation\n"
                    "2026-02-05 09:00:00\nUpdated by the system"
                ),
            }
        ]
        inv_rows = [
            {
                "ticket_number": "R-500001",
                "invoice_number": "INV500A",
                "item_number": "500-XYZ",
                "note_id": "N500A",
                "title": "Investigation",
                "body": "Ticket reviewed and credit processed.",
            }
        ]

        service = ActusHybridRAGService(embed_fn=_fake_embed)
        service.load_from_rows(credit_rows, inv_rows)
        out = service.analyze_ticket("R-500001")

        self.assertEqual("R-500001", out["ticket_id"])
        self.assertEqual(18.0, out["credit_total"])
        self.assertEqual(1, out["line_count"])
        self.assertTrue(out["is_credited"])
        self.assertIn("timeline_metrics", out)
        self.assertIn("answer", out)


if __name__ == "__main__":
    unittest.main()
