import unittest

import numpy as np

from app.rag.new_design.index_build import build_pipeline_artifacts
from app.rag.new_design.answer import answer_from_results
from app.rag.new_design.models import RetrievalChunk, SearchResult
from app.rag.new_design.retrieve import routed_hybrid_search_real, search
from app.rag.new_design.root_cause import load_root_cause_rules


class TestE2ERetrieval(unittest.TestCase):
    def test_retrieval_pipeline_sanity(self) -> None:
        credit_rows = [
            {
                "Ticket Number": "R-100001",
                "Invoice Number": "INV123",
                "Item Number": "100-ABC",
                "Reason for Credit": "wrong price loaded",
                "Credit Request Total": "12.34",
                "Customer Number": "CHL17",
                "Sales Rep": "REP1",
            }
        ]
        inv_rows = [
            {
                "ticket_number": "R-100001",
                "invoice_number": "INV123",
                "item_number": "100-ABC",
                "note_id": "N1",
                "title": "Price Trace",
                "body": "Order Details\nwrong price loaded\nPrice Trace\nppd mismatch",
            }
        ]

        rules = load_root_cause_rules()
        artifacts = build_pipeline_artifacts(credit_rows, inv_rows, rules=rules)
        self.assertGreaterEqual(len(artifacts.chunks), 2)

        # Deterministic synthetic embeddings: make first chunk most similar.
        embeddings = np.zeros((len(artifacts.chunks), 3), dtype=np.float32)
        embeddings[0] = np.array([1.0, 0.0, 0.0], dtype=np.float32)
        if len(artifacts.chunks) > 1:
            embeddings[1] = np.array([0.8, 0.1, 0.0], dtype=np.float32)

        result = routed_hybrid_search_real(
            query="show me ticket R-100001",
            chunks=artifacts.chunks,
            embeddings=embeddings,
            query_embedding=np.array([1.0, 0.0, 0.0], dtype=np.float32),
            top_k=3,
        )

        self.assertFalse(result["not_found"])
        self.assertGreaterEqual(len(result["results"]), 1)
        self.assertEqual("R-100001", result["results"][0].ticket_id)

    def test_phrase_match_boosts_investigation_chunks(self) -> None:
        credit_rows = [
            {
                "Ticket Number": "R-200001",
                "Invoice Number": "INV200",
                "Item Number": "100-XYZ",
                "Reason for Credit": "pricing discrepancy",
                "Credit Request Total": "10.00",
                "Customer Number": "C1",
                "Sales Rep": "S1",
            }
        ]
        inv_rows = [
            {
                "ticket_number": "R-200001",
                "invoice_number": "INV200",
                "item_number": "100-XYZ",
                "note_id": "N2",
                "title": "Price Trace",
                "body": "Order Details\nItem was loaded incorrectly in the system.\nPrice Trace\nwrong price loaded.",
            }
        ]

        artifacts = build_pipeline_artifacts(credit_rows, inv_rows, rules=load_root_cause_rules())
        embeddings = np.zeros((len(artifacts.chunks), 3), dtype=np.float32)
        for i, chunk in enumerate(artifacts.chunks):
            if chunk.chunk_type == "ticket_summary":
                embeddings[i] = np.array([0.95, 0.0, 0.0], dtype=np.float32)
            elif chunk.chunk_type == "ticket_investigation_section":
                embeddings[i] = np.array([0.90, 0.0, 0.0], dtype=np.float32)
            else:
                embeddings[i] = np.array([0.89, 0.0, 0.0], dtype=np.float32)

        result = routed_hybrid_search_real(
            query="show tickets where item loaded incorrectly",
            chunks=artifacts.chunks,
            embeddings=embeddings,
            query_embedding=np.array([1.0, 0.0, 0.0], dtype=np.float32),
            top_k=5,
        )

        self.assertFalse(result["not_found"])
        self.assertGreaterEqual(len(result["results"]), 1)
        top = result["results"][0]
        self.assertEqual("ticket_investigation_section", top.chunk_type)
        self.assertIn("loaded incorrectly", top.text.lower())

    def test_evidence_fallback_never_outputs_none(self) -> None:
        results = [
            SearchResult(
                score=1.0,
                semantic_score=1.0,
                exact_boost=0.0,
                type_boost=0.0,
                root_cause_boost=0.0,
                chunk_type="ticket_line_summary",
                ticket_id="R-300001",
                text="Ticket R-300001 line INV|ITEM. Reason for credit: None.",
                metadata={"ticket_id": "R-300001"},
                intent="semantic_lookup",
                chunk_id=999,
            )
        ]

        answer = answer_from_results("test query", results, max_tickets_in_answer=1)
        self.assertNotIn("Evidence: None", answer)
        self.assertIn("No direct investigation text found.", answer)

    def test_aging_answer_uses_aging_format_not_generic_evidence(self) -> None:
        artifacts = type(
            "A",
            (),
            {
                "canonical_tickets": {
                    "R-900001": {
                        "entered_to_credited_days": 52.0,
                        "investigation_to_credited_days": 41.0,
                        "root_cause_ids": ["price_discrepancy"],
                        "item_numbers": ["1007986"],
                        "invoice_numbers": ["INV900001"],
                    }
                },
                "chunks": [
                    RetrievalChunk(
                        chunk_id=1,
                        ticket_id="R-900001",
                        chunk_type="ticket_summary",
                        text="Ticket R-900001 summary",
                        metadata={"ticket_id": "R-900001"},
                    )
                ],
            },
        )()
        embeddings = np.zeros((1, 3), dtype=np.float32)

        payload = search(
            "which tickets took more than 30 days to resolve",
            artifacts,
            embeddings,
            top_k=10,
        )
        self.assertEqual("aging_lookup", payload["intent"])

        answer = answer_from_results(
            "which tickets took more than 30 days to resolve",
            payload["results"],
            max_tickets_in_answer=5,
        )

        self.assertIn("Tickets exceeding the 30-day threshold:", answer)
        self.assertIn("days from entry to credit", answer)
        self.assertNotIn("Evidence:", answer)


if __name__ == "__main__":
    unittest.main()
