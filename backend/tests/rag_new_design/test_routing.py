import unittest
from types import SimpleNamespace

import numpy as np

from app.rag.new_design.models import RetrievalChunk
from app.rag.new_design.retrieve import analyze_query, compute_root_cause_boost, route_candidate_chunks, search


class TestRouting(unittest.TestCase):
    def test_exact_lookup_not_found(self) -> None:
        chunks = [
            RetrievalChunk(
                chunk_id=1,
                ticket_id="R-000001",
                chunk_type="ticket_summary",
                text="Ticket R-000001",
                metadata={"ticket_id": "R-000001"},
            )
        ]
        query_info = analyze_query("show me ticket R-999999")
        filtered, exact_not_found = route_candidate_chunks(query_info, chunks)
        self.assertTrue(exact_not_found)
        self.assertEqual([], filtered)

    def test_aging_query_routes_to_deterministic_filter(self) -> None:
        artifacts = SimpleNamespace(
            canonical_tickets={
                "R-1": {"entered_to_credited_days": 45.0, "investigation_to_credited_days": 10.0},
                "R-2": {"entered_to_credited_days": 31.0, "investigation_to_credited_days": 5.0},
                "R-3": {"entered_to_credited_days": 12.0, "investigation_to_credited_days": 9.0},
            },
            chunks=[
                RetrievalChunk(chunk_id=1, ticket_id="R-1", chunk_type="ticket_summary", text="R-1", metadata={"ticket_id": "R-1"}),
                RetrievalChunk(chunk_id=2, ticket_id="R-2", chunk_type="ticket_summary", text="R-2", metadata={"ticket_id": "R-2"}),
                RetrievalChunk(chunk_id=3, ticket_id="R-3", chunk_type="ticket_summary", text="R-3", metadata={"ticket_id": "R-3"}),
            ],
        )
        embeddings = np.zeros((3, 3), dtype=np.float32)

        payload = search("which tickets took more than 30 days to resolve", artifacts, embeddings, top_k=10)
        results = payload["results"]

        self.assertEqual("aging_lookup", payload["intent"])
        self.assertEqual(["R-1", "R-2"], [r.ticket_id for r in results])
        for result in results:
            self.assertGreater(float(result.metadata.get("aging_days", 0.0)), 30.0)

    def test_price_loaded_after_invoice_root_cause_boost(self) -> None:
        query = "Investigate price loaded after invoice where order placed before price loaded"
        metadata = {
            "root_cause_ids": ["price_loaded_after_invoice"],
            "root_cause_primary_id": "price_loaded_after_invoice",
        }
        boost = compute_root_cause_boost(query, metadata)
        self.assertGreaterEqual(boost, 0.35)

    def test_price_loaded_after_invoice_secondary_root_cause_gets_lighter_boost(self) -> None:
        query = "Investigate price loaded after invoice where order placed before price loaded"
        metadata = {
            "root_cause_ids": ["sub_price_mismatch", "price_loaded_after_invoice"],
            "root_cause_primary_id": "sub_price_mismatch",
        }
        boost = compute_root_cause_boost(query, metadata)
        self.assertAlmostEqual(0.10, boost, places=5)

    def test_semantic_root_cause_query_routes_to_matching_root_chunks(self) -> None:
        chunks = [
            RetrievalChunk(
                chunk_id=1,
                ticket_id="R-100001",
                chunk_type="ticket_line_summary",
                text="sub mismatch ticket",
                metadata={
                    "ticket_id": "R-100001",
                    "root_cause_ids": ["sub_price_mismatch"],
                    "root_cause_primary_id": "sub_price_mismatch",
                },
            ),
            RetrievalChunk(
                chunk_id=2,
                ticket_id="R-100002",
                chunk_type="ticket_line_summary",
                text="timing mismatch ticket",
                metadata={
                    "ticket_id": "R-100002",
                    "root_cause_ids": ["price_loaded_after_invoice"],
                    "root_cause_primary_id": "price_loaded_after_invoice",
                },
            ),
        ]
        query_info = analyze_query("show tickets where price was loaded after invoice")
        filtered, exact_not_found = route_candidate_chunks(query_info, chunks)
        self.assertFalse(exact_not_found)
        self.assertEqual(["R-100002"], [chunk.ticket_id for _, chunk in filtered])

    def test_semantic_root_cause_query_falls_back_when_no_matching_root(self) -> None:
        chunks = [
            RetrievalChunk(
                chunk_id=1,
                ticket_id="R-200001",
                chunk_type="ticket_line_summary",
                text="sub mismatch ticket",
                metadata={"ticket_id": "R-200001", "root_cause_ids": ["sub_price_mismatch"]},
            ),
            RetrievalChunk(
                chunk_id=2,
                ticket_id="R-200002",
                chunk_type="ticket_line_summary",
                text="ppd ticket",
                metadata={"ticket_id": "R-200002", "root_cause_ids": ["ppd_mismatch"]},
            ),
        ]
        query_info = analyze_query("show freight handling issues")
        filtered, exact_not_found = route_candidate_chunks(query_info, chunks)
        self.assertFalse(exact_not_found)
        self.assertEqual(2, len(filtered))


if __name__ == "__main__":
    unittest.main()
