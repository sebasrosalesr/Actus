from __future__ import annotations

import unittest

import numpy as np

from app.rag.new_design.models import RetrievalChunk
from app.rag.new_design.service import ActusHybridRAGService


class _FakeStore:
    def search(self, query_embedding, top_k=10):
        _ = query_embedding
        _ = top_k
        return [(1, 0.35), (2, 0.1)]

    def fetch_chunks(self, chunk_ids):
        _ = chunk_ids
        return []


class FastServiceTests(unittest.TestCase):
    def test_search_uses_catalog_without_firebase_refresh(self) -> None:
        service = ActusHybridRAGService(
            embed_fn=lambda texts: np.ones((len(texts), 4), dtype=np.float32)
        )
        chunks = [
            RetrievalChunk(
                chunk_id=1,
                ticket_id="R-100001",
                chunk_type="ticket_summary",
                text="Ticket R-100001 invoice INV100 item 100500",
                metadata={
                    "ticket_id": "R-100001",
                    "invoice_numbers": ["INV100"],
                    "item_numbers": ["100500"],
                    "root_cause_ids": ["price_discrepancy"],
                    "root_cause_primary_id": "price_discrepancy",
                },
            ),
            RetrievalChunk(
                chunk_id=2,
                ticket_id="R-100002",
                chunk_type="ticket_event",
                text="Ticket R-100002 invoice INV200 item 100600",
                metadata={
                    "ticket_id": "R-100002",
                    "invoice_numbers": ["INV200"],
                    "item_numbers": ["100600"],
                },
            ),
        ]
        service._catalog_chunks = chunks
        service._chunk_by_id = {chunk.chunk_id: chunk for chunk in chunks}
        service._store = _FakeStore()

        payload = service.search("show me ticket R-100001", top_k=1)

        self.assertEqual(payload["intent"], "ticket_lookup")
        self.assertFalse(payload["not_found"])
        self.assertEqual(len(payload["results"]), 1)
        self.assertEqual(payload["results"][0]["ticket_id"], "R-100001")


if __name__ == "__main__":
    unittest.main()
