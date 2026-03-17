from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
from threading import RLock
from typing import Any, Callable

import numpy as np

from .answer import answer_from_results
from .analytics import analyze_item_actus, analyze_ticket_actus
from .index_build import build_pipeline_artifacts, index_pipeline_artifacts
from .ingest import load_credit_requests, load_env, load_investigation_notes
from .models import PipelineArtifacts, SearchResult
from .retrieve import routed_hybrid_search_real


@dataclass(slots=True)
class ServiceConfig:
    rules_path: str | Path | None = None
    fallback_max_chars: int = 700


class ActusHybridRAGService:
    """In-memory runtime service for the new hybrid RAG pipeline."""

    def __init__(
        self,
        config: ServiceConfig | None = None,
        *,
        embed_fn: Callable[[list[str]], np.ndarray] | None = None,
    ) -> None:
        self.config = config or ServiceConfig()
        self._embed_fn = embed_fn
        self._lock = RLock()

        self._artifacts: PipelineArtifacts | None = None
        self._embeddings: np.ndarray | None = None

    @property
    def is_ready(self) -> bool:
        return self._artifacts is not None and self._embeddings is not None

    @property
    def chunk_count(self) -> int:
        if self._artifacts is None:
            return 0
        return len(self._artifacts.chunks)

    def _embed(self, texts: list[str]) -> np.ndarray:
        if self._embed_fn is not None:
            vecs = self._embed_fn(texts)
        else:
            from app.rag.embeddings import embed_texts

            vecs = embed_texts(texts)

        arr = np.asarray(vecs, dtype=np.float32)
        if arr.ndim != 2:
            raise ValueError("Embedding function must return a 2D array")

        norms = np.linalg.norm(arr, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        return arr / norms

    def load_from_rows(
        self,
        credit_rows: list[dict[str, Any]],
        investigation_rows: list[dict[str, Any]],
    ) -> dict[str, Any]:
        artifacts = build_pipeline_artifacts(
            credit_rows=credit_rows,
            investigation_rows=investigation_rows,
            rules_path=self.config.rules_path,
            fallback_max_chars=self.config.fallback_max_chars,
        )

        texts = [chunk.text for chunk in artifacts.chunks]
        embeddings = self._embed(texts) if texts else np.empty((0, 1), dtype=np.float32)

        with self._lock:
            self._artifacts = artifacts
            self._embeddings = embeddings

        return {
            "ticket_count": len(artifacts.canonical_tickets),
            "chunk_count": len(artifacts.chunks),
            "embedding_dim": int(embeddings.shape[1]) if embeddings.size else 0,
        }

    def refresh_from_firebase(self) -> dict[str, Any]:
        load_env()
        credit_rows = load_credit_requests()
        investigation_rows = load_investigation_notes()
        return self.load_from_rows(credit_rows, investigation_rows)

    def index_current(self, data_dir: str | Path | None = None) -> dict[str, Any]:
        with self._lock:
            if self._artifacts is None:
                raise RuntimeError("Service is not initialized. Call load_from_rows() first.")
            artifacts = self._artifacts

        target_dir = data_dir or self._default_index_data_dir()
        return index_pipeline_artifacts(artifacts, data_dir=target_dir)

    def _default_index_data_dir(self) -> Path:
        raw = os.environ.get("ACTUS_NEW_RAG_DATA_DIR", "").strip()
        if raw:
            return Path(raw)
        return Path(__file__).resolve().parents[4] / "rag_data" / "new_design"

    def _serialize_results(self, results: list[SearchResult]) -> list[dict[str, Any]]:
        return [
            {
                "score": r.score,
                "semantic_score": r.semantic_score,
                "exact_boost": r.exact_boost,
                "type_boost": r.type_boost,
                "root_cause_boost": r.root_cause_boost,
                "chunk_type": r.chunk_type,
                "ticket_id": r.ticket_id,
                "text": r.text,
                "metadata": r.metadata,
                "intent": r.intent,
                "chunk_id": r.chunk_id,
            }
            for r in results
        ]

    @staticmethod
    def _dedupe_by_ticket(results: list[SearchResult], top_k: int) -> list[SearchResult]:
        deduped: list[SearchResult] = []
        seen: set[str] = set()
        for result in results:
            ticket_id = str(result.ticket_id)
            if ticket_id in seen:
                continue
            deduped.append(result)
            seen.add(ticket_id)
            if len(deduped) >= top_k:
                break
        return deduped

    def search(self, query: str, top_k: int = 5) -> dict[str, Any]:
        with self._lock:
            if self._artifacts is None or self._embeddings is None:
                raise RuntimeError("Service is not initialized. Call refresh_from_firebase() or load_from_rows() first.")
            artifacts = self._artifacts
            embeddings = self._embeddings

        payload = routed_hybrid_search_real(
            query=query,
            chunks=artifacts.chunks,
            embeddings=embeddings,
            query_embedding=self._embed([query])[0],
            top_k=top_k,
        )

        results = self._dedupe_by_ticket(payload["results"], top_k=top_k)
        query_info = payload["query_info"]
        not_found = bool(payload.get("not_found"))

        return {
            "query": query,
            "intent": query_info.intent,
            "not_found": not_found,
            "results": self._serialize_results(results),
        }

    def answer(
        self,
        query: str,
        top_k: int = 10,
        max_tickets_in_answer: int = 5,
    ) -> dict[str, Any]:
        with self._lock:
            if self._artifacts is None or self._embeddings is None:
                raise RuntimeError("Service is not initialized. Call refresh_from_firebase() or load_from_rows() first.")
            artifacts = self._artifacts
            embeddings = self._embeddings

        payload = routed_hybrid_search_real(
            query=query,
            chunks=artifacts.chunks,
            embeddings=embeddings,
            query_embedding=self._embed([query])[0],
            top_k=top_k,
        )

        results = payload["results"]
        query_info = payload["query_info"]
        not_found = bool(payload.get("not_found"))

        if not_found:
            text = f"No exact match found for: {query}"
        else:
            text = answer_from_results(query, results, max_tickets_in_answer=max_tickets_in_answer)

        return {
            "query": query,
            "intent": query_info.intent,
            "not_found": not_found,
            "answer": text,
            "results": self._serialize_results(results),
        }

    def analyze_item(self, item_number: str) -> dict[str, Any]:
        with self._lock:
            if self._artifacts is None:
                raise RuntimeError("Service is not initialized. Call refresh_from_firebase() or load_from_rows() first.")
            canonical_tickets = self._artifacts.canonical_tickets
        return analyze_item_actus(item_number, canonical_tickets)

    def analyze_ticket(self, ticket_id: str, threshold_days: int = 30) -> dict[str, Any]:
        with self._lock:
            if self._artifacts is None:
                raise RuntimeError("Service is not initialized. Call refresh_from_firebase() or load_from_rows() first.")
            canonical_tickets = self._artifacts.canonical_tickets
        return analyze_ticket_actus(ticket_id, canonical_tickets, threshold_days=threshold_days)


_RUNTIME_SERVICE: ActusHybridRAGService | None = None
_RUNTIME_LOCK = RLock()


def get_runtime_service(refresh: bool = False) -> ActusHybridRAGService:
    global _RUNTIME_SERVICE
    with _RUNTIME_LOCK:
        if _RUNTIME_SERVICE is None:
            _RUNTIME_SERVICE = ActusHybridRAGService()

        if refresh or not _RUNTIME_SERVICE.is_ready:
            _RUNTIME_SERVICE.refresh_from_firebase()

        return _RUNTIME_SERVICE
