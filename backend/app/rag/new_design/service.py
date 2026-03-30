from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
from threading import RLock, Thread
import sqlite3
from typing import Any, Callable

import numpy as np

from .answer import answer_from_results
from .analytics import analyze_customer_actus, analyze_item_actus, analyze_ticket_actus
from .index_build import build_pipeline_artifacts, build_retrieval_chunks, index_pipeline_artifacts
from .ingest import load_credit_requests, load_env, load_investigation_notes
from .models import PipelineArtifacts, RetrievalChunk, SearchResult
from .snapshot import load_canonical_ticket_models, save_canonical_tickets
from .retrieve import (
    analyze_query,
    compute_chunk_type_boost,
    compute_exact_match_boost,
    compute_phrase_boost,
    compute_root_cause_boost,
    rerank_diversity,
    route_candidate_chunks,
    routed_hybrid_search_real,
)


@dataclass(slots=True)
class ServiceConfig:
    rules_path: str | Path | None = None
    fallback_max_chars: int = 700
    catalog_db_path: str | Path | None = None


class ActusHybridRAGService:
    """Runtime service for the hybrid RAG pipeline."""

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

        self._catalog_chunks: list[RetrievalChunk] | None = None
        self._chunk_by_id: dict[int, RetrievalChunk] = {}
        self._store: Any = None

    @property
    def is_ready(self) -> bool:
        return self._catalog_chunks is not None and self._store is not None

    @property
    def chunk_count(self) -> int:
        with self._lock:
            if self._catalog_chunks is not None:
                return len(self._catalog_chunks)
            if self._artifacts is not None:
                return len(self._artifacts.chunks)
        return 0

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

    @staticmethod
    def _as_list(value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, list):
            return [str(v) for v in value if v not in {None, ""}]
        if isinstance(value, (tuple, set)):
            return [str(v) for v in value if v not in {None, ""}]
        text = str(value).strip()
        return [text] if text else []

    def _normalize_metadata(self, metadata: dict[str, Any] | None) -> dict[str, Any]:
        payload = dict(metadata or {})

        invoice_numbers = self._as_list(
            payload.get("invoice_numbers")
            or payload.get("invoice")
            or payload.get("invoice_ids")
        )
        if invoice_numbers:
            payload["invoice_numbers"] = invoice_numbers
            payload.setdefault("invoice_number", invoice_numbers[0])

        item_numbers = self._as_list(payload.get("item_numbers") or payload.get("items"))
        if item_numbers:
            payload["item_numbers"] = item_numbers
            payload.setdefault("item_number", item_numbers[0])

        root_ids = self._as_list(
            payload.get("root_cause_ids")
            or payload.get("root_cause_rule_ids")
        )
        if root_ids:
            payload["root_cause_ids"] = root_ids

        primary_root = payload.get("root_cause_primary_id") or payload.get("root_cause_rule_id")
        if primary_root:
            payload["root_cause_primary_id"] = str(primary_root)

        ticket_id = payload.get("ticket_id")
        if ticket_id:
            payload["ticket_id"] = str(ticket_id).upper()

        return payload

    def _chunk_from_row(
        self,
        *,
        chunk_id: int,
        ticket_id: str,
        chunk_type: str,
        text: str,
        metadata: dict[str, Any] | None,
    ) -> RetrievalChunk:
        return RetrievalChunk(
            chunk_id=int(chunk_id),
            ticket_id=str(ticket_id).upper(),
            chunk_type=str(chunk_type),
            text=str(text),
            metadata=self._normalize_metadata(metadata),
        )

    def _default_catalog_db_path(self) -> Path:
        raw = (
            os.environ.get("ACTUS_RAG_CHUNKS_DB_PATH", "").strip()
            or os.environ.get("ACTUS_NEW_RAG_CHUNKS_DB_PATH", "").strip()
        )
        if raw:
            return Path(raw)
        if self.config.catalog_db_path:
            return Path(self.config.catalog_db_path)
        return Path(__file__).resolve().parents[3] / "rag_data" / "chunks.sqlite"

    def _load_catalog_from_sqlite(self) -> list[RetrievalChunk]:
        db_path = self._default_catalog_db_path()
        if not db_path.exists():
            raise RuntimeError(f"Chunk catalog not found at {db_path}")

        chunks: list[RetrievalChunk] = []
        with sqlite3.connect(db_path) as conn:
            rows = conn.execute(
                """
                SELECT chunk_id, ticket_id, chunk_type, text, metadata_json
                FROM chunks
                ORDER BY chunk_id
                """
            ).fetchall()

        for chunk_id, ticket_id, chunk_type, text, metadata_json in rows:
            metadata: dict[str, Any] = {}
            if metadata_json:
                try:
                    metadata = json.loads(metadata_json)
                except Exception:
                    metadata = {}
            chunks.append(
                self._chunk_from_row(
                    chunk_id=int(chunk_id),
                    ticket_id=str(ticket_id),
                    chunk_type=str(chunk_type),
                    text=str(text),
                    metadata=metadata,
                )
            )
        return chunks

    def _load_catalog_from_snapshot(self) -> list[RetrievalChunk] | None:
        try:
            canonical_tickets = load_canonical_ticket_models()
        except FileNotFoundError:
            return None
        return build_retrieval_chunks(canonical_tickets)

    def _update_catalog(self, chunks: list[RetrievalChunk]) -> None:
        with self._lock:
            if self._catalog_chunks is None:
                self._catalog_chunks = []
            for chunk in chunks:
                if chunk.chunk_id in self._chunk_by_id:
                    self._chunk_by_id[chunk.chunk_id] = chunk
                    continue
                self._catalog_chunks.append(chunk)
                self._chunk_by_id[chunk.chunk_id] = chunk

    def _get_store(self):
        if self._store is None:
            from app.rag.store import get_rag_store

            self._store = get_rag_store()
        return self._store

    def _ensure_catalog_ready(self) -> None:
        with self._lock:
            has_catalog = self._catalog_chunks is not None
        if not has_catalog:
            chunks = self._load_catalog_from_snapshot()
            if chunks is None:
                chunks = self._load_catalog_from_sqlite()
            with self._lock:
                self._catalog_chunks = chunks
                self._chunk_by_id = {chunk.chunk_id: chunk for chunk in chunks}

    def ensure_search_ready(self) -> None:
        self._ensure_catalog_ready()
        with self._lock:
            has_store = self._store is not None
        if not has_store:
            self._get_store()

    def _load_canonical_snapshot(self) -> dict[str, Any] | None:
        try:
            return load_canonical_ticket_models()
        except FileNotFoundError:
            return None
        except Exception:
            raise

    def _hydrate_artifacts_from_snapshot(self) -> bool:
        canonical_tickets = self._load_canonical_snapshot()
        if not canonical_tickets:
            return False

        try:
            self._ensure_catalog_ready()
        except Exception:
            pass

        with self._lock:
            if self._artifacts is not None:
                return True
            chunks = list(self._catalog_chunks or [])
            self._artifacts = PipelineArtifacts(
                ticket_line_map={},
                canonical_tickets=canonical_tickets,
                chunks=chunks,
            )
        return True

    def warm(self) -> None:
        if self._embed_fn is not None:
            self.ensure_search_ready()
            self._embed(["warmup"])
            return

        from app.rag.embeddings import warm_embedding_model

        errors: list[Exception] = []

        def _run(task: Callable[[], None]) -> None:
            try:
                task()
            except Exception as exc:
                errors.append(exc)

        threads = [
            Thread(target=_run, args=(self.ensure_search_ready,), name="rag-search-ready"),
            Thread(target=_run, args=(warm_embedding_model,), name="rag-embed-warm"),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        if errors:
            raise errors[0]

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
            self._catalog_chunks = list(artifacts.chunks)
            self._chunk_by_id = {chunk.chunk_id: chunk for chunk in artifacts.chunks}

        save_canonical_tickets(artifacts.canonical_tickets)
        self._get_store()

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

    def _refresh_canonical_only_from_firebase(self) -> dict[str, Any]:
        load_env()
        credit_rows = load_credit_requests()
        investigation_rows = load_investigation_notes()
        artifacts = build_pipeline_artifacts(
            credit_rows=credit_rows,
            investigation_rows=investigation_rows,
            rules_path=self.config.rules_path,
            fallback_max_chars=self.config.fallback_max_chars,
        )
        snapshot_path = save_canonical_tickets(artifacts.canonical_tickets)

        with self._lock:
            existing_chunks = list(
                self._catalog_chunks
                or (self._artifacts.chunks if self._artifacts is not None else artifacts.chunks)
            )
            self._artifacts = PipelineArtifacts(
                ticket_line_map=artifacts.ticket_line_map,
                canonical_tickets=artifacts.canonical_tickets,
                chunks=existing_chunks,
            )

        return {
            "ticket_count": len(artifacts.canonical_tickets),
            "snapshot_path": str(snapshot_path),
        }

    def _ensure_canonical_ready(self) -> None:
        with self._lock:
            if self._artifacts is not None:
                return
        if self._hydrate_artifacts_from_snapshot():
            return
        self.refresh_from_firebase()

    def index_current(self, data_dir: str | Path | None = None) -> dict[str, Any]:
        self._ensure_canonical_ready()
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
        return Path(__file__).resolve().parents[3] / "rag_data" / "new_design"

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

    def _load_missing_chunks(self, chunk_ids: list[int]) -> None:
        missing = [chunk_id for chunk_id in chunk_ids if chunk_id not in self._chunk_by_id]
        if not missing:
            return

        rows = self._get_store().fetch_chunks(missing)
        hydrated = [
            self._chunk_from_row(
                chunk_id=int(row["chunk_id"]),
                ticket_id=str(row.get("ticket_id") or ""),
                chunk_type=str(row.get("chunk_type") or "event"),
                text=str(row.get("text") or ""),
                metadata=row.get("metadata") if isinstance(row.get("metadata"), dict) else {},
            )
            for row in rows
        ]
        self._update_catalog(hydrated)

    def _search_via_store(self, query: str, top_k: int = 5) -> dict[str, Any]:
        self.ensure_search_ready()
        with self._lock:
            chunks = list(self._catalog_chunks or [])
            chunk_by_id = dict(self._chunk_by_id)

        query_info = analyze_query(query)
        candidates, exact_not_found = route_candidate_chunks(query_info, chunks)

        if exact_not_found:
            return {"results": [], "query_info": query_info, "not_found": True, "intent": query_info.intent}

        if not candidates and query_info.intent != "semantic_lookup":
            return {"results": [], "query_info": query_info, "not_found": False, "intent": query_info.intent}

        query_embedding = self._embed([query])[0]
        score_map: dict[int, float] = {}

        if query_info.intent == "semantic_lookup":
            initial_k = top_k * 5
            matches = self._get_store().search(query_embedding, top_k=max(initial_k * 3, 50))
            score_map = {chunk_id: float(score) for chunk_id, score in matches}
            allowed_ids = {chunk.chunk_id for _, chunk in candidates} if candidates else set(chunk_by_id)
            selected_ids = [chunk_id for chunk_id, _score in matches if chunk_id in allowed_ids]

            self._load_missing_chunks(selected_ids)
            with self._lock:
                chunk_by_id = dict(self._chunk_by_id)
            filtered_chunks = [chunk_by_id[chunk_id] for chunk_id in selected_ids if chunk_id in chunk_by_id]
            if not filtered_chunks:
                return {"results": [], "query_info": query_info, "not_found": False, "intent": query_info.intent}
        else:
            matches = self._get_store().search(query_embedding, top_k=min(max(top_k * 10, 25), 200))
            score_map = {chunk_id: float(score) for chunk_id, score in matches}
            filtered_chunks = [chunk for _, chunk in candidates]

        scored: list[SearchResult] = []
        for chunk in filtered_chunks:
            semantic = float(score_map.get(chunk.chunk_id, 0.0))
            exact = compute_exact_match_boost(query, chunk)
            type_boost = compute_chunk_type_boost(query, chunk.chunk_type)
            phrase_boost = compute_phrase_boost(query, chunk.text)
            root_boost = compute_root_cause_boost(query, chunk.metadata or {})
            total_type_boost = type_boost + phrase_boost
            final = semantic + exact + total_type_boost + root_boost

            scored.append(
                SearchResult(
                    score=final,
                    semantic_score=semantic,
                    exact_boost=exact,
                    type_boost=total_type_boost,
                    root_cause_boost=root_boost,
                    chunk_type=chunk.chunk_type,
                    ticket_id=chunk.ticket_id,
                    text=chunk.text,
                    metadata=chunk.metadata or {},
                    intent=query_info.intent,
                    chunk_id=chunk.chunk_id,
                )
            )

        scored.sort(key=lambda result: (-result.score, result.ticket_id, result.chunk_id))
        initial_k = top_k * 5 if query_info.intent == "semantic_lookup" else top_k
        scored = scored[:initial_k]
        final = rerank_diversity(scored, query_info.intent, max_chunks_per_ticket=2, final_top_k=top_k)
        return {"results": final, "query_info": query_info, "not_found": False, "intent": query_info.intent}

    def _run_search(self, query: str, top_k: int) -> dict[str, Any]:
        query_info = analyze_query(query)
        if query_info.intent == "aging_lookup":
            self._ensure_canonical_ready()
            with self._lock:
                if self._artifacts is None:
                    raise RuntimeError("Service is not initialized. Call refresh_from_firebase() or load_from_rows() first.")
                artifacts = self._artifacts

            return routed_hybrid_search_real(
                query=query,
                chunks=artifacts.chunks,
                embeddings=np.empty((0, 1), dtype=np.float32),
                top_k=top_k,
                canonical_tickets=artifacts.canonical_tickets,
            )

        return self._search_via_store(query, top_k=top_k)

    def search(self, query: str, top_k: int = 5) -> dict[str, Any]:
        payload = self._run_search(query, top_k=top_k)
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
        payload = self._run_search(query, top_k=top_k)
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

    def get_canonical_tickets(
        self,
        *,
        required_ticket_ids: set[str] | None = None,
    ) -> dict[str, Any]:
        self._ensure_canonical_ready()
        with self._lock:
            if self._artifacts is None:
                raise RuntimeError("Service is not initialized. Call refresh_from_firebase() or load_from_rows() first.")
            canonical_tickets = self._artifacts.canonical_tickets

        normalized_required = {
            str(ticket_id).strip().upper()
            for ticket_id in (required_ticket_ids or set())
            if str(ticket_id).strip()
        }
        missing = normalized_required.difference(canonical_tickets.keys())
        if missing:
            self._refresh_canonical_only_from_firebase()
            with self._lock:
                if self._artifacts is None:
                    raise RuntimeError("Service is not initialized. Call refresh_from_firebase() or load_from_rows() first.")
                canonical_tickets = self._artifacts.canonical_tickets

        return canonical_tickets

    def analyze_item(self, item_number: str) -> dict[str, Any]:
        self._ensure_canonical_ready()
        with self._lock:
            if self._artifacts is None:
                raise RuntimeError("Service is not initialized. Call refresh_from_firebase() or load_from_rows() first.")
            canonical_tickets = self._artifacts.canonical_tickets
        return analyze_item_actus(item_number, canonical_tickets)

    def analyze_customer(
        self,
        customer_query: str,
        *,
        match_mode: str = "account_prefix",
        threshold_days: int = 30,
    ) -> dict[str, Any]:
        self._ensure_canonical_ready()
        with self._lock:
            if self._artifacts is None:
                raise RuntimeError("Service is not initialized. Call refresh_from_firebase() or load_from_rows() first.")
            canonical_tickets = self._artifacts.canonical_tickets
        return analyze_customer_actus(
            customer_query,
            canonical_tickets,
            match_mode=match_mode,
            threshold_days=threshold_days,
        )

    def analyze_ticket(self, ticket_id: str, threshold_days: int = 30) -> dict[str, Any]:
        self._ensure_canonical_ready()
        with self._lock:
            if self._artifacts is None:
                raise RuntimeError("Service is not initialized. Call refresh_from_firebase() or load_from_rows() first.")
            canonical_tickets = self._artifacts.canonical_tickets
        analysis = analyze_ticket_actus(ticket_id, canonical_tickets, threshold_days=threshold_days)

        # The live app can keep serving an older canonical snapshot long after Pinecone
        # has been rebuilt. On a miss, rebuild only the canonical ticket map from Firebase
        # and retry without recomputing embeddings.
        if str(analysis.get("answer") or "").endswith("was not found."):
            self._refresh_canonical_only_from_firebase()
            with self._lock:
                if self._artifacts is None:
                    raise RuntimeError("Service is not initialized. Call refresh_from_firebase() or load_from_rows() first.")
                canonical_tickets = self._artifacts.canonical_tickets
            analysis = analyze_ticket_actus(ticket_id, canonical_tickets, threshold_days=threshold_days)

        return analysis


_RUNTIME_SERVICE: ActusHybridRAGService | None = None
_RUNTIME_LOCK = RLock()


def get_runtime_service(
    refresh: bool = False,
    *,
    search_ready: bool = True,
) -> ActusHybridRAGService:
    global _RUNTIME_SERVICE
    with _RUNTIME_LOCK:
        if _RUNTIME_SERVICE is None:
            _RUNTIME_SERVICE = ActusHybridRAGService()

        if refresh:
            _RUNTIME_SERVICE.refresh_from_firebase()
        elif search_ready:
            _RUNTIME_SERVICE.ensure_search_ready()

        return _RUNTIME_SERVICE
