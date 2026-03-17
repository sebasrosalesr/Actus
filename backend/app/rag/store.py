from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Iterable

import numpy as np

from app.rag.embeddings import embed_texts


def _safe_json_loads(value: Any) -> dict[str, Any]:
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    try:
        return json.loads(value)
    except Exception:
        return {}


class PineconeRagStore:
    def __init__(
        self,
        api_key: str | None = None,
        index_name: str | None = None,
        namespace: str | None = None,
        embedding_dim: int | None = None,
    ) -> None:
        try:
            from pinecone import Pinecone  # type: ignore
        except Exception as exc:
            raise RuntimeError("pinecone client is not installed") from exc

        self.api_key = api_key or os.environ.get("ACTUS_PINECONE_API_KEY") or os.environ.get("PINECONE_API_KEY")
        self.index_name = index_name or os.environ.get("ACTUS_PINECONE_INDEX") or os.environ.get("PINECONE_INDEX")
        self.namespace = namespace or os.environ.get("ACTUS_PINECONE_NAMESPACE") or os.environ.get("PINECONE_NAMESPACE") or ""
        self.embedding_dim = embedding_dim
        if not self.api_key:
            raise RuntimeError("Missing Pinecone API key (ACTUS_PINECONE_API_KEY or PINECONE_API_KEY).")
        if not self.index_name:
            raise RuntimeError("Missing Pinecone index name (ACTUS_PINECONE_INDEX or PINECONE_INDEX).")

        self._client = Pinecone(api_key=self.api_key)
        self.index = self._client.Index(self.index_name)

    def _normalize_ticket_id(self, ticket_id: str | None) -> str | None:
        if not ticket_id:
            return None
        return str(ticket_id).strip().upper() or None

    def _metadata_for_chunk(self, chunk: dict[str, Any]) -> dict[str, Any]:
        ticket_id = self._normalize_ticket_id(chunk.get("ticket_id"))
        metadata = {
            "ticket_id": ticket_id,
            "chunk_type": chunk.get("chunk_type"),
            "text": chunk.get("text"),
            "metadata_json": json.dumps(chunk.get("metadata") or {}),
        }
        return {k: v for k, v in metadata.items() if v is not None}

    def _safe_to_int(self, value: Any) -> int | None:
        try:
            return int(value)
        except Exception:
            return None

    def _parse_vector_row(self, chunk_id: str, meta: dict[str, Any]) -> dict[str, Any]:
        metadata_json = meta.get("metadata_json") or meta.get("metadata")
        metadata = _safe_json_loads(metadata_json)
        text = meta.get("text") or ""
        chunk_type = (
            meta.get("chunk_type")
            or metadata.get("chunk_type")
            or metadata.get("event_type")
            or "event"
        )
        ticket_id = meta.get("ticket_id") or metadata.get("ticket_id") or metadata.get("TicketId")
        chunk_id_value = meta.get("chunk_id")
        if chunk_id_value is None:
            chunk_id_value = self._safe_to_int(chunk_id) or chunk_id

        if ticket_id and isinstance(metadata, dict):
            metadata.setdefault("ticket_id", ticket_id)

        return {
            "id": self._safe_to_int(chunk_id_value) or chunk_id_value,
            "chunk_id": self._safe_to_int(chunk_id_value) or chunk_id_value,
            "ticket_id": ticket_id,
            "text": text,
            "chunk_type": chunk_type,
            "metadata": metadata,
        }

    def _describe_stats(self) -> dict[str, Any]:
        try:
            response = self.index.describe_index_stats()
        except Exception:
            return {}
        if hasattr(response, "to_dict"):
            return response.to_dict()  # type: ignore[no-any-return]
        if isinstance(response, dict):
            return response
        return {}

    def has_data(self) -> bool:
        stats = self._describe_stats()
        if not stats:
            return True
        if self.namespace:
            namespaces = stats.get("namespaces") or {}
            ns = namespaces.get(self.namespace) or {}
            return int(ns.get("vector_count") or 0) > 0
        return int(stats.get("total_vector_count") or 0) > 0

    def provider_name(self) -> str:
        return "pinecone"

    def stats(self) -> dict[str, Any]:
        stats = self._describe_stats()
        if not stats:
            return {}
        if self.namespace:
            namespaces = stats.get("namespaces") or {}
            ns = namespaces.get(self.namespace) or {}
            return {
                "namespace": self.namespace,
                "vector_count": int(ns.get("vector_count") or 0),
            }
        return {"vector_count": int(stats.get("total_vector_count") or 0)}

    def reset(self) -> None:
        try:
            self.index.delete(delete_all=True, namespace=self.namespace)
        except Exception:
            return None

    def upsert_chunks(self, chunks: list[dict], embeddings: np.ndarray) -> None:
        if not chunks:
            return
        if embeddings.ndim != 2:
            raise ValueError("Embeddings must be a 2D array.")
        if len(chunks) != embeddings.shape[0]:
            raise ValueError("Number of embeddings must match number of chunks.")

        vectors = []
        for chunk, vector in zip(chunks, embeddings):
            chunk_id = chunk.get("chunk_id")
            if chunk_id is None:
                continue
            vectors.append(
                {
                    "id": str(int(chunk_id)),
                    "values": vector.astype("float32", copy=False).tolist(),
                    "metadata": self._metadata_for_chunk(chunk),
                }
            )

        for i in range(0, len(vectors), 100):
            self.index.upsert(vectors=vectors[i:i + 100], namespace=self.namespace)

    def search(self, query_embedding: np.ndarray, top_k: int = 10) -> list[tuple[int, float]]:
        if query_embedding.ndim > 1:
            query_embedding = query_embedding[0]
        response = self.index.query(
            vector=query_embedding.astype("float32", copy=False).tolist(),
            top_k=top_k,
            include_metadata=False,
            namespace=self.namespace,
        )

        matches = getattr(response, "matches", None)
        if matches is None and isinstance(response, dict):
            matches = response.get("matches")
        if not matches:
            return []

        results: list[tuple[int, float]] = []
        for match in matches:
            match_id = match.get("id") if isinstance(match, dict) else getattr(match, "id", None)
            score = match.get("score") if isinstance(match, dict) else getattr(match, "score", None)
            if match_id is None or score is None:
                continue
            try:
                results.append((int(match_id), float(score)))
            except Exception:
                continue
        return results

    def fetch_chunks(self, chunk_ids: Iterable[int]) -> list[dict]:
        ids = [str(int(cid)) for cid in chunk_ids]
        if not ids:
            return []
        response = self.index.fetch(ids=ids, namespace=self.namespace)
        vectors = getattr(response, "vectors", None)
        if vectors is None and isinstance(response, dict):
            vectors = response.get("vectors")
        if not vectors:
            return []

        rows: list[dict[str, Any]] = []
        for chunk_id, vector in vectors.items():
            if isinstance(vector, dict):
                meta = vector.get("metadata") or {}
            else:
                meta = getattr(vector, "metadata", None) or {}
            rows.append(self._parse_vector_row(chunk_id, meta))
        rows_by_id = {row.get("chunk_id"): row for row in rows}
        return [rows_by_id[cid] for cid in chunk_ids if cid in rows_by_id]

    def get_ticket_chunks(self, ticket_id: str) -> list[dict[str, Any]]:
        ticket_key = self._normalize_ticket_id(ticket_id)
        if not ticket_key:
            return []
        vector = embed_texts([ticket_key])[0].astype("float32", copy=False).tolist()
        top_k = int(os.environ.get("ACTUS_PINECONE_TICKET_TOP_K", "500"))
        response = self.index.query(
            vector=vector,
            top_k=top_k,
            include_metadata=True,
            namespace=self.namespace,
            filter={"ticket_id": {"$eq": ticket_key}},
        )
        matches = getattr(response, "matches", None)
        if matches is None and isinstance(response, dict):
            matches = response.get("matches")

        if not matches:
            fallback_top_k = int(os.environ.get("ACTUS_PINECONE_TICKET_FALLBACK_TOP_K", "1000"))
            fallback = self.index.query(
                vector=vector,
                top_k=max(top_k, fallback_top_k),
                include_metadata=True,
                namespace=self.namespace,
            )
            fallback_matches = getattr(fallback, "matches", None)
            if fallback_matches is None and isinstance(fallback, dict):
                fallback_matches = fallback.get("matches")
            if fallback_matches:
                filtered = []
                for match in fallback_matches:
                    if isinstance(match, dict):
                        meta = match.get("metadata") or {}
                    else:
                        meta = getattr(match, "metadata", None) or {}
                    nested_meta = _safe_json_loads(meta.get("metadata_json") or meta.get("metadata"))
                    if not isinstance(nested_meta, dict):
                        nested_meta = {}
                    match_ticket = self._normalize_ticket_id(
                        meta.get("ticket_id")
                        or nested_meta.get("ticket_id")
                    )
                    if match_ticket == ticket_key:
                        filtered.append(match)
                matches = filtered

        if not matches:
            return []
        rows = []
        for match in matches:
            if isinstance(match, dict):
                match_id = match.get("id")
                meta = match.get("metadata") or {}
            else:
                match_id = getattr(match, "id", None)
                meta = getattr(match, "metadata", None) or {}
            if match_id is None:
                continue
            rows.append(self._parse_vector_row(match_id, meta))
        return rows

    def get_ticket_line_texts(self, ticket_id: str) -> list[str]:
        return []

    def close(self) -> None:
        return None


def get_rag_store(
    data_dir: str | Path | None = None,
    embedding_dim: int | None = None,
) -> PineconeRagStore:
    _ = data_dir  # kept for backward-compatible signature
    provider = (
        os.environ.get("ACTUS_RAG_PROVIDER")
        or os.environ.get("ACTUS_RAG_BACKEND")
        or "pinecone"
    ).strip().lower()
    if provider and provider != "pinecone":
        raise RuntimeError(
            "Legacy local RAG store was removed. Set ACTUS_RAG_PROVIDER=pinecone."
        )
    return PineconeRagStore(embedding_dim=embedding_dim)
