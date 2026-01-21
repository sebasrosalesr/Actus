from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from typing import Any, Iterable, Optional

import faiss
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


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    """
    Normalize SQLite row into the same chunk shape used everywhere:
      { id, text, chunk_type, metadata }
    Tolerant to missing columns.
    """
    d = dict(row)
    metadata = _safe_json_loads(d.get("metadata_json") or d.get("metadata") or {})
    text = d.get("text") or d.get("content") or d.get("chunk") or ""
    chunk_type = (
        d.get("chunk_type")
        or metadata.get("chunk_type")
        or metadata.get("event_type")
        or "event"
    )
    ticket_id = d.get("ticket_id") or metadata.get("ticket_id") or metadata.get("TicketId")

    if ticket_id and isinstance(metadata, dict):
        metadata.setdefault("ticket_id", ticket_id)

    chunk_id = d.get("chunk_id")
    if chunk_id is None:
        chunk_id = d.get("id")

    return {
        "id": d.get("id"),
        "chunk_id": chunk_id,
        "ticket_id": ticket_id,
        "text": text,
        "chunk_type": chunk_type,
        "metadata": metadata,
    }


def _table_has_column(conn: sqlite3.Connection, table: str, col: str) -> bool:
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        cols = {r[1] for r in rows}
        return col in cols
    except Exception:
        return False


def _pick_id_expr(conn: sqlite3.Connection, table: str) -> str:
    if _table_has_column(conn, table, "id"):
        return "id"
    if _table_has_column(conn, table, "chunk_id"):
        return "chunk_id AS id"
    return "rowid AS id"


class RagStore:
    """
    Local FAISS + SQLite store for RAG chunks.

    - FAISS: IndexFlatIP wrapped by IndexIDMap2 (cosine similarity via L2 norm)
    - SQLite: chunk metadata + text
    """

    def __init__(self, data_dir: str | Path | None = None, embedding_dim: int | None = None) -> None:
        base_dir = Path(data_dir) if data_dir else Path(__file__).resolve().parents[2] / "rag_data"
        self.data_dir = base_dir
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.index_path = self.data_dir / "index.faiss"
        self.sqlite_path = self.data_dir / "chunks.sqlite"
        self.embedding_dim = embedding_dim

        self._conn: sqlite3.Connection | None = None
        self._open()
        self._ensure_schema()

        self.index = self._load_or_create_index()

    def _ensure_schema(self) -> None:
        self._ensure_open()
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chunks (
                chunk_id INTEGER PRIMARY KEY,
                ticket_id TEXT,
                chunk_type TEXT,
                text TEXT,
                metadata_json TEXT
            )
            """
        )
        self._conn.commit()

    def _chunks_table(self) -> str:
        return "chunks"

    def _open(self) -> None:
        if self._conn is None:
            self._conn = sqlite3.connect(self.sqlite_path)
            self._conn.row_factory = sqlite3.Row

    def _ensure_open(self) -> None:
        if self._conn is None:
            self._open()
        try:
            self._conn.execute("SELECT 1")
        except sqlite3.ProgrammingError:
            self._conn = None
            self._open()

    def _load_or_create_index(self) -> faiss.IndexIDMap2:
        if self.index_path.exists():
            index = faiss.read_index(str(self.index_path))
            if not isinstance(index, faiss.IndexIDMap2):
                index = faiss.IndexIDMap2(index)
            self.embedding_dim = index.d
            return index

        if self.embedding_dim is None:
            self.embedding_dim = 1
            base = faiss.IndexFlatIP(self.embedding_dim)
            return faiss.IndexIDMap2(base)

        base = faiss.IndexFlatIP(self.embedding_dim)
        return faiss.IndexIDMap2(base)

    @staticmethod
    def _normalize_embeddings(embeddings: np.ndarray) -> np.ndarray:
        norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        return embeddings / norms

    def _persist_index(self) -> None:
        faiss.write_index(self.index, str(self.index_path))

    def upsert_chunks(self, chunks: list[dict], embeddings: np.ndarray) -> None:
        if not chunks:
            return

        if embeddings.ndim != 2:
            raise ValueError("Embeddings must be a 2D array.")

        if len(chunks) != embeddings.shape[0]:
            raise ValueError("Number of embeddings must match number of chunks.")

        embeddings = embeddings.astype("float32", copy=False)
        embeddings = self._normalize_embeddings(embeddings)

        ids = np.array([int(chunk["chunk_id"]) for chunk in chunks], dtype="int64")
        self.embedding_dim = int(embeddings.shape[1])
        base = faiss.IndexFlatIP(self.embedding_dim)
        self.index = faiss.IndexIDMap2(base)
        self.index.add_with_ids(embeddings, ids)
        self._persist_index()

        rows = [
            (
                int(chunk["chunk_id"]),
                chunk.get("ticket_id"),
                chunk.get("chunk_type"),
                chunk.get("text"),
                json.dumps(chunk.get("metadata") or {}),
            )
            for chunk in chunks
        ]

        self._conn.executemany(
            """
            INSERT OR REPLACE INTO chunks (chunk_id, ticket_id, chunk_type, text, metadata_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            rows,
        )
        self._conn.commit()

    def search(self, query_embedding: np.ndarray, top_k: int = 10) -> list[tuple[int, float]]:
        if self.index.ntotal == 0:
            return []

        if query_embedding.ndim == 1:
            query_embedding = query_embedding[None, :]

        query_embedding = query_embedding.astype("float32", copy=False)
        query_embedding = self._normalize_embeddings(query_embedding)

        scores, ids = self.index.search(query_embedding, top_k)
        results: list[tuple[int, float]] = []
        for chunk_id, score in zip(ids[0].tolist(), scores[0].tolist()):
            if chunk_id == -1:
                continue
            results.append((int(chunk_id), float(score)))
        return results

    def fetch_chunks(self, chunk_ids: Iterable[int]) -> list[dict]:
        self._ensure_open()
        chunk_ids = [int(cid) for cid in chunk_ids]
        if not chunk_ids:
            return []

        placeholders = ", ".join(["?"] * len(chunk_ids))
        rows = self._conn.execute(
            f"""
            SELECT chunk_id, ticket_id, chunk_type, text, metadata_json
            FROM chunks
            WHERE chunk_id IN ({placeholders})
            """,
            chunk_ids,
        ).fetchall()

        by_id: dict[int, dict] = {}
        for row in rows:
            record = _row_to_dict(row)
            cid = record.get("chunk_id")
            if cid is None:
                continue
            by_id[int(cid)] = record

        return [by_id[cid] for cid in chunk_ids if cid in by_id]

    def get_ticket_chunks(self, ticket_id: str) -> list[dict[str, Any]]:
        """
        Return all chunks for a specific ticket_id, using your SQLite schema.
        This is safe across schemas that may not have an 'id' column.
        Requires your table to have a 'ticket_id' column (you said it does).
        """
        self._ensure_open()
        ticket_key = (ticket_id or "").strip().upper()
        if not ticket_key:
            return []

        table = "chunks"
        id_expr = _pick_id_expr(self._conn, table)
        has_chunk_id = _table_has_column(self._conn, table, "chunk_id")
        chunk_id_select = ", chunk_id" if has_chunk_id else ""

        sql = f"""
        SELECT
          {id_expr},
          ticket_id,
          text,
          chunk_type,
          metadata_json
          {chunk_id_select}
        FROM {table}
        WHERE UPPER(ticket_id) = ?
        ORDER BY 1 ASC
        """
        rows = self._conn.execute(sql, (ticket_key,)).fetchall() or []
        return [_row_to_dict(r) for r in rows]

    def get_ticket_line_texts(self, ticket_id: str) -> list[str]:
        """
        Optional fallback: pull raw line-level texts for a ticket from a separate table.
        Change table/column names to match your schema.
        """
        self._ensure_open()

        table = "ticket_lines"
        col_ticket = "ticket_id"
        col_text = "text"

        try:
            rows = self._conn.execute(
                f"SELECT {col_text} AS text FROM {table} WHERE {col_ticket} = ?",
                (ticket_id,),
            ).fetchall()
            return [str(r["text"] or "") for r in rows]
        except sqlite3.OperationalError:
            return []

    def has_data(self) -> bool:
        return self.index_path.exists() and self.index.ntotal > 0

    def reset(self) -> None:
        # Local store rebuilds overwrite data via upsert; keep this as a no-op.
        return None

    def provider_name(self) -> str:
        return "faiss"

    def stats(self) -> dict[str, Any]:
        return {
            "vector_count": int(self.index.ntotal),
            "index_path": str(self.index_path),
            "sqlite_path": str(self.sqlite_path),
        }

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None


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
        response = self.index.query(
            vector=embed_texts([ticket_key])[0].astype("float32", copy=False).tolist(),
            top_k=int(os.environ.get("ACTUS_PINECONE_TICKET_TOP_K", "500")),
            include_metadata=True,
            namespace=self.namespace,
            filter={"ticket_id": {"$eq": ticket_key}},
        )
        matches = getattr(response, "matches", None)
        if matches is None and isinstance(response, dict):
            matches = response.get("matches")
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
) -> RagStore | PineconeRagStore:
    provider = (
        os.environ.get("ACTUS_RAG_PROVIDER")
        or os.environ.get("ACTUS_RAG_BACKEND")
        or "faiss"
    ).strip().lower()
    if provider == "pinecone":
        return PineconeRagStore(embedding_dim=embedding_dim)
    return RagStore(data_dir=data_dir, embedding_dim=embedding_dim)
