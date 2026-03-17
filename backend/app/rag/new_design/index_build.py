from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np

from .canonicalize import (
    attach_investigation_notes,
    build_canonical_tickets,
    build_line_map,
    compute_line_root_causes,
)
from .models import CanonicalTicket, PipelineArtifacts, RetrievalChunk, RootCauseRule
from .root_cause import load_root_cause_rules


def _build_ticket_summary_chunk(ticket: CanonicalTicket, chunk_id: int) -> RetrievalChunk:
    text = (
        f"Ticket {ticket.ticket_id}. "
        f"Customers: {', '.join(ticket.customer_numbers) if ticket.customer_numbers else 'None'}. "
        f"Sales reps: {', '.join(ticket.sales_reps) if ticket.sales_reps else 'None'}. "
        f"Invoice count: {len(ticket.invoice_numbers)}. "
        f"Item count: {len(ticket.item_numbers)}. "
        f"Primary root cause: {ticket.root_cause_primary_id}. "
        f"All root causes: {', '.join(ticket.root_cause_ids) if ticket.root_cause_ids else 'unidentified'}."
    )

    return RetrievalChunk(
        chunk_id=chunk_id,
        ticket_id=ticket.ticket_id,
        chunk_type="ticket_summary",
        text=text,
        metadata={
            "ticket_id": ticket.ticket_id,
            "source_nodes": ticket.source_nodes,
            "customer_numbers": ticket.customer_numbers,
            "sales_reps": ticket.sales_reps,
            "invoice_numbers": ticket.invoice_numbers,
            "item_numbers": ticket.item_numbers,
            "credit_numbers": ticket.credit_numbers,
            "root_cause_ids": ticket.root_cause_ids,
            "root_cause_primary_id": ticket.root_cause_primary_id,
        },
    )


def _build_ticket_line_chunks(ticket: CanonicalTicket, start_chunk_id: int) -> list[RetrievalChunk]:
    chunks: list[RetrievalChunk] = []
    chunk_id = start_chunk_id

    for combo_key, lines in ticket.line_map.items():
        for line in lines:
            reason = " | ".join(line.reason_for_credit_raw_list) if line.reason_for_credit_raw_list else "None"
            text = (
                f"Ticket {ticket.ticket_id} line {combo_key}. "
                f"Invoice {line.invoice_number}. "
                f"Item {line.item_number}. "
                f"Primary root cause: {line.root_cause_primary_id}. "
                f"All root causes: {', '.join(line.root_cause_ids) if line.root_cause_ids else 'unidentified'}. "
                f"Reason for credit: {reason}."
            )

            chunks.append(
                RetrievalChunk(
                    chunk_id=chunk_id,
                    ticket_id=ticket.ticket_id,
                    chunk_type="ticket_line_summary",
                    text=text,
                    metadata={
                        "ticket_id": ticket.ticket_id,
                        "combo_key": combo_key,
                        "invoice_number": line.invoice_number,
                        "item_number": line.item_number,
                        "root_cause_ids": line.root_cause_ids,
                        "root_cause_primary_id": line.root_cause_primary_id,
                    },
                )
            )
            chunk_id += 1

    return chunks


def _build_ticket_investigation_chunks(ticket: CanonicalTicket, start_chunk_id: int) -> list[RetrievalChunk]:
    chunks: list[RetrievalChunk] = []
    chunk_id = start_chunk_id

    for combo_key, lines in ticket.line_map.items():
        for line in lines:
            for inv_chunk in line.investigation_chunks:
                text = (
                    f"Ticket {ticket.ticket_id} investigation for invoice {line.invoice_number} "
                    f"item {line.item_number}. "
                    f"Section: {inv_chunk.section_name}. {inv_chunk.chunk_text}"
                )

                chunks.append(
                    RetrievalChunk(
                        chunk_id=chunk_id,
                        ticket_id=ticket.ticket_id,
                        chunk_type="ticket_investigation_section",
                        text=text,
                        metadata={
                            "ticket_id": ticket.ticket_id,
                            "combo_key": combo_key,
                            "invoice_number": line.invoice_number,
                            "item_number": line.item_number,
                            "section_name": inv_chunk.section_name,
                            "note_id": inv_chunk.note_id,
                            "root_cause_ids": line.root_cause_ids,
                            "root_cause_primary_id": line.root_cause_primary_id,
                        },
                    )
                )
                chunk_id += 1

    return chunks


def build_retrieval_chunks(canonical_tickets: dict[str, CanonicalTicket]) -> list[RetrievalChunk]:
    chunks: list[RetrievalChunk] = []
    chunk_id = 1

    for ticket_id in sorted(canonical_tickets.keys()):
        ticket = canonical_tickets[ticket_id]

        chunks.append(_build_ticket_summary_chunk(ticket, chunk_id=chunk_id))
        chunk_id += 1

        line_chunks = _build_ticket_line_chunks(ticket, start_chunk_id=chunk_id)
        chunks.extend(line_chunks)
        chunk_id += len(line_chunks)

        investigation_chunks = _build_ticket_investigation_chunks(ticket, start_chunk_id=chunk_id)
        chunks.extend(investigation_chunks)
        chunk_id += len(investigation_chunks)

    return chunks


def build_pipeline_artifacts(
    credit_rows: list[dict[str, Any]],
    investigation_rows: list[dict[str, Any]],
    *,
    rules: list[RootCauseRule] | None = None,
    rules_path: str | Path | None = None,
    fallback_max_chars: int = 700,
) -> PipelineArtifacts:
    active_rules = rules or load_root_cause_rules(rules_path)

    ticket_line_map = build_line_map(credit_rows)
    attach_investigation_notes(ticket_line_map, investigation_rows, fallback_max_chars=fallback_max_chars)
    compute_line_root_causes(ticket_line_map, active_rules)

    canonical_tickets = build_canonical_tickets(credit_rows, ticket_line_map)
    chunks = build_retrieval_chunks(canonical_tickets)

    return PipelineArtifacts(
        ticket_line_map=ticket_line_map,
        canonical_tickets=canonical_tickets,
        chunks=chunks,
    )


def index_pipeline_artifacts(artifacts: PipelineArtifacts, data_dir: str | Path | None = None) -> dict[str, Any]:
    from app.rag.embeddings import embed_texts
    from app.rag.store import get_rag_store

    chunks = artifacts.chunks
    if not chunks:
        raise RuntimeError("No chunks produced by pipeline.")

    texts = [chunk.text for chunk in chunks]
    embeddings = embed_texts(texts)
    if not isinstance(embeddings, np.ndarray):
        raise ValueError("embed_texts() must return numpy.ndarray")

    norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    embeddings = embeddings / norms

    store = get_rag_store(data_dir=data_dir, embedding_dim=embeddings.shape[1])
    try:
        store.reset()
    except Exception:
        pass

    payload = [
        {
            "chunk_id": c.chunk_id,
            "ticket_id": c.ticket_id,
            "chunk_type": c.chunk_type,
            "text": c.text,
            "metadata": c.metadata,
        }
        for c in chunks
    ]
    store.upsert_chunks(payload, embeddings)

    info = {
        "chunk_count": len(chunks),
        "vector_dim": int(embeddings.shape[1]),
    }
    if hasattr(store, "sqlite_path"):
        info["sqlite_path"] = str(store.sqlite_path)
    store.close()
    return info
