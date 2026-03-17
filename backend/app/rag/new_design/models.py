from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class InvestigationNote:
    ticket_id: str
    invoice_number: str | None
    item_number: str | None
    combo_key: str | None
    note_id: str
    title: str | None
    body_raw: str
    body_clean: str
    customer_number: str | None
    created_at: str | None
    created_by: str | None
    updated_at: str | None
    updated_by: str | None
    source_node: str = "investigation_notes"


@dataclass(slots=True)
class InvestigationChunk:
    chunk_id: str
    chunk_index: int
    chunk_type: str
    section_name: str
    chunk_text: str
    ticket_id: str
    invoice_number: str | None
    item_number: str | None
    combo_key: str | None
    note_id: str
    title: str | None
    source_node: str
    created_at: str | None


@dataclass(slots=True)
class TicketLine:
    row_index: int
    invoice_number: str
    item_number: str
    combo_key: str
    customer_number: str | None = None
    sales_rep: str | None = None
    credit_number: str | None = None
    reason_for_credit_raw_list: list[str] = field(default_factory=list)
    investigation_notes: list[InvestigationNote] = field(default_factory=list)
    investigation_chunks: list[InvestigationChunk] = field(default_factory=list)
    root_cause_ids: list[str] = field(default_factory=list)
    root_cause_labels: list[str] = field(default_factory=list)
    root_cause_primary_id: str = "unidentified"
    root_cause_primary_label: str = "Unidentified"
    root_cause_triggers: list[str] = field(default_factory=list)
    root_cause_score: float = 0.0
    credit_request_total: float = 0.0


@dataclass(slots=True)
class CanonicalTicket:
    ticket_id: str
    source_nodes: list[str]
    customer_numbers: list[str]
    sales_reps: list[str]
    invoice_numbers: list[str]
    item_numbers: list[str]
    credit_numbers: list[str]
    credit_request_totals: list[float]
    reason_for_credit_raw_list: list[str]
    status_raw_list: list[str]
    root_cause_ids: list[str]
    root_cause_labels: list[str]
    root_cause_primary_id: str
    root_cause_primary_label: str
    root_cause_triggers: list[str]
    line_map: dict[str, list[TicketLine]]
    account_prefixes: list[str]


@dataclass(slots=True)
class RetrievalChunk:
    chunk_id: int
    ticket_id: str
    chunk_type: str
    text: str
    metadata: dict[str, Any]


@dataclass(slots=True)
class RootCauseRule:
    id: str
    label: str
    priority: int = 0
    threshold: int = 1
    keywords: list[str] = field(default_factory=list)
    negative_keywords: list[str] = field(default_factory=list)


@dataclass(slots=True)
class RootCauseMatch:
    id: str
    label: str
    priority: int
    count: int
    score: float
    triggers: list[str]


@dataclass(slots=True)
class PipelineArtifacts:
    ticket_line_map: dict[str, dict[str, list[TicketLine]]]
    canonical_tickets: dict[str, CanonicalTicket]
    chunks: list[RetrievalChunk]


@dataclass(slots=True)
class QueryInfo:
    raw_query: str
    intent: str
    ticket_ids: list[str]
    invoice_ids: list[str]
    item_candidates: list[str]
    is_status_lookup: bool
    is_reason_lookup: bool


@dataclass(slots=True)
class SearchResult:
    score: float
    semantic_score: float
    exact_boost: float
    type_boost: float
    root_cause_boost: float
    chunk_type: str
    ticket_id: str
    text: str
    metadata: dict[str, Any]
    intent: str
    chunk_id: int
