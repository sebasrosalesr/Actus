# Actus README V2

Current product and engineering guide for the Actus monorepo.

## What changed in V2

- Legacy RAG pipeline is removed.
- RAG runtime is `new_design` only.
- RAG storage is Pinecone-backed (legacy local FAISS path removed).
- Chat and RAG now support structured ticket and item analyzers.
- Ticket analyzer supports:
  - `credited`
  - `open`
  - `partially credited` (mixed credited/uncredited lines)
- Partially credited tickets include a mixed-lines follow-up path.
- Investigation highlights support model summarization with fallback behavior.

## Repo structure

- `backend/`: FastAPI service, intent router, RAG runtime
- `frontend/`: React + Vite UI

## Core architecture

- Chat entrypoint: `POST /api/ask`
- Intent routing: `backend/actus/intent_router.py`
- RAG runtime service: `backend/app/rag/new_design/service.py`
- RAG API routes: `backend/app/api/rag.py`
- Analyzer logic:
  - Ticket: `backend/app/rag/new_design/analytics.py`
  - Item: `backend/app/rag/new_design/analytics.py`

## Prerequisites

- Python 3.11+
- Node 18+
- Pinecone index configured
- Firebase RTDB credentials configured

## Backend setup

```bash
cd backend
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

## Frontend setup

```bash
cd frontend
npm install
```

## Required environment variables

From `backend/.env.example`:

- Firebase:
  - `ACTUS_FIREBASE_URL`
  - `ACTUS_FIREBASE_JSON` or `ACTUS_FIREBASE_PATH`
- Pinecone:
  - `ACTUS_RAG_PROVIDER=pinecone`
  - `ACTUS_PINECONE_API_KEY`
  - `ACTUS_PINECONE_INDEX`
  - `ACTUS_PINECONE_NAMESPACE`
- OpenRouter (optional but recommended):
  - `ACTUS_OPENROUTER_API_KEY`
  - `ACTUS_OPENROUTER_MODEL`
  - `ACTUS_OPENROUTER_MODEL_FALLBACK`
  - `ACTUS_OPENROUTER_HIGHLIGHTS_MODEL`
  - `ACTUS_OPENROUTER_HIGHLIGHTS_MODEL_FALLBACK`
  - `ACTUS_OPENROUTER_MODE` (`always` or `fallback`)
  - `ACTUS_INV_NOTE_SUMMARY=true`
- Optional API protection:
  - `ACTUS_API_KEY`

## Run locally

Backend:

```bash
cd backend
set -a; source .env; set +a
uvicorn main:APP --reload --port 8000
```

Frontend:

```bash
cd frontend
npm run dev
```

## Health checks

```bash
curl -s http://127.0.0.1:8000/api/health | jq
curl -s http://127.0.0.1:8000/api/health/openrouter | jq
curl -s http://127.0.0.1:8000/rag/health | jq
```

If `ACTUS_API_KEY` is enabled, include:

```bash
-H "x-api-key: $ACTUS_API_KEY"
```

## RAG index build (V2)

```bash
python backend/scripts/build_rag_index.py
```

The script builds the `new_design` pipeline only.

## Main API routes (V2)

- `POST /api/ask`
- `GET /api/help`
- `GET /api/health`
- `GET /api/health/openrouter`
- `GET /api/user-context?email=...`
- `GET /rag/health`
- `GET /rag/ticket/{ticket_id}/refs`
- `POST /rag/next-action/trace`
- `POST /rag/new/search`
- `POST /rag/new/answer`
- `POST /rag/new/refresh`
- `POST /rag/new/item-analysis`
- `POST /rag/new/ticket-analysis`

## Useful curl examples

Ticket analyzer:

```bash
curl -sS -X POST http://127.0.0.1:8000/rag/new/ticket-analysis \
  -H 'Content-Type: application/json' \
  -d '{"ticket_id":"R-048484","threshold_days":30}' | jq
```

Item analyzer:

```bash
curl -sS -X POST http://127.0.0.1:8000/rag/new/item-analysis \
  -H 'Content-Type: application/json' \
  -d '{"item_number":"1007986"}' | jq
```

RAG search:

```bash
curl -sS -X POST http://127.0.0.1:8000/rag/new/search \
  -H 'Content-Type: application/json' \
  -d '{"query":"price loaded after invoice","top_k":8}' | jq
```

## Intent capabilities

Intent catalog is exposed in help text and includes:

- Analyze ticket
- Analyze item
- Ticket status
- Mixed lines
- Credit trends
- Stalled tickets
- Root causes
- Top accounts/reps/items
- Credit amount chart
- Investigation notes
- Bulk search
- and more

Reference: `backend/actus/help_text.py`

## Ticket analyzer behavior in V2

- Uses line-level credit evidence where available.
- Distinguishes:
  - fully credited
  - open
  - partially credited
- For partially credited tickets:
  - status messaging focuses on pending portion
  - follow-up includes mixed-lines display

## Tests

Backend:

```bash
cd backend
pytest -q tests/actus tests/rag_new_design
```

Frontend build check:

```bash
cd frontend
npm run build
```

## Deployment note (Fly scheduler)

Scheduled rebuild can run:

```bash
python scripts/build_rag_index.py
```

inside backend app context, with required Firebase/Pinecone env vars present.

