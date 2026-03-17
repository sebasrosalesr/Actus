# Actus

Monorepo with a React frontend and a Python FastAPI backend for the Actus agent orchestration system.

## Structure

- `backend/` Python service (FastAPI + Actus intent router)
- `frontend/` React UI (Vite)

## Tech specs (business-friendly)

Actus combines a fast, modern web UI with a reliable backend that connects to your operational data. The stack is chosen to keep the product responsive for users while staying dependable and easy to maintain for the team.

Frontend (user experience):
- React 19 + TypeScript 5.9 for a safe, polished UI
- Vite 7 for fast iteration and builds
- Tailwind CSS 3.4 for consistent design
- react-markdown + remark-gfm for rich ticket summaries
- lucide-react for clean, consistent icons

Backend (data + automation):
- Python (FastAPI) for a robust API and intent routing
- Uvicorn for high-performance serving
- pandas for analytics and ticket enrichment
- Firebase Admin SDK (RTDB) for real-time data access
- python-dateutil, python-dotenv for configuration and date handling
- openpyxl (xlsx), optional xlrd (xls) for spreadsheet imports

## Backend setup

```bash
cd backend
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Create a local env file:

```bash
cp .env.example .env
```

Set Firebase credentials (one of):

```bash
export ACTUS_FIREBASE_JSON='{"type":"service_account", ... }'
# or
export ACTUS_FIREBASE_PATH="/absolute/path/to/firebase.json"
```

Optional (defaults to your RTDB URL if not set):

```bash
export ACTUS_FIREBASE_URL="https://creditapp-tm-default-rtdb.firebaseio.com/"
```

Optional security and CORS:

```bash
export ACTUS_API_KEY="your-api-key"
export ACTUS_CORS_ORIGINS="https://your-vercel-domain.com"
```

## OpenRouter 

Actus can route `/api/ask` to OpenRouter.

```bash
export ACTUS_OPENROUTER_API_KEY="your-openrouter-key"
export ACTUS_OPENROUTER_MODEL="openai/gpt-4o-mini"
# Modes: "always" (use OpenRouter for all asks) or "fallback" (only when Actus doesn't match an intent)
export ACTUS_OPENROUTER_MODE="fallback"
```

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
npm install
npm run dev
```

The dev server proxies `/api` to `http://localhost:8000`.

## RAG health checks

Endpoint:

```bash
curl -s http://127.0.0.1:8000/rag/health | jq
```

CLI script:

```bash
python backend/scripts/rag_health_check.py
```

## Fly scheduled RAG rebuild

Use Fly Machines scheduling to run the index build every 15 minutes. This runs in the backend image and uses the same env vars (set via `fly secrets set`).

Example (replace placeholders):

```bash
flyctl machines run \
  --app <fly-backend-app> \
  --schedule "*/15 * * * *" \
  --command "python" \
  -- \
  scripts/build_rag_index.py
```

`scripts/build_rag_index.py` now runs the `new_design` pipeline only (legacy pipeline removed).

If your app is not in the backend root, add `--workdir /app/backend` (or your deploy path).

## Next action rules

Edit `backend/config/next_action_rules.json` to adjust deterministic "Next Action" routing for RAG.
Rules are evaluated in priority order; the first match wins. Each rule has:
- `when`: condition tree (`all`, `any`, `not`, `signal`, `flag`, `field`)
- `action`: `next_action`, `confidence`, `reason_codes`, optional `tag`

Example trace request:

```bash
curl -s http://127.0.0.1:8000/rag/next-action/trace \
  -H "Content-Type: application/json" \
  -d '{
    "ticket_id": "R-012345",
    "reason_for_credit": "PPD mismatch",
    "snippets": [
      {"text": "ticket R-012345 summary: invoice: INV14068709 customer: DWC01", "chunk_type": "summary"}
    ]
  }'
```

Example response (trimmed):

```json
{
  "ticket_id": "R-012345",
  "decision": {
    "next_action": "Likely PPD mismatch. Verify contract/PPD coverage + effective date; confirm whether items should be PPD vs non-PPD; if mismatch confirmed, update pricing and credit difference.",
    "action_confidence": "medium",
    "action_reason_codes": ["tier_b_summary_status", "ppd_keywords"],
    "action_rule_id": "summary_ppd_medium"
  }
}
```

## Additional docs

For commercial positioning and launch messaging, see `Commercial_README.md`.
