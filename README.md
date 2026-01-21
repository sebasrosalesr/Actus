# Actus

Monorepo with a React frontend and a Python backend API for the Actus agent orchestration system.

## Structure

- `backend/` Python service (FastAPI + Actus intent router)
- `frontend/` React UI (Vite)

## Tech specs

Frontend:
- React 19 + TypeScript 5.9
- Vite 7
- Tailwind CSS 3.4
- react-markdown + remark-gfm
- lucide-react

Backend:
- Python (FastAPI)
- Uvicorn
- pandas
- Firebase Admin SDK (RTDB)
- python-dateutil, python-dotenv
- openpyxl (xlsx), optional xlrd (xls)

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

## OpenRouter (optional)

Actus can route `/api/ask` to OpenRouter.

```bash
export ACTUS_OPENROUTER_API_KEY="your-openrouter-key"
export ACTUS_OPENROUTER_MODEL="openai/gpt-4o-mini"
# Modes: "always" (use OpenRouter for all asks) or "fallback" (only when Actus doesn't match an intent)
export ACTUS_OPENROUTER_MODE="fallback"
```

Run the API:

```bash
uvicorn app:APP --reload --port 8000
```

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

## Frontend setup

```bash
cd frontend
npm install
npm run dev
```

The dev server proxies `/api` to `http://localhost:8000`.
