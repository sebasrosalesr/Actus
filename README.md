# Actus

Actus is a credit operations copilot — a React frontend and FastAPI backend that lets credit teams query their data in natural language. It routes queries to specialist analytics intents, supports a multi-intent orchestration mode called Auto, and surfaces results as structured cards, tables, and charts in a chat interface.

## Repo layout

```
backend/         FastAPI API, intent router, auto mode engine, RAG runtime, tests, Fly.io config
frontend/        React 19 + TypeScript + Vite chat UI
scripts/         Project-level helper scripts
```

## How it works

Every query hits `POST /api/ask` with a `mode` of `manual` or `auto`.

### Manual mode

The intent router inspects the query using keyword rules, alias matching, and an optional LLM classifier, then dispatches it to a single specialist intent. The specialist returns its native output — a markdown summary, a structured table, cards, or a chart.

### Auto mode

Auto mode plans which specialists to run (up to 3), executes them sequentially against the same live dataset, then synthesizes their outputs into a single executive answer using OpenRouter. The UI shows an orchestration pipeline card listing each executed intent with its status before displaying the synthesized response.

**Planning families:**

| Family | Triggered by | Candidate intents |
| --- | --- | --- |
| `entity` | Explicit ticket ID, item number, or customer name | `ticket_analysis`, `item_analysis`, `customer_analysis`, `investigation_notes` |
| `portfolio` | Portfolio-scope queries (trends, anomalies, overviews, RTN) | `credit_ops_snapshot`, `system_updates`, `billing_queue_hotspots`, `root_cause_rtn_timing`, `top_accounts`, `top_items`, `credit_anomalies`, `credit_root_causes`, `credit_trends`, `credit_aging`, `overall_summary` |

**Planning strategy:**

1. Deterministic entity detection (regex on ticket IDs, item numbers, account tokens)
2. Deterministic portfolio matching (keyword vocabularies per intent)
3. LLM planning fallback via OpenRouter — only when `ACTUS_INTENT_CLASSIFIER` is enabled and deterministic planning yields nothing

**Synthesis:**

- Primary: OpenRouter (model controlled by `ACTUS_OPENROUTER_SUMMARY_MODEL`)
- Fallback: deterministic concatenation of specialist outputs if OpenRouter is unavailable or disabled

**Architecture:**

```
User
 └─→ POST /api/ask { query, mode }
       │
       ├─ mode=manual
       │    └─ intent_router.actus_answer()
       │         └─ One specialist intent → structured output
       │
       └─ mode=auto
            └─ auto_mode.plan_auto_mode()
                 └─ Deterministic plan (+ optional LLM fallback)
                      └─ Run up to 3 specialists sequentially
                           └─ OpenRouter synthesis → unified answer + intent chips
```

## Specialist intents

### Entity analysis

| ID | Purpose |
| --- | --- |
| `ticket_analysis` | Deep-dive on one ticket; RAG retrieval + optional OpenRouter highlights and root-cause suggestions |
| `item_analysis` | Deep-dive on one product/SKU; RAG-backed credit pattern analysis |
| `customer_analysis` | Deep-dive on one customer/account; historical patterns and issue breakdown |
| `investigation_notes` | Fetch and optionally summarize investigation notes for a ticket via OpenRouter |

### Portfolio review

| ID | Purpose |
| --- | --- |
| `credit_ops_snapshot` | Operational health snapshot: throughput, backlog, root cause breakdown, SLA status |
| `overall_summary` | Executive overview: count, total dollars, status breakdown, top causes, system updates |
| `system_updates` | System-generated RTN assignments in a configurable time window |
| `billing_queue_hotspots` | Where billing queue delays are accumulating |
| `root_cause_rtn_timing` | Which root causes take the longest to reach RTN resolution |
| `credit_root_causes` | Credit totals ranked by root cause from RAG metadata |
| `credit_anomalies` | Detects unusual credits: z-score > 3 or amount > $500; flags hard-cap breaches |
| `credit_trends` | Last 30 days vs. previous 30: volume, dollars, top customers, items, and sales reps |
| `credit_aging` | Buckets open tickets without RTN by days open (0–7, 8–15, 16–30, 31–60, 61–90, 90+) |
| `credit_amount_plot` | Generates time-series chart data for credit amounts |

### Rankings

| ID | Purpose |
| --- | --- |
| `top_accounts` | Top customers by credited volume in a time window |
| `top_items` | Top products/SKUs by credited volume |
| `top_salesreps` | Top sales reps by credit request volume |

### Ticket and record lookup

| ID | Purpose |
| --- | --- |
| `ticket_status` | Latest status timestamp for a specific ticket |
| `ticket_requests` | Pending ticket requests filtered by status |
| `record_lookup` | Find records by ticket number, invoice number, or item ID |
| `bulk_search` | Multi-token search: extracts numbers from the query and returns matching rows |
| `priority_tickets` | High-priority tickets: pending RTN or time-sensitive without credit |
| `stalled_tickets` | Tickets with no update in N days |
| `credit_activity` | Credits added in a specified time window |
| `credit_numbers` | Credits that have an RTN assigned; returns RTN metadata |
| `mixed_lines` | Tickets referencing multiple items or customers |
| `customer_tickets` | Historical ticket timeline for a customer |

## Stack

| Layer | Technology |
| --- | --- |
| Frontend | React 19, TypeScript 5.9, Vite 7, Tailwind CSS 3.4 |
| UI primitives | lucide-react, Recharts, react-markdown + remark-gfm |
| Backend | Python 3.11+, FastAPI, Uvicorn |
| Data | pandas, Firebase Admin SDK (Realtime Database) |
| RAG | sentence-transformers, Pinecone (`new_design` pipeline) |
| LLM | OpenRouter (configurable models per use-case) |
| Telemetry | SQLite quality metrics |
| Deployment | Fly.io (backend), Vite dev server proxy (local) |

## Prerequisites

- Python 3.11+
- Node 20.19+ or 22.12+
- Firebase RTDB credentials
- Pinecone index (for RAG)
- OpenRouter API key (for LLM synthesis, classification, and summarization)

## Backend setup

```bash
cd backend
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Edit .env with your credentials
```

### Environment variables

#### Firebase

| Variable | Required | Notes |
| --- | --- | --- |
| `ACTUS_FIREBASE_URL` | Usually | RTDB URL; falls back to default Actus URL if omitted |
| `ACTUS_FIREBASE_JSON` | One of these | Service account JSON string |
| `ACTUS_FIREBASE_PATH` | One of these | Path to service account JSON file |

#### RAG / Pinecone

| Variable | Required | Notes |
| --- | --- | --- |
| `ACTUS_PINECONE_API_KEY` | Yes (for RAG) | Pinecone API key |
| `ACTUS_PINECONE_INDEX` | Yes (for RAG) | Index name |
| `ACTUS_PINECONE_NAMESPACE` | Recommended | Vector namespace |
| `ACTUS_PINECONE_TICKET_TOP_K` | No | Ticket search limit; default `500` |
| `ACTUS_RAG_PROVIDER` | No | Defaults to `pinecone` |

#### OpenRouter

| Variable | Required | Notes |
| --- | --- | --- |
| `ACTUS_OPENROUTER_API_KEY` | Optional | Required for all LLM-backed flows |
| `ACTUS_OPENROUTER_MODEL` | Optional | Primary chat/intent model |
| `ACTUS_OPENROUTER_MODEL_FALLBACK` | Optional | Fallback chat model |
| `ACTUS_OPENROUTER_SUMMARY_MODEL` | Optional | Auto mode synthesis + note summarization model |
| `ACTUS_OPENROUTER_SUMMARY_MODEL_FALLBACK` | Optional | Fallback synthesis model |
| `ACTUS_OPENROUTER_HIGHLIGHTS_MODEL` | Optional | Ticket highlight model (e.g. `openai/gpt-4o-mini`) |
| `ACTUS_OPENROUTER_HIGHLIGHTS_MODEL_FALLBACK` | Optional | Fallback highlight model |
| `ACTUS_OPENROUTER_MODE` | Optional | `always` or `fallback`; omit for intent-router-only |
| `ACTUS_OPENROUTER_SYSTEM` | Optional | System prompt override for direct OpenRouter calls |

#### Routing and classification

| Variable | Notes |
| --- | --- |
| `ACTUS_INTENT_CLASSIFIER` | Enables LLM intent classification and Auto mode planning fallback |
| `ACTUS_INV_NOTE_SUMMARY` | Enables LLM summarization of investigation notes |
| `ACTUS_INV_NOTE_SUMMARY_MAX_CHARS` | Input size cap for note summarization |

#### Security, auth, and rate limiting

| Variable | Notes |
| --- | --- |
| `ACTUS_ENV` | `development` or `production`; production enables fail-fast security validation |
| `ACTUS_AUTH_MODE` | `api_key` or `cloudflare_access`. Required in production |
| `ACTUS_API_KEY` | Required when `ACTUS_AUTH_MODE=api_key`. Protects all `/api/*` and `/rag/*` routes except `GET /api/health`. Pass via `x-api-key` header or `Authorization: Bearer` |
| `ACTUS_CLOUDFLARE_ACCESS_TEAM_DOMAIN` | Required when `ACTUS_AUTH_MODE=cloudflare_access` (for example `example.cloudflareaccess.com`) |
| `ACTUS_CLOUDFLARE_ACCESS_AUD` | Required when `ACTUS_AUTH_MODE=cloudflare_access`. Audience tag from the Access application |
| `ACTUS_CLOUDFLARE_ACCESS_JWKS_URL` | Optional override for the Access JWKS endpoint. Defaults to `https://<team-domain>/cdn-cgi/access/certs` |
| `ACTUS_ALLOWED_HOSTS` | Required in production. Comma-separated host allowlist for `TrustedHostMiddleware` (for example `actus-app.fly.dev`) |
| `ACTUS_DOCS_ENABLED` | Set `false` in production to disable `/docs`, `/redoc`, and `/openapi.json` |
| `ACTUS_CORS_ORIGINS` | Comma-separated origin allowlist. Required in production |
| `ACTUS_CORS_ORIGIN_REGEX` | Dev-only convenience for dynamic origins (e.g. ngrok). Not allowed in production |
| `ACTUS_RATE_LIMIT_ENABLED` | Enables app-layer rate limiting; defaults on in production |
| `ACTUS_RATE_LIMIT_WINDOW_SEC` | Shared fixed window for app-layer rate limits; default `60` |
| `ACTUS_RATE_LIMIT_ASK_PER_MIN` | Limit for `POST /api/ask`; default `30` |
| `ACTUS_RATE_LIMIT_RAG_PER_MIN` | Limit for `/rag/*`; default `20` |
| `ACTUS_RATE_LIMIT_OPENROUTER_HEALTH_PER_MIN` | Limit for `GET /api/health/openrouter`; default `5` |
| `ACTUS_LOG_RAW_QUERIES` | If `false`, quality logs store a SHA-256 fingerprint instead of raw query text. Defaults off in production |
| `ACTUS_LOG_TRACEBACKS` | Controls traceback logging for handled API errors. Defaults off in production |

#### Runtime

| Variable | Notes |
| --- | --- |
| `ACTUS_PRELOAD_NEW_RAG` | Warms RAG service at startup; default enabled |
| `ACTUS_RAG_REBUILD_SEC` | Interval (seconds) for periodic in-process RAG rebuilds; `0` = disabled |
| `ACTUS_NEW_RAG_DATA_DIR` | Override default `backend/rag_data/new_design` output path |
| `ACTUS_QUALITY_DB_PATH` | Override quality metrics SQLite path |
| `ACTUS_QUALITY_RETENTION_DAYS` | Retention window for quality events; defaults to `90` days in production |
| `ACTUS_QUALITY_PRUNE_INTERVAL_SEC` | How often startup/event writes trigger retention pruning; default `3600` seconds |
| `ACTUS_RELEASE_TAG` | Labels quality events by deployment/release |
| `ACTUS_HF_LOCAL_ONLY` | Restricts sentence-transformer loads to local cache |

## Frontend setup

```bash
cd frontend
npm install
```

Optional `frontend/.env.local`:

```bash
VITE_API_BASE_URL=http://127.0.0.1:8000   # Usually not needed in local dev
VITE_USER_EMAIL=you@example.com
VITE_USER_NAME=Your Name
# or
VITE_USER_FIRST_NAME=First
```

In local dev, Vite proxies `/api` and `/rag` to `http://localhost:8000`, so `VITE_API_BASE_URL` is generally not required.

## Run locally

```bash
# Backend
cd backend
source .venv/bin/activate
set -a && source .env && set +a
uvicorn main:APP --reload --port 8000

# Frontend (separate terminal)
cd frontend
npm run dev
```

Frontend: `http://localhost:5173`
Backend: `http://localhost:8000`

## API reference

### Core

| Method | Endpoint | Description |
| --- | --- | --- |
| `POST` | `/api/ask` | Main query endpoint; authenticated when production auth is configured |
| `GET` | `/api/help` | Returns current help text; authenticated when production auth is configured |
| `GET` | `/api/health` | Liveness check |
| `GET` | `/api/health/openrouter` | Checks OpenRouter connectivity; authenticated and rate-limited |
| `GET` | `/api/user-context?email=...` | User profile lookup from Firebase; authenticated when production auth is configured |

**`POST /api/ask` request:**
```json
{ "query": "give me a credit overview for the last month", "mode": "auto" }
```

**Response:**
```json
{
  "text": "...",
  "rows": [...],
  "meta": {
    "intent_id": "...",
    "auto_mode": {
      "enabled": true,
      "primary_intent": "...",
      "executed_intents": [{ "id": "...", "label": "...", "status": "ok" }],
      "planner": "deterministic"
    }
  }
}
```

### Quality metrics

| Method | Endpoint | Description |
| --- | --- | --- |
| `GET` | `/api/quality/summary` | Rollup for a window (e.g. `?window=28d`); authenticated when production auth is configured |
| `GET` | `/api/quality/trends` | Grouped trends (default: weekly over `12w`); authenticated when production auth is configured |

### RAG

| Method | Endpoint | Description |
| --- | --- | --- |
| `GET` | `/rag/health` | RAG service status; authenticated when production auth is configured |
| `POST` | `/rag/new/search` | Vector search; authenticated and rate-limited |
| `POST` | `/rag/new/answer` | RAG-grounded answer; authenticated and rate-limited |
| `POST` | `/rag/new/refresh` | Rebuild index from Firebase; authenticated and rate-limited |
| `POST` | `/rag/new/ticket-analysis` | Ticket deep-dive; authenticated and rate-limited |
| `POST` | `/rag/new/item-analysis` | Item deep-dive; authenticated and rate-limited |
| `POST` | `/rag/new/customer-analysis` | Customer deep-dive; authenticated and rate-limited |
| `GET` | `/rag/ticket/{ticket_id}/refs` | Ticket RAG references; authenticated when production auth is configured |
| `POST` | `/rag/next-action/trace` | Next-action rule trace; authenticated when production auth is configured |

## Example requests

```bash
# Manual — single intent
curl -sS -X POST http://127.0.0.1:8000/api/ask \
  -H 'Content-Type: application/json' \
  -d '{"query":"analyze ticket R-048484","mode":"manual"}' | jq

# Auto — portfolio
curl -sS -X POST http://127.0.0.1:8000/api/ask \
  -H 'Content-Type: application/json' \
  -d '{"query":"give me a credit overview for the last month","mode":"auto"}' | jq

# Auto — entity
curl -sS -X POST http://127.0.0.1:8000/api/ask \
  -H 'Content-Type: application/json' \
  -d '{"query":"analyze ticket R-067298 with investigation notes","mode":"auto"}' | jq

# RAG search
curl -sS -X POST http://127.0.0.1:8000/rag/new/search \
  -H 'Content-Type: application/json' \
  -d '{"query":"price loaded after invoice","top_k":8}' | jq

# Health
curl -sS http://127.0.0.1:8000/api/health | jq
```

If `ACTUS_AUTH_MODE=api_key`, include `-H 'x-api-key: $ACTUS_API_KEY'` on every endpoint except `GET /api/health`. If `ACTUS_AUTH_MODE=cloudflare_access`, requests must arrive through Cloudflare Access with a valid `Cf-Access-Jwt-Assertion`.

## RAG pipeline

The only supported pipeline is `new_design` (Pinecone-backed). Legacy local/FAISS behavior has been removed.

Build the index:

```bash
cd backend
source .venv/bin/activate
python scripts/build_rag_index.py
```

If Pinecone credentials are missing, the build script rebuilds the canonical snapshot locally and skips remote indexing.

Refresh at runtime:

```bash
curl -sS -X POST http://127.0.0.1:8000/rag/new/refresh \
  -H 'Content-Type: application/json' \
  -d '{"index": true}' | jq
```

Deterministic next-action rules live in `backend/config/next_action_rules.json`.

## Quality metrics

Events are stored in SQLite (`backend/rag_data/quality_metrics.sqlite` by default). In production, quality logging fingerprints query text by default instead of storing raw prompts, stores client-safe error text, and prunes old rows using `ACTUS_QUALITY_RETENTION_DAYS` (default `90` days). Use `ACTUS_RELEASE_TAG` to segment events by deployment.

## Security defaults

- Production boot fails fast if auth, CORS, or host validation are incomplete.
- Production boot also fails if `ACTUS_DOCS_ENABLED=true` or `ACTUS_CORS_ORIGIN_REGEX` is set.
- `GET /api/health` is the only intentionally public endpoint.
- All other `/api/*` and `/rag/*` routes are protected by either `ACTUS_AUTH_MODE=api_key` or `ACTUS_AUTH_MODE=cloudflare_access`.
- App-layer rate limits apply to `POST /api/ask`, `/rag/*`, and `GET /api/health/openrouter`.
- Provider-backed and backend error responses are sanitized in production to avoid leaking raw upstream details.

## Tests

```bash
# Backend
cd backend && source .venv/bin/activate
python -m pytest -q tests

# Frontend
cd frontend
npm run build
npm run lint
```

## Deployment

- Fly.io config: `backend/fly.toml`
- Docker image: `backend/Dockerfile`
- Process: `.venv/bin/uvicorn main:APP --host 0.0.0.0 --port 8000`

Recommended production secrets before deploying to Fly:

```bash
ACTUS_ENV=production
ACTUS_AUTH_MODE=cloudflare_access
ACTUS_CLOUDFLARE_ACCESS_TEAM_DOMAIN=<your-team>.cloudflareaccess.com
ACTUS_CLOUDFLARE_ACCESS_AUD=<access-audience-tag>
ACTUS_CORS_ORIGINS=https://<frontend-domain>
ACTUS_ALLOWED_HOSTS=actus-app.fly.dev
ACTUS_DOCS_ENABLED=false
```

Also remove `ACTUS_CORS_ORIGIN_REGEX` from production if it is set, because production boot now rejects regex-based origin config.

Scheduled index rebuild on Fly:

```bash
flyctl machines run \
  --app <fly-backend-app> \
  --schedule "*/15 * * * *" \
  --command "python" \
  -- scripts/build_rag_index.py
```

## Related docs

- `ACTUS_TECHNICAL_SPECS.md` — broader technical summary
- `LLM_ROUTING_ARCH.md` — model and routing architecture notes
- `SECURITY_BACKEND_HARDENING.md` — security backlog and hardening plan
