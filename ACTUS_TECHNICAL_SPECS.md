# Actus Technical Specifications

## Project Overview
Actus is a full-stack agent orchestration system designed for credit operations analytics and ticket management. It provides an AI-powered chatbot interface for querying credit request data, generating insights, and recommending next actions using Retrieval-Augmented Generation (RAG).

## Architecture
- **Monorepo Structure**: Unified codebase with separate frontend and backend directories
- **Intent-Based Routing**: Deterministic intent recognition with LLM fallback for flexible natural language processing
- **RAG System**: Custom-built retrieval-augmented generation for next action recommendations with configurable rules engine
- **Real-time Data Pipeline**: Firebase Realtime Database integration with caching and automatic rebuild intervals

## Technology Stack

### Frontend
- **React 19** with TypeScript 5.9 for type-safe component development
- **Vite 7** for fast development server and optimized production builds
- **Tailwind CSS 3.4** for utility-first responsive styling
- **React Markdown** with GitHub Flavored Markdown support for rich text rendering
- **Recharts** for interactive data visualizations and charts
- **Lucide React** for consistent iconography

### Backend
- **Python FastAPI** for high-performance REST API with automatic OpenAPI documentation
- **Uvicorn** ASGI server for production-ready deployment
- **Pandas** for data manipulation and analysis of credit request datasets
- **Firebase Admin SDK** for Realtime Database operations and user authentication
- **OpenRouter Client** for LLM integration with configurable models (OpenAI GPT-4o-mini)
- **OpenPyXL/XLRD** for Excel file processing (.xlsx/.xls)

## Key Features

### Core Functionality
- **Natural Language Query Processing**: Intent recognition for 20+ credit operations queries (activity, aging, anomalies, trends, etc.)
- **Data Visualization**: Interactive charts and tables for credit analytics
- **User Management**: Role-based access control with Firebase authentication
- **Real-time Updates**: Cached data with TTL-based refresh from Firebase RTDB

### Advanced Capabilities
- **LLM Routing Architecture**: Hybrid approach combining deterministic rules with LLM parsing
- **RAG Next Action Engine**: Rule-based recommendations for credit ticket resolution
- **Fallback Mechanisms**: OpenRouter integration for unmatched queries
- **Audit Logging**: Request tracing and performance monitoring

### Intent Categories
- Credit Analytics (activity, aging, amounts, anomalies, trends)
- Ticket Management (status, priority, stalled, requests)
- Customer Insights (top accounts, sales reps, items)
- Operational Reporting (snapshots, summaries, root cause analysis)
- Data Operations (record lookup, bulk search, system updates)

## Performance & Scalability
- **Caching Layer**: In-memory data caching with configurable TTL (120 seconds default)
- **Asynchronous Processing**: Threaded RAG index rebuilds for continuous optimization
- **CORS Configuration**: Support for local development and ngrok tunneling
- **Error Handling**: Comprehensive exception handling with graceful fallbacks

## Development Environment
- **Docker Support**: Containerized deployment with environment-specific configurations
- **Hot Reload**: Frontend development with Vite's fast refresh
- **Linting & Type Checking**: ESLint and TypeScript for code quality
- **Environment Management**: Dotenv configuration for secrets and API keys

## Deployment Considerations
- **Modular Design**: Separable frontend/backend for independent scaling
- **Firebase Integration**: Serverless database with real-time synchronization
- **API Gateway**: FastAPI router with automatic request validation
- **Monitoring**: Built-in health checks and performance logging

## Security Features
- **Firebase Authentication**: Secure user sessions with JWT tokens
- **Environment Variables**: Sensitive credentials managed via dotenv
- **Input Validation**: Pydantic models for request/response schema enforcement
- **PII Handling**: Data sanitization and audit logging without personal information exposure

## Repository Structure
- `frontend/`: React UI (chat, dashboards, charts)
- `backend/`: FastAPI service and intent/RAG logic
- `backend/app/`: API routers and RAG runtime modules
- `backend/actus/`: Deterministic intent handlers + OpenRouter client
- `backend/rag_data/`: Local FAISS + SQLite store for RAG chunks
- `backend/scripts/`: RAG index build and smoke tests
- `scripts/`: Root-level utilities

## API Surface (Backend)
- `GET /api/health`: Basic liveness check
- `GET /api/health/openrouter`: Validate OpenRouter connectivity
- `GET /api/user-context?email=`: Resolve user role/profile from Firebase RTDB
- `GET /api/help`: Returns help/capabilities text used in UI
- `POST /api/ask`: Main query endpoint; returns text + optional tabular rows
- `POST /rag/search`: RAG semantic search with filters and scoring
- `GET /rag/ticket/{ticket_id}/refs`: Extracts invoice/item references for a ticket
- `POST /rag/next-action/trace`: Runs next-action rules with full trace output

### `/api/ask` Request/Response
- Request: `{ "query": "..." }`
- Response: `{ "text": "...", "rows": [...], "meta": { ... } }`
- `rows` is a serialized DataFrame (dates ISO formatted); `meta` includes intent info, CSV preview rows, and provider hints

## Data Sources & Normalization
- **Firebase RTDB**: `credit_requests` for ticket/credit data, `user_roles` for user context
- **Column normalization**: Renames `Invoice #`, `Customer #`, `Item #` to consistent labels
- **Expected fields**: `Record ID`, `Ticket Number`, `Invoice Number`, `Requested By`, `Sales Rep`, `Issue Type`, `Date`, `Status`, `Reason for Credit`, `RTN_CR_No`, `Customer Number`, `Item Number`, `Credit Request Total`
- **Date handling**: `Date` parsed to pandas timestamps, stored as ISO strings in API output

## Intent Routing & NLP
- **Deterministic routing**: Alias-based matcher over 20+ intents with fuzzy matching
- **Help detection**: Special-case phrases route to `help` intent output
- **Optional LLM classifier**: When enabled, OpenRouter classifies intent IDs with confidence gating
- **Fallback behavior**: If no intent matches and OpenRouter mode is `fallback`, `/api/ask` returns an LLM response

## RAG Subsystem
- **Embeddings**: Sentence Transformers `all-MiniLM-L6-v2`
- **Vector store**: FAISS `IndexFlatIP` (cosine similarity via normalization)
- **Metadata store**: SQLite `chunks` table with `chunk_id`, `ticket_id`, `chunk_type`, `text`, `metadata_json`
- **Artifacts**: `backend/rag_data/index.faiss` + `backend/rag_data/chunks.sqlite`
- **Rebuild**: `backend/scripts/build_rag_index.py` refreshes chunks + embeddings

### `/rag/search` Filters
- `top_k`, `min_score`, `only_chunk_type`, `status_contains`, `reason_contains`, `customer`, `invoice`, `max_events`

## Next-Action Engine
- **Rules file**: `backend/config/next_action_rules.json` evaluated in priority order
- **Inputs**: Ticket snippets, reason for credit, investigation notes, and metadata
- **Outputs**: `next_action`, `action_confidence`, `action_reason_codes`, `action_tag`, `action_rule_id`
- **Trace**: `/rag/next-action/trace` returns the full rule evaluation trace for debugging

## Caching & Background Tasks
- **Data cache**: In-memory DataFrame with 120s TTL for Firebase data
- **RAG rebuild loop**: Optional background thread with configurable interval

## Configuration (Environment Variables)
- `ACTUS_FIREBASE_JSON`: Inline Firebase service account JSON
- `ACTUS_FIREBASE_PATH`: Path to Firebase JSON file
- `ACTUS_FIREBASE_URL`: RTDB URL override
- `ACTUS_OPENROUTER_API_KEY`: OpenRouter API key
- `ACTUS_OPENROUTER_MODEL`: Primary OpenRouter model
- `ACTUS_OPENROUTER_MODEL_FALLBACK`: Fallback OpenRouter model
- `ACTUS_OPENROUTER_MODE`: `always` or `fallback` LLM routing mode
- `ACTUS_OPENROUTER_SYSTEM`: System prompt for LLM responses
- `ACTUS_INTENT_CLASSIFIER`: Enable OpenRouter intent classifier (`true`/`false`)
- `ACTUS_RAG_REBUILD_SEC`: RAG rebuild interval in seconds
- `ACTUS_ENV`: Environment (`dev`, `development`, `local`) to enable default rebuild intervals

## Frontend Integration
- **Dev proxy**: Vite proxies `/api` and `/rag` to `http://localhost:8000`
- **Rendering**: Markdown output for answers + Recharts for analytics visuals

## Observability & Error Handling
- **Timing logs**: `/api/ask` prints total/phase latency to stdout
- **RTDB failures**: Return a configuration hint if Firebase creds are missing
- **HTTP errors**: Standard 400/500 responses for invalid payloads or backend failures
