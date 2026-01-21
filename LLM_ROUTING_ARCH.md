# LLM Routing Architecture (Draft)

Goal: add an LLM NLP layer for flexible language while keeping execution deterministic.

## Flow

1. Router (fast rules)
   - If query matches known deterministic patterns, route directly.
   - Otherwise, send to LLM parser.

2. LLM Parser (intent + parameters only)
   - Output a strict JSON schema: `intent`, `parameters`, `confidence`.
   - No direct execution logic.

3. Validator
   - Enforce required params and data types.
   - Normalize date formats and IDs.
   - Reject/ask follow‑up if confidence is low or missing fields.

4. Deterministic Executor
   - Calls existing intent functions with validated params.
   - No LLM here.

5. Response Composer
   - Builds text + dataframe preview + CSV metadata.

## Suggested JSON schema

```json
{
  "intent": "credit_activity",
  "parameters": {
    "date_start": "2025-12-01",
    "date_end": "2025-12-31",
    "timezone": "America/Indiana/Indianapolis"
  },
  "confidence": 0.82
}
```

## Safety rules

- If confidence < 0.6 → ask a clarification question.
- If dates are ambiguous → default to user locale/timezone but confirm.
- Log LLM output for audit/debug (strip any PII if needed).

## Integration points

- `backend/actus/intent_router.py` for routing.
- Add `llm_parser.py` for OpenRouter integration.
- Add `intent_validator.py` to enforce schema before execution.
