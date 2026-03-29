import json
import os
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional


def _is_production() -> bool:
    raw = os.environ.get("ACTUS_ENV", "").strip().lower()
    return raw in {"production", "prod"}


def _debug_enabled() -> bool:
    return os.environ.get("ACTUS_OPENROUTER_DEBUG", "").strip().lower() in {"1", "true", "yes", "on"}


def _client_safe_failure(detail: str, *, generic: str) -> str:
    return detail if _debug_enabled() or not _is_production() else generic


def openrouter_chat(messages: List[Dict[str, str]], model: Optional[str] = None) -> str:
    api_key = os.environ.get("ACTUS_OPENROUTER_API_KEY") or os.environ.get("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError("Missing OpenRouter API key. Set ACTUS_OPENROUTER_API_KEY.")

    resolved_model = model or os.environ.get("ACTUS_OPENROUTER_MODEL", "openai/gpt-4o-mini")
    referer = os.environ.get("ACTUS_OPENROUTER_REFERER", "http://localhost:8000")
    title = os.environ.get("ACTUS_OPENROUTER_TITLE", "Actus")

    payload: Dict[str, Any] = {
        "model": resolved_model,
        "messages": messages,
    }
    debug = _debug_enabled()
    if debug:
        print(f"[openrouter] request model={resolved_model} messages={len(messages)}")
    req = urllib.request.Request(
        "https://openrouter.ai/api/v1/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": referer,
            "X-Title": title,
        },
        method="POST",
    )
    max_retries = 2
    for attempt in range(max_retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read().decode("utf-8")
            break
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8") if exc.fp else str(exc)
            if exc.code == 429 and attempt < max_retries:
                time.sleep(0.5 * (2 ** attempt))
                continue
            raise RuntimeError(
                _client_safe_failure(
                    f"OpenRouter request failed: {detail}",
                    generic=f"OpenRouter request failed (status {exc.code}).",
                )
            ) from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(
                _client_safe_failure(
                    f"OpenRouter request failed: {exc}",
                    generic="OpenRouter request failed.",
                )
            ) from exc

    data = json.loads(raw)
    if debug:
        usage = data.get("usage") or {}
        print(
            "[openrouter] response "
            f"id={data.get('id')} model={data.get('model')} "
            f"prompt_tokens={usage.get('prompt_tokens')} completion_tokens={usage.get('completion_tokens')}"
        )
    choice = (data.get("choices") or [{}])[0]
    message = choice.get("message") or {}
    content = message.get("content")
    if not content:
        raise RuntimeError("OpenRouter returned empty response.")
    return content
